package deploy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bmatcuk/doublestar"
	"github.com/inshur/dagger/internal"
	"github.com/inshur/dagger/pkg/gcshasher"
	"google.golang.org/api/iterator"
)

// ComposerEnv is a lightweight representaataion of Cloud Composer environment
type ComposerEnv struct {
	Name            string
	Project         string
	Location        string
	DagBucketPrefix string
	LocalDagsDir    string
	LocalPluginsDir string
	LocalDataDir    string
	VariablesFile   string
	ConnectionsFile string
}

// Dag is a type for dag containing it's path
type Dag struct {
	ID   string
	Path string
}

type Connection struct {
	Name     string                 `json:"name"`
	Uri      string                 `json:"uri"`
	Type     string                 `json:"type"`
	Schema   string                 `json:"schema"`
	Port     string                 `json:"port"`
	Password string                 `json:"password"`
	Login    string                 `json:"login"`
	Host     string                 `json:"host"`
	Extra    map[string]interface{} `json:"extra"`
	Test     string                 `json:"test"`
}

type Describe struct {
	Config struct {
		DagGcsPrefix string `yaml:"dagGcsPrefix"`
	}
}

func logDagList(a map[string]bool) {
	for k := range a {
		log.Printf("=> \t%s", k)
	}
	return
}

// DagList is a set of dags (for quick membership check)
type DagList map[string]bool

// ReadRunningDagsTxt reads a newline separated list of dags from a text file
func ReadRunningDagsTxt(filename string) (map[string]bool, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	dagsToRun := make(map[string]bool)
	sc := bufio.NewScanner(file)

	for sc.Scan() {
		dagsToRun[sc.Text()] = true
	}
	log.Printf("reading DAGs to run from %s:", filename)
	logDagList(dagsToRun)
	return dagsToRun, err
}

// DagListIntersect finds the common keys in two map[string]bool representing a
// list of airflow DAG IDs.
func DagListIntersect(a map[string]bool, b map[string]bool) map[string]bool {
	short := make(map[string]bool)
	long := make(map[string]bool)
	in := make(map[string]bool)

	if len(a) < len(b) {
		short, long = a, b
	} else {
		short, long = b, a
	}
	for k := range short {
		if long[k] {
			in[k] = true
		}
	}
	return in
}

// DagListDiff finds the keys in the first map[string]bool that do no appear in
// the second.
func DagListDiff(a map[string]bool, b map[string]bool) map[string]bool {
	diff := make(map[string]bool)
	for k := range a {
		if !b[k] {
			diff[k] = true
		}
	}
	return diff
}

func Upload(bucket, object, file string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("os.Open: %v", err)
	}
	wc := client.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("File.Close: %v", err)
	}
	fmt.Printf("%v uploaded.\n", object)
	return nil
}

// BulkUpload uploads files in bulk
func BulkUpload(bucket, folder, rootPath string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	fileList, objPath, err := internal.PathWalk(rootPath)
	if err != nil {
		return err
	}
	_ = objPath
	isFile := false

	for i := 0; i < len(fileList); i++ {
		// Open and read local file
		info, err := os.Stat(fileList[i])
		if os.IsNotExist(err) {
			log.Fatal("File does not exist.")
		}
		if info.IsDir() {
			continue
		} else {
			isFile = true
		}
		if isFile && !strings.Contains(fileList[i], "__pycache__") {
			f, err := os.Open(fileList[i])
			if err != nil {
				return fmt.Errorf("os.Open: %v", err)
			}

			ctx, cancel := context.WithTimeout(ctx, time.Second*50)
			defer cancel()

			// Upload an object with storage.Writer.
			object := objPath[i]
			if folder != "" {
				object = fmt.Sprintf("%s/%s", folder, objPath[i])
			}
			wc := client.Bucket(bucket).Object(object).NewWriter(ctx)
			if _, err = io.Copy(wc, f); err != nil {
				return fmt.Errorf("io.Copy: %v", err)
			}
			if err := wc.Close(); err != nil {
				return fmt.Errorf("Writer.Close: %v", err)
			}
			if err = f.Close(); err != nil {
				return fmt.Errorf("File.Close: %v", err)
			}
			fmt.Printf("%v uploaded.\n", objPath[i])
		}
	}

	return nil
}

func BulkDownload(bucket, folder, localDir string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	objects, err := ListFiles(bucket, folder)
	if err != nil {
		return fmt.Errorf("ListFiles: %s", err)
	}

	for i := 0; i < len(objects); i++ {
		rc, err := client.Bucket(bucket).Object(objects[i]).NewReader(ctx)
		if err != nil {
			return fmt.Errorf("Object(%q).NewReader: %v", objects[i], err)
		}
		defer rc.Close()

		data, err := ioutil.ReadAll(rc)
		if err != nil {
			return fmt.Errorf("ioutil.ReadAll: %v", err)
		}
		file := filepath.Join(localDir, objects[i])
		dir := strings.Join(strings.Split(file, "/")[0:len(strings.Split(file, "/"))-1], "/")

		if dir != "" {
			err = os.MkdirAll(dir, os.ModePerm)
			if err != nil {
				return fmt.Errorf("error creating directory: %v", err)
			}
		}
		err = ioutil.WriteFile(file, data, 0644)
		if err != nil {
			return fmt.Errorf("error writing to file: %v", err)
		}
		fmt.Printf("%v downloaded.\n", objects[i])
	}
	return nil
}

func DeleteFile(bucket, object string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	o := client.Bucket(bucket).Object(object)
	if err := o.Delete(ctx); err != nil {
		return fmt.Errorf("Object(%q).Delete: %v", object, err)
	}
	fmt.Printf("%v deleted.\n", object)
	return nil
}

func ListFiles(bucket string, prefix string) ([]string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	var objectPath []string
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	it := client.Bucket(bucket).Objects(ctx, nil)
	if prefix != "" {
		it = client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	}
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Bucket(%q).Objects: %v", bucket, err)
		}
		if !strings.HasSuffix(attrs.Name, "/") {
			objectPath = append(objectPath, attrs.Name)
		}
	}
	return objectPath, nil
}

func (c *ComposerEnv) Configure() error {
	subCmdArgs := []string{
		"composer", "environments", "describe",
		c.Name,
		fmt.Sprintf("--location=%s", c.Location),
	}
	log.Printf("running gcloud %s", strings.Join(subCmdArgs, " "))
	cmd := exec.Command(
		"gcloud", subCmdArgs...)

	var config Describe
	data, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}
	yaml.Unmarshal(data, &config)
	c.DagBucketPrefix = config.Config.DagGcsPrefix
	return nil
}

func (c *ComposerEnv) SyncPlugins() error {
	bucket := strings.TrimSuffix(strings.TrimPrefix(c.DagBucketPrefix, "gs://"), "/dags")
	log.Printf("syncing plugins from %s\n", c.LocalPluginsDir)
	err := BulkUpload(bucket, "plugins", c.LocalPluginsDir)
	if err != nil {
		return err
	}
	return nil
}

func (c *ComposerEnv) SyncData() error {
	bucket := strings.TrimSuffix(strings.TrimPrefix(c.DagBucketPrefix, "gs://"), "/dags")
	log.Printf("syncing data from %s\n", c.LocalDataDir)
	err := BulkUpload(bucket, "data", c.LocalDataDir)
	if err != nil {
		return err
	}
	return nil
}

func (c *ComposerEnv) ImportVariables() error {
	if c.VariablesFile != "" {
		out, err := c.Run("variables", "import", c.VariablesFile)
		if err != nil {
			log.Fatalf("Variables import failed: %s with %s", err, out)
		}
		log.Printf("Imported variables: %s", c.VariablesFile)
		log.Printf("Output: \n%s", out)
		return err
	}
	return nil
}

func (c *ComposerEnv) ImportConnections() error {
	if c.ConnectionsFile != "" {
		file, _ := ioutil.ReadFile(c.ConnectionsFile)
		var connections []Connection
		_ = json.Unmarshal(file, &connections)

		for i := 0; i < len(connections); i++ {
			out, err := c.Run("connections", "delete",
				connections[i].Name,
			)
			if err != nil {
				log.Fatalf("Connections delete failed: %s with %s", err, out)
			}
			extra, err := json.Marshal(connections[i].Extra)
			if err != nil {
				log.Fatalf("Connections json marshal failed: %s", err)
			}

			out, err = c.Run("connections", "add",
				"--conn-uri", connections[i].Uri,
				"--conn-type", connections[i].Type,
				"--conn-schema", connections[i].Schema,
				"--conn-port", connections[i].Port,
				"--conn-password", connections[i].Password,
				"--conn-login", connections[i].Login,
				"--conn-host", connections[i].Host,
				"--conn-extra", string(extra),
				connections[i].Name,
			)
			if err != nil {
				log.Fatalf("Connections import failed: %s with %s", err, out)
			}
			log.Printf("Imported variables: %s", c.ConnectionsFile)
			log.Printf("Output: \n%s", out)
			return err
		}

	}
	return nil
}

func (c *ComposerEnv) assembleComposerRunCmd(subCmd string, args ...string) []string {
	subCmdArgs := []string{
		"beta", "composer", "environments", "run",
		c.Name,
		fmt.Sprintf("--location=%s", c.Location),
		subCmd}

	if len(args) > 0 {
		subCmdArgs = append(subCmdArgs, "--")
		subCmdArgs = append(subCmdArgs, args...)
	}
	return subCmdArgs
}

// Run is used to run airflow cli commands
// it is a wrapper of gcloud composer environments run
func (c *ComposerEnv) Run(subCmd string, args ...string) ([]byte, error) {
	subCmdArgs := c.assembleComposerRunCmd(subCmd, args...)
	log.Printf("running gcloud %s", strings.Join(subCmdArgs, " "))
	cmd := exec.Command(
		"gcloud", subCmdArgs...)
	return cmd.CombinedOutput()
}

func parseListDagsOuput(out []byte) map[string]bool {
	runningDags := make(map[string]bool)
	outArr := strings.Split(string(out[:]), "\n")
	fmt.Println(outArr)

	// Find the DAGs in output
	dagSep := "-------------------------------------------------------------------"
	var dagsIdx, nSep int

	for nSep < 2 {
		if outArr[dagsIdx] == dagSep {
			nSep++
		}
		dagsIdx++
		if dagsIdx >= len(outArr) {
			log.Fatalf("list_dags output did not contain expected separators: %s", out)
		}
	}

	// Ignore empty newline and airflow_monitoring dag.
	for _, dag := range outArr[dagsIdx:] {
		if dag != "" && dag != "airflow_monitoring" {
			runningDags[dag] = true
		}
	}

	return runningDags
}

// GetRunningDags lists dags currently running in Composer Environment.
func (c *ComposerEnv) GetRunningDags() (map[string]bool, error) {
	runningDags := make(map[string]bool)
	out, err := c.Run("dags", "list")
	if err != nil {
		log.Fatalf("list_dags failed: %s with %s", err, out)
	}

	runningDags = parseListDagsOuput(out)
	log.Printf("running DAGs:")
	logDagList(runningDags)
	return runningDags, err
}

func readCommentScrubbedLines(path string) ([]string, error) {
	log.Printf("scrubbing comments in %v", path)
	commentPattern, err := regexp.Compile(`#.+`)
	if err != nil {
		return nil, fmt.Errorf("error compiling regex: %v", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't open file %v: %v", path, err)
	}
	defer file.Close()

	lines := make([]string, 0, 1)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		candidate := commentPattern.ReplaceAllString(scanner.Text(), "")
		if len(candidate) > 0 {
			lines = append(lines, candidate)
		}
	}

	return lines, scanner.Err()
}

// FindDagFilesInLocalTree searches for Dag files in dagsRoot with names in dagNames respecting .airflowignores
func FindDagFilesInLocalTree(dagsRoot string, dagNames map[string]bool) (map[string][]string, error) {

	if len(dagNames) == 0 {
		return make(map[string][]string), nil
	}
	log.Printf("searching for these DAGs in %v:", dagsRoot)
	logDagList(dagNames)
	matches := make(map[string][]string)
	// This should map a dir to the ignore patterns in it's airflow ignore if relevant
	// this allows us to easily identify the patterns relevant to this dir and it's parents, grandparents, etc.
	airflowignoreTree := make(map[string][]string)
	_, err := ioutil.ReadDir(dagsRoot)
	if err != nil {
		return matches, fmt.Errorf("error reading dagRoot: %v. %v", dagsRoot, err)
	}
	filepath.Walk(dagsRoot, func(path string, info os.FileInfo, err error) error {
		dagID := strings.TrimSuffix(info.Name(), ".py")
		relPath, err := filepath.Rel(dagsRoot, path)

		if info == nil {
			dur, _ := time.ParseDuration("5s")
			time.Sleep(dur)
		}
		// resepect .airflowignore
		if info.Name() == ".airflowignore" {
			log.Printf("found %v, adding to airflowignoreTree", path)
			patterns, err := readCommentScrubbedLines(path)
			if err != nil {
				return err
			}
			dir, err := filepath.Rel(dagsRoot, filepath.Dir(path))
			if err != nil {
				return fmt.Errorf("error making %v relative to dag root %v: %v", filepath.Dir(path), dagsRoot, err)
			}
			fullyQualifiedPatterns := make([]string, 0, len(patterns))
			for _, p := range patterns {
				fullyQualifiedPatterns = append(fullyQualifiedPatterns, filepath.Join(dir, p))
			}
			log.Printf("adding the following patterns to airflowignoreTree[%v]: %+v", dir, fullyQualifiedPatterns)
			airflowignoreTree[filepath.Dir(path)] = fullyQualifiedPatterns
			return nil
		}

		if !info.IsDir() && !dagNames[dagID] { // skip to next file if this is not relevant to dagNames
			return nil
		}

		relevantIgnores := make([]string, 0)
		// Ignore anything with tmp
		relevantIgnores = append(relevantIgnores, "tmp")
		p := path

		if ignores, ok := airflowignoreTree[p]; ok {
			relevantIgnores = append(relevantIgnores, ignores...)
		}

		// walk back to respect all parents' .airflowignore
		for {
			if p == filepath.Dir(dagsRoot) {
				break
			}
			parent := filepath.Dir(p)
			p = parent                                         // for next iteration.
			if patterns, ok := airflowignoreTree[parent]; ok { // parent has .airflowignore
				relevantIgnores = append(relevantIgnores, patterns...)
			}
		}

		thisMatch := make(map[string]bool)
		if err != nil {
			log.Printf("error making %v relative to %v, %v", path, dagsRoot, err)
			return fmt.Errorf("error making %v relative to %v, %v", path, dagsRoot, err)
		}

		for _, ignore := range relevantIgnores {
			absIgnore, err := filepath.Abs(filepath.Join(".", ignore))
			if err != nil {
				return err
			}
			absPath, err := filepath.Abs(filepath.Join(".", relPath))
			if err != nil {
				return err
			}
			var match bool
			if strings.Contains(absIgnore, "**") {
				match, err = doublestar.PathMatch(absIgnore, absPath)
				if err != nil {
					return err
				}
			}
			if !match && !strings.Contains(ignore, "**") {
				match, err = regexp.MatchString(ignore, relPath)
				if err != nil {
					log.Printf("ERROR: comparing %v %v: %v", relPath, ignore, err)
					return err
				}
			}

			// don't walk dirs we don't have to
			if match && info.IsDir() {
				log.Printf("ignoring dir: %v because matched %v", relPath, ignore)
				return filepath.SkipDir
			}

			// remove matches if previously added but now matches this ignore pattern
			if match && !info.IsDir() && dagNames[dagID] {
				log.Printf("ignoring path: %v because matched %v", relPath, ignore)
				if _, ok := matches[dagID]; ok {
					matches[dagID] = make([]string, 0)
					break // no other ignore patterns relevant if we now know this file should be ignored
				}
				return nil
			}

			// if we shouldn't ignore it and it is in dagNames then add it to matches if not already present
			if !match && !info.IsDir() && dagNames[dagID] {
				thisMatch[dagID] = true
			}
		}

		if thisMatch[dagID] {
			alreadyMatched := false
			for _, p := range matches[dagID] {
				if relPath == p {
					alreadyMatched = true
					break
				}
			}
			if !alreadyMatched {
				matches[dagID] = append(matches[dagID], relPath)
			}
		}
		return nil
	})

	errs := make([]error, 0)

	// should match exactly one path in the tree.
	for dag, matches := range matches {
		if len(matches) == 0 {
			errs = append(errs, fmt.Errorf("did not find match for %v", dag))
		} else if len(matches) > 1 {
			errs = append(errs, fmt.Errorf("found multiple matches for %v: %v", dag, matches))
		}
	}

	if len(errs) > 0 {
		return matches, fmt.Errorf("Encountered errors matching files to dags: %+v", errs)
	}
	return matches, nil
}

// FindDagFilesInGcsPrefix necessary find the file path of a dag that has been deleted from VCS
func FindDagFilesInGcsPrefix(prefix string, dagFileNames map[string]bool) (map[string][]string, error) {
	bucket := strings.TrimSuffix(strings.TrimPrefix(prefix, "gs://"), "/dags")
	dir, err := ioutil.TempDir("/tmp", "gcsDags_")
	if err != nil {
		return nil, fmt.Errorf("error creating temp dir to pull gcs dags: %v", err)
	}
	defer os.RemoveAll(dir) // clean up temp dir

	// copy gcs dags dir to local temp dir
	log.Printf("pulling down %v", prefix)
	err = BulkDownload(bucket, "dags/", dir)
	if err != nil {
		return nil, err
	}
	return FindDagFilesInLocalTree(filepath.Join(dir, "dags"), dagFileNames)
}

func (c *ComposerEnv) getRestartDags(sameDags map[string]string) map[string]bool {
	dagsToRestart := make(map[string]bool)
	for dag, relPath := range sameDags {
		// We know that the file name = dag id from the dag validation test asseting this.
		local := filepath.Join(c.LocalDagsDir, relPath)
		gcs, err := url.Parse(c.DagBucketPrefix)
		gcs.Path = path.Join(gcs.Path, relPath)
		eq, err := gcshasher.LocalFileEqGCS(local, gcs.String())
		if err != nil {
			log.Printf("error comparing file hashes %s, attempting to restart: %s", err, dag)
			dagsToRestart[dag] = true
		} else if !eq {
			dagsToRestart[dag] = true
		}
	}
	return dagsToRestart
}

// GetStopAndStartDags uses set differences between dags running in the Composer
// Environment and those in the running dags text config file.
func (c *ComposerEnv) GetStopAndStartDags(filename string) (map[string]string, map[string]string) {
	dagsToRun, err := ReadRunningDagsTxt(filename)
	if err != nil {
		log.Fatalf("couldn't read running_dags.txt: %v", filename)
	}
	runningDags, err := c.GetRunningDags()
	if err != nil {
		log.Fatalf("couldn't list dags in composer environment: %v", err)
	}
	dagsToStop := DagListDiff(runningDags, dagsToRun)
	dagsToStart := DagListDiff(dagsToRun, runningDags)
	dagsSame := DagListIntersect(runningDags, dagsToRun)
	log.Printf("DAGs same:")
	logDagList(dagsSame)

	dagPathListsSame, err := FindDagFilesInGcsPrefix(c.DagBucketPrefix, dagsToStop)
	if err != nil {
		log.Fatalf("error finding dags to stop: %v", err)
	}
	// unnest out of slice
	dagPathsSame := make(map[string]string)
	for k, v := range dagPathListsSame {
		dagPathsSame[k] = v[0]
	}
	restartDags := c.getRestartDags(dagPathsSame)

	for k, v := range restartDags {
		dagsToStop[k], dagsToStart[k] = v, v
	}

	log.Printf("DAGs to Stop:")
	logDagList(dagsToStop)
	log.Printf("DAGs to Start:")
	logDagList(dagsToStart)

	dagPathListsToStop, err := FindDagFilesInGcsPrefix(c.DagBucketPrefix, dagsToStop)
	if err != nil {
		log.Fatalf("error finding dags to stop: %v", err)
	}
	dagPathsToStop := make(map[string]string)
	for k, v := range dagPathListsToStop {
		dagPathsToStop[k] = v[0]
	}
	dagPathListsToStart, err := FindDagFilesInLocalTree(c.LocalDagsDir, dagsToStart)
	if err != nil {
		log.Fatalf("error finding dags to start: %v", err)
	}

	dagPathsToStart := make(map[string]string)
	for k, v := range dagPathListsToStart {
		dagPathsToStart[k] = v[0]
	}
	return dagPathsToStop, dagPathsToStart
}

// ComposerEnv.stopDag pauses the dag, removes the dag definition file from gcs
// and deletes the DAG from the airflow db.
func (c *ComposerEnv) stopDag(dag string, relPath string, wg *sync.WaitGroup) (err error) {
	bucket := strings.TrimSuffix(strings.TrimPrefix(c.DagBucketPrefix, "gs://"), "/dags")
	defer wg.Done()
	log.Printf("pausing dag: %v with relPath: %v", dag, relPath)
	out, err := c.Run("pause", dag)
	if err != nil {
		return fmt.Errorf("error pausing dag %v: %v", dag, string(out))
	}
	log.Printf("parsing gcs url %v", c.DagBucketPrefix)
	gcs, err := url.Parse(c.DagBucketPrefix)
	if err != nil {
		panic("error parsing dag bucket prefix")
	}

	gcs.Path = path.Join(gcs.Path, relPath)
	log.Printf("deleting %v", gcs.String())
	err = DeleteFile(bucket, fmt.Sprintf("dags/%s", relPath))
	if err != nil {
		panic("error deleting from gcs")
	}

	_, err = c.Run("delete_dag", dag)
	if err != nil {
		panic("error deleteing dag")
	}

	for i := 0; i < 5; i++ {
		if err == nil {
			break
		}
		log.Printf("Waiting 5s to retry")
		dur, _ := time.ParseDuration("5s")
		time.Sleep(dur)
		log.Printf("Retrying delete %s", dag)
		_, err = c.Run("delete_dag", dag)
	}
	if err != nil {
		return fmt.Errorf("Retried 5x, pause still failing with: %v", string(out))
	}
	return err
}

// StopDags deletes a list of dags in parallel go routines
func (c *ComposerEnv) StopDags(dagsToStop map[string]string) error {
	var stopWg sync.WaitGroup
	for k, v := range dagsToStop {
		stopWg.Add(1)
		go c.stopDag(k, v, &stopWg)
	}
	stopWg.Wait()
	return nil
}

func jitter(d time.Duration) time.Duration {
	const pct = 0.10 //Jitter up to 10% of the supplied duration.
	jit := 1 + pct*(rand.Float64()*2-1)
	return time.Duration(jit * float64(d))
}

// ComposerEnv.waitForDeploy polls a Composer environment trying to unpause
// dags. This should be called after copying a dag file to gcs when
// dag_paused_on_creation=True.
func (c *ComposerEnv) waitForDeploy(dag string) error {
	_, err := c.Run("unpause", dag)
	for i := 0; i < 5; i++ {
		if err == nil {
			break
		}
		log.Printf("Waiting 60s to retry")
		time.Sleep(jitter(time.Minute))
		log.Printf("Retrying unpause %s", dag)
		_, err = c.Run("unpause", dag)
	}
	if err != nil {
		err = fmt.Errorf("Retried 5x, unpause still failing with: %s", err)
	}
	return err
}

// ComposerEnv.startDag copies a DAG definition file to GCS and waits until you can
// successfully unpause.
func (c *ComposerEnv) startDag(dagsFolder string, dag string, relPath string, wg *sync.WaitGroup) error {
	bucket := strings.TrimSuffix(strings.TrimPrefix(c.DagBucketPrefix, "gs://"), "/dags")
	defer wg.Done()
	loc := filepath.Join(dagsFolder, relPath)
	gcs, err := url.Parse(c.DagBucketPrefix)
	if err != nil {
		return fmt.Errorf("error parsing dags prefix %v", err)
	}
	gcs.Path = path.Join(gcs.Path, relPath)
	// remove DAG first before uploading it
	err = DeleteFile(bucket, fmt.Sprintf("dags/%s", relPath))
	if err != nil {
		fmt.Printf("Cant delete dags/%s\n", relPath)
	}
	err = Upload(bucket, fmt.Sprintf("dags/%s", relPath), loc)
	if err != nil {
		return fmt.Errorf("error copying file %v to gcs: %v", loc, err)
	}
	c.waitForDeploy(dag)
	return err
}

func (c *ComposerEnv) StartMonitoringDag() error {
	c.Run("unpause", "airflow_monitoring")
	return nil
}

// StartDags deploys a list of dags in parallel go routines
func (c *ComposerEnv) StartDags(dagsFolder string, dagsToStart map[string]string) error {
	var startWg sync.WaitGroup
	for k, v := range dagsToStart {
		startWg.Add(1)
		go c.startDag(dagsFolder, k, v, &startWg)
	}
	startWg.Wait()
	return nil
}
