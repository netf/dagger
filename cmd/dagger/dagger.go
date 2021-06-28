package main

import (
	"flag"
	"github.com/netf/dagger/internal/deployer"
	"log"
)

func main() {

	var dagsFolder, dagList, projectID, composerRegion, composerEnvName, dagBucketPrefix string
	var replace bool

	flag.StringVar(&dagList, "dagList", "./config/running_dags.txt", "path to the list of dags that should be running after the deploy")
	flag.StringVar(&dagsFolder, "dagsFolder", "./dags", "path to the dags folder in the repo.")
	flag.StringVar(&projectID, "project", "", "gcp project id")
	flag.StringVar(&composerRegion, "region", "", "project")
	flag.StringVar(&composerEnvName, "composerEnv", "", "Composer environment name")
	flag.StringVar(&dagBucketPrefix, "dagBucketPrefix", "", "Composer DAGs bucket prefix")
	flag.BoolVar(&replace, "replace", false, "Boolean flag to indicatae if source dag mismatches the object of same name in GCS delte the old version and deploy over it")

	flag.Parse()

	flags := map[string]string{
		"dagsFolder":      dagsFolder,
		"dagList":         dagList,
		"projectID":       projectID,
		"composerRegion":  composerRegion,
		"composerEnvName": composerEnvName,
		"dagBucketPrefix": dagBucketPrefix,
	}

	// Check flags are not empty.
	for k, v := range flags {
		if v == "" {
			log.Panicf("%v must not be empty.", k)
		}
	}

	c := deployer.ComposerEnv{
		Name:            composerEnvName,
		Project:         projectID,
		Location:        composerRegion,
		DagBucketPrefix: dagBucketPrefix,
		LocalDagsPrefix: dagsFolder}

	dagsToStop, dagsToStart := c.GetStopAndStartDags(dagList, replace)
	c.StopDags(dagsToStop, !replace)
	c.StartDags(dagsFolder, dagsToStart)
}
