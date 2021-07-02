package gcshasher

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
)

func parseGcsPath(gcsPath string) (bucket string, path string, err error) {
	uri, err := url.Parse(gcsPath)
	bucket = ""
	path = ""
	if err != nil {
		err = fmt.Errorf("couldn't parse GCS URI %+v", gcsPath)
		return
	}
	if uri.Scheme != "gs" {
		err = fmt.Errorf("couldn't parse GCS URI: %+v scheme should be 'gs'", gcsPath)
		return
	}
	bucket = uri.Host
	path = uri.Path[1:]
	return
}
func gcsMD5(gcsPath string) ([]byte, error) {
	bktName, path, err := parseGcsPath(gcsPath)
	if err != nil {
		log.Fatalf("%s", err)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("Couldn't authenticate GCS client: %s", err)
	}

	attrs, err := client.Bucket(bktName).Object(path).Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("Couldn't read file hash for %s: %s", path, err)
	}

	hash := attrs.MD5
	return hash, nil
}

func localMD5(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// LocalFileEqGCS check equalit of local file and GCS object using md5 hash
func LocalFileEqGCS(localPath, gcsPath string) (bool, error) {
	loc, err := localMD5(localPath)
	if err != nil {
		err = fmt.Errorf("Local file not found %s", err)
		return false, err
	}
	gcs, err := gcsMD5(gcsPath)
	if err != nil {
		err = fmt.Errorf("GCS file not found %s", err)
		return false, err
	}

	return bytes.Compare(loc, gcs) == 0, nil
}
