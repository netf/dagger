package gcshasher

import (
	"cloud.google.com/go/storage"
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"
)

var testBkt = flag.String("bkt", "", "The bucket to use for testing the hash comparison")

func TestLocalMD5(t *testing.T) {
	locPath := filepath.Join("testdata", "test.txt")
	_, err := localMD5(locPath)
	if err != nil {
		t.Errorf("error hashing local file: %s", err)
	}
}

func TestLocalFileEqGCS(t *testing.T) {
	if *testBkt == "" {
		t.Skip("skipping hash comparison integration test because no test bucket passed")
	}

	locPath := filepath.Join("testdata", "test.txt")
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Errorf("Couldn't authenticate GCS client: %s", err)
	}

	var r io.Reader
	f, err := os.Open(locPath)
	defer f.Close()
	r = f

	obj := client.Bucket(*testBkt).Object("testdata/test.txt")
	w := obj.NewWriter(ctx)
	io.Copy(w, r)
	if err := w.Close(); err != nil {
		t.Errorf("couldn't write test object %s ", err)
	}

	eq, err := LocalFileEqGCS(locPath, "gs://"+*testBkt+"/testdata/test.txt")
	if !eq {
		t.Errorf("hashes were not equal for local test.txt vs gcs test.txt")
	}

	diffLocPath := filepath.Join("testdata", "test_diff.txt")
	eq, err = LocalFileEqGCS(diffLocPath, "gs://"+*testBkt+"/testdata/test.txt")
	if eq {
		t.Errorf("hashes were equal for local test_diff.txt vs gcs test.txt")
	}
	if err := obj.Delete(ctx); err != nil {
		t.Logf("couldn't clean up test object: %s", err)
	}
}
