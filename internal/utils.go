package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func PathWalk(root string) ([]string, []string, error){
	var files []string
	var objectPath []string

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error walking path: %v", err)
	}

	for _, f := range files {
		objectPath = append(objectPath, strings.TrimPrefix(strings.TrimPrefix(f, root), "/"))
	}

	return files, objectPath, nil
}
