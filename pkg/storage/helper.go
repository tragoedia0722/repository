package storage

import (
	"errors"
	"os"
	"path/filepath"
)

func Writable(path string) error {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return err
	}

	if f, err := os.Create(filepath.Join(path, "._check_writable")); err == nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
	} else {
		return errors.New("'" + path + "' is not writable")
	}

	return nil
}

func DatastoreSpecPath(repoPath string) string {
	return filepath.Join(repoPath, "datastore_spec")
}

func FileExists(filename string) bool {
	fi, err := os.Lstat(filename)
	if fi != nil || (err != nil && !os.IsNotExist(err)) {
		return true
	}

	return false
}
