package extractor

import (
	"os"
	"path/filepath"
)

// fileInfo holds information about a file system entry
type fileInfo struct {
	os.FileInfo
	exists bool
}

// getPathInfo returns information about a path. If the path doesn't exist,
// exists will be false and info will be nil. If there's an error other than
// "not exist", the error is returned.
func getPathInfo(path string) (info fileInfo, err error) {
	fi, err := os.Lstat(path)
	if err == nil {
		return fileInfo{FileInfo: fi, exists: true}, nil
	}
	if os.IsNotExist(err) {
		return fileInfo{exists: false}, nil
	}
	return fileInfo{}, err
}

// shouldSkipExistingFile checks if an existing file should be skipped during extraction.
// Returns true if the file exists, is a regular file, has the same size as the node.
// For directories, returns false to allow merging of directory contents.
func shouldSkipExistingFile(fi os.FileInfo, nodeSize int64, isNodeDir bool) bool {
	// Skip regular files with the same size
	if fi.Mode().IsRegular() && !isNodeDir && fi.Size() == nodeSize {
		return true
	}

	// Don't skip directories - allow merging contents
	if fi.IsDir() && isNodeDir {
		return false
	}

	return false
}

// removePath removes a file system path. If the path is a directory, all contents
// are removed recursively. Returns an error if the removal fails.
func removePath(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return wrapRemoveFailed(path, err)
	}
	return nil
}

// createParentDirectories creates all parent directories for the given path.
// Returns an error if directory creation fails.
func createParentDirectories(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, dirPermissions); err != nil {
		return wrapMkdirFailed(dir, err)
	}
	return nil
}
