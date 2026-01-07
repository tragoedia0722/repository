package extractor

import (
	"errors"
	"fmt"
)

// Common error variables
var (
	// ErrPathExistsOverwrite is returned when a path exists and overwriting is not allowed
	ErrPathExistsOverwrite = errors.New("path already exists and overwriting is not allowed")

	// ErrPathTraversal is returned when extraction path attempts to escape base directory
	ErrPathTraversal = errors.New("extraction path escapes base directory")

	// ErrInterrupted is returned when extraction is interrupted
	ErrInterrupted = errors.New("extraction was interrupted")

	// ErrInvalidDirectoryEntry is returned when a directory entry name is invalid
	ErrInvalidDirectoryEntry = errors.New("invalid directory entry name")

	// ErrPathTraversalAttempt is returned when a path traversal attempt is detected
	ErrPathTraversalAttempt = errors.New("path traversal attempt")

	// ErrInvalidPathComponent is returned when a path component is invalid
	ErrInvalidPathComponent = errors.New("invalid path component")

	// ErrInvalidSymlinkTarget is returned when a symlink target is invalid
	ErrInvalidSymlinkTarget = errors.New("invalid symlink target")

	// ErrUnsupportedFileType is returned when a file type is not supported
	ErrUnsupportedFileType = errors.New("unsupported file type")
)

// PathError represents an error related to path operations
type PathError struct {
	Path string
	Op   string
	Err  error
}

func (e *PathError) Error() string {
	if e.Op == "" {
		return fmt.Sprintf("path error %q: %v", e.Path, e.Err)
	}
	return fmt.Sprintf("%s %q: %v", e.Op, e.Path, e.Err)
}

func (e *PathError) Unwrap() error {
	return e.Err
}

// Error wrapping helpers

// wrapPathTraversal wraps an error with path traversal information
func wrapPathTraversal(path string) error {
	return &PathError{
		Path: path,
		Op:   "extract",
		Err:  ErrPathTraversal,
	}
}

// wrapInvalidSymlinkTarget wraps an error with invalid symlink target information
func wrapInvalidSymlinkTarget(target string) error {
	return fmt.Errorf("%w: %s", ErrInvalidSymlinkTarget, target)
}

// wrapUnsupportedFileType wraps an error with unsupported file type information
func wrapUnsupportedFileType(path string, fileType interface{}) error {
	return &PathError{
		Path: path,
		Op:   "extract",
		Err:  fmt.Errorf("%w: %T", ErrUnsupportedFileType, fileType),
	}
}

// wrapInvalidDirectoryEntry wraps an error with invalid directory entry information
func wrapInvalidDirectoryEntry(entryName string) error {
	return fmt.Errorf("%w: %s", ErrInvalidDirectoryEntry, entryName)
}

// wrapPathComponent wraps an error with invalid path component information
func wrapPathComponent(component string) error {
	return fmt.Errorf("%w: %s", ErrInvalidPathComponent, component)
}

// wrapPathTraversalAttempt wraps an error with path traversal attempt information
func wrapPathTraversalAttempt(entryName string) error {
	return fmt.Errorf("%w: %s", ErrPathTraversalAttempt, entryName)
}

// wrapRemoveFailed wraps an error when removing a path fails
func wrapRemoveFailed(path string, err error) error {
	return &PathError{
		Path: path,
		Op:   "remove",
		Err:  err,
	}
}

// wrapMkdirFailed wraps an error when creating a directory fails
func wrapMkdirFailed(path string, err error) error {
	return &PathError{
		Path: path,
		Op:   "mkdir",
		Err:  err,
	}
}
