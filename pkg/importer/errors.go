package importer

import (
	"errors"
	"fmt"
)

var (
	// ErrNoContent is returned when no file content is found
	ErrNoContent = errors.New("no file")

	// ErrInterrupted is returned when import is interrupted
	ErrInterrupted = errors.New("import was interrupted")

	// ErrInvalidNodeType is returned for unsupported file types
	ErrInvalidNodeType = errors.New("unknown file type")

	// ErrMfsRootNil is returned when MFS root initialization fails
	ErrMfsRootNil = errors.New("mfs root is nil")
)

// ImportError represents an error during import with context
type ImportError struct {
	Path string
	Op   string
	Err  error
}

func (e *ImportError) Error() string {
	if e.Op == "" {
		return fmt.Sprintf("import error %q: %v", e.Path, e.Err)
	}
	return fmt.Sprintf("%s %q: %v", e.Op, e.Path, e.Err)
}

func (e *ImportError) Unwrap() error {
	return e.Err
}
