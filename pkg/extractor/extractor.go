// Package extractor provides functionality for extracting files from IPFS DAG nodes
// to the local file system. It supports atomic writes, progress tracking, and handles
// various file types including regular files, directories, and symlinks.
//
// The extractor ensures safe extraction by:
//   - Preventing path traversal attacks
//   - Supporting atomic writes using temporary .part files
//   - Handling backslash-separated paths (Windows-style)
//   - Cleaning invalid filenames automatically
//
// Example usage:
//
//	extractor := NewExtractor(blockStore, cid, "/output/path")
//	extractor.WithProgress(func(completed, total int64, file string) {
//	    fmt.Printf("Progress: %d/%d\n", completed, total)
//	})
//	err := extractor.Extract(ctx, true) // true to allow overwriting
package extractor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	"github.com/ipfs/go-cid"
)

// Error variables are defined in errors.go
// progressCallback is defined in progress.go

// Extractor handles extraction of files from IPFS DAG nodes to the local filesystem.
// It provides atomic writes, progress tracking, and safety features like path traversal
// prevention.
type Extractor struct {
	blockStore blockstore.Blockstore // IPFS blockstore for reading data
	cid        string                // Content ID of the root node to extract
	path       string                // Target extraction path (cleaned and normalized)
	basePath   string                // Base path for security validation
	trackerMu  sync.RWMutex          // Protects tracker access
	tracker    *progressTracker      // Progress tracking and interruption state
	bufferPool sync.Pool             // Buffer pool for efficient file writes
}

// NewExtractor creates a new Extractor instance with the given blockstore, CID,
// and target path. The path is cleaned and normalized for safety.
func NewExtractor(blockStore blockstore.Blockstore, cid string, path string) *Extractor {
	finalPath := cleanPathComponents(path)

	return &Extractor{
		blockStore: blockStore,
		cid:        cid,
		path:       finalPath,
		basePath:   finalPath,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, defaultWriteBufferSize)
			},
		},
	}
}

// WithProgress sets a callback function that will be called periodically during
// extraction to report progress. The callback receives the number of bytes completed,
// total bytes, and the current file being extracted.
// Returns the extractor instance for method chaining.
func (ext *Extractor) WithProgress(progressFn progressCallback) *Extractor {
	ext.trackerMu.Lock()
	defer ext.trackerMu.Unlock()

	if ext.tracker == nil {
		ext.tracker = newProgressTracker(0, progressFn)
	} else {
		ext.tracker.callback = progressFn
	}
	return ext
}

// Extract starts the extraction process from the IPFS DAG node specified by the CID.
// If overwrite is true, existing files will be replaced. Otherwise, extraction will
// fail if any file already exists.
//
// The extraction is performed atomically using temporary .part files, and supports
// context cancellation for graceful interruption.
func (ext *Extractor) Extract(ctx context.Context, overwrite bool) error {
	bs := blockservice.New(ext.blockStore, nil)
	ds := merkledag.NewDAGService(bs)

	c, err := cid.Parse(ext.cid)
	if err != nil {
		return err
	}

	node, err := ds.Get(ctx, c)
	if err != nil {
		return err
	}

	fileNode, err := unixfile.NewUnixfsFile(ctx, ds, node)
	if err != nil {
		return err
	}

	size, err := fileNode.Size()
	if err != nil {
		return err
	}

	// Initialize progress tracker if not already initialized
	ext.trackerMu.Lock()
	if ext.tracker == nil {
		ext.tracker = newProgressTracker(size, nil)
	} else {
		ext.tracker.setTotal(size)
	}
	ext.trackerMu.Unlock()

	if !ext.isSubPath(ext.path, ext.basePath) {
		return ErrPathTraversal
	}

	err = ext.writeTo(ctx, fileNode, ext.path, overwrite, "")
	if err != nil {
		return err
	}

	return nil
}

func (ext *Extractor) updateProgress(size int64, filename string) {
	ext.trackerMu.RLock()
	defer ext.trackerMu.RUnlock()

	if ext.tracker != nil {
		ext.tracker.update(size, filename)
	}
}

func (*Extractor) isSubPath(path, base string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	absBase, err := filepath.Abs(base)
	if err != nil {
		return false
	}

	absPath = filepath.Clean(absPath)
	absBase = filepath.Clean(absBase)

	return absPath == absBase || strings.HasPrefix(absPath, absBase+string(filepath.Separator))
}

func (ext *Extractor) writeTo(ctx context.Context, nd files.Node, path string, allowOverwrite bool, relativePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ext.trackerMu.RLock()
	interrupted := ext.tracker != nil && ext.tracker.isSet()
	ext.trackerMu.RUnlock()

	if interrupted {
		return ErrInterrupted
	}

	if err := ensureNoSymlinkInPath(ext.basePath, path); err != nil {
		return err
	}

	// Check if path exists and get its info
	pathInfo, err := getPathInfo(path)
	if err != nil {
		return err
	}

	if pathInfo.exists {
		if !allowOverwrite {
			return ErrPathExistsOverwrite
		}

		isNodeDir := ext.isDir(nd)
		nodeSize, err := nd.Size()
		if err != nil {
			return fmt.Errorf("failed to get node size: %w", err)
		}

		// Check if we should skip this existing file (only for regular files with same size)
		if shouldSkipExistingFile(pathInfo.FileInfo, nodeSize, isNodeDir) {
			// Update progress and skip extraction
			ext.updateProgress(nodeSize, relativePath)
			return nil
		}

		// For existing directories that match node directories, merge contents (do nothing)
		// For everything else, remove the existing path
		if !(pathInfo.IsDir() && isNodeDir) {
			if err := removePath(path); err != nil {
				return err
			}
		}
	}

	switch node := nd.(type) {
	case *files.Symlink:
		target := node.Target
		if !ext.isValidSymlinkTarget(target) {
			return wrapInvalidSymlinkTarget(target)
		}
		return os.Symlink(target, path)

	case files.File:
		return ext.writeFileWithBuffer(ctx, node, path, relativePath)

	case files.Directory:
		if err := os.MkdirAll(path, dirPermissions); err != nil {
			return err
		}
		entries := node.Entries()
		return ext.processDirectory(ctx, entries, path, allowOverwrite, relativePath)

	default:
		return wrapUnsupportedFileType(path, node)
	}
}

func (*Extractor) createPartFile(finalPath string) (*os.File, string, error) {
	dir := filepath.Dir(finalPath)
	if err := os.MkdirAll(dir, dirPermissions); err != nil {
		return nil, "", err
	}

	partPath := finalPath + partFileSuffix

	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, filePermissions)
	if err == nil {
		return f, partPath, nil
	}
	if !os.IsExist(err) {
		return nil, "", err
	}

	if remErr := os.Remove(partPath); remErr != nil && !os.IsNotExist(remErr) {
		return nil, "", remErr
	}
	f, err = os.OpenFile(partPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, filePermissions)
	if err != nil {
		return nil, "", err
	}

	return f, partPath, nil
}

func (ext *Extractor) writeFileWithBuffer(ctx context.Context, node files.File, path string, relativePath string) error {
	tmpF, tmpPath, err := ext.createPartFile(path)
	if err != nil {
		return err
	}

	var retErr error
	defer func() {
		if tmpF != nil {
			_ = tmpF.Close()
		}
		if retErr != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	pr := &extractReader{
		r: node,
		onProgress: func(n int64) {
			ext.updateProgress(n, relativePath)
		},
	}

	buf := ext.bufferPool.Get().([]byte)
	defer ext.bufferPool.Put(buf)

	_, copyErr := io.CopyBuffer(tmpF, pr, buf)
	if copyErr != nil {
		retErr = copyErr
		return retErr
	}

	// Flush any remaining progress
	if pr.bytesSinceUpdate > 0 {
		ext.updateProgress(pr.bytesSinceUpdate, relativePath)
	}

	if err = tmpF.Sync(); err != nil {
		retErr = err
		return retErr
	}

	if err = tmpF.Close(); err != nil {
		retErr = err
		return retErr
	}
	tmpF = nil

	if err = os.Rename(tmpPath, path); err != nil {
		retErr = err
		return retErr
	}

	return nil
}

func (ext *Extractor) processDirectory(ctx context.Context, entries files.DirIterator, path string, allowOverwrite bool, relativePath string) error {
	for entries.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		entryName := entries.Name()
		if entryName == "" || entryName == "." || entryName == ".." {
			return wrapInvalidDirectoryEntry(entryName)
		}

		// Normalize the entry name (handles both simple names and backslash paths)
		cleanedName, err := normalizeEntryName(entryName)
		if err != nil {
			return err
		}

		// Build child paths
		var childPath, childRelPath string
		childPath = filepath.Join(path, cleanedName)
		childRelPath = filepath.Join(relativePath, cleanedName)

		// If the cleaned name contains path separators (nested path from backslash handling),
		// create parent directories to ensure they exist before writing the file
		if strings.Contains(cleanedName, string(filepath.Separator)) {
			parentDir := filepath.Dir(childPath)
			if err := os.MkdirAll(parentDir, dirPermissions); err != nil {
				return wrapMkdirFailed(parentDir, err)
			}
		}

		entryNode := entries.Node()

		if err := ext.writeTo(ctx, entryNode, childPath, allowOverwrite, childRelPath); err != nil {
			return err
		}
	}

	return entries.Err()
}

func (*Extractor) createNewFile(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, dirPermissions); err != nil {
		return nil, err
	}

	return os.OpenFile(path, os.O_EXCL|os.O_CREATE|os.O_WRONLY, filePermissions)
}

func (*Extractor) isDir(nd files.Node) bool {
	_, isDir := nd.(files.Directory)
	return isDir
}

func (*Extractor) isValidSymlinkTarget(target string) bool {
	return validateSymlinkTarget(target)
}

type extractReader struct {
	r                io.Reader
	onProgress       func(int64)
	bytesSinceUpdate int64
}

func (pr *extractReader) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	if n > 0 && pr.onProgress != nil {
		pr.bytesSinceUpdate += int64(n)
		if pr.bytesSinceUpdate >= progressUpdateThreshold {
			pr.onProgress(pr.bytesSinceUpdate)
			pr.bytesSinceUpdate = 0
		}
	}

	return
}
