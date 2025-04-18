package extractor

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	"github.com/ipfs/go-cid"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
)

const (
	invalidChars       = `/` + "\x00"
	maxConcurrency     = 8
	largeFileThreshold = 100 * 1024 * 1024
	writeBufferSize    = 4 * 1024 * 1024
)

var (
	ErrInvalidDirectoryEntry = errors.New("invalid directory entry name")
	ErrPathExistsOverwrite   = errors.New("path already exists and overwriting is not allowed")
	ErrPathTraversal         = errors.New("extraction path escapes base directory")
)

type extractorProgressCallback func(completed, total int64, currentFile string)

type Extractor struct {
	blockStore    blockstore.Blockstore
	cid           string
	path          string
	basePath      string
	progress      extractorProgressCallback
	bufferPool    sync.Pool
	nodeSize      int64
	extractedSize atomic.Int64
	isInterrupted bool
}

func NewExtractor(blockStore blockstore.Blockstore, cid string, path string) *Extractor {
	cleanPath := filepath.Clean(path)

	return &Extractor{
		blockStore: blockStore,
		cid:        cid,
		path:       cleanPath,
		basePath:   cleanPath,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, writeBufferSize)
			},
		},
	}
}

func (e *Extractor) WithProgress(progressFn extractorProgressCallback) *Extractor {
	e.progress = progressFn
	return e
}

func (e *Extractor) Extract(ctx context.Context, overwrite bool) error {
	bs := blockservice.New(e.blockStore, nil)
	ds := merkledag.NewDAGService(bs)

	c, err := cid.Parse(e.cid)
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
	e.nodeSize = size

	if !isSubPath(e.path, e.basePath) {
		return ErrPathTraversal
	}

	err = e.writeTo(ctx, fileNode, e.path, overwrite, "")
	if err != nil {
		return err
	}

	return nil
}

func (e *Extractor) updateProgress(size int64, filename string) {
	e.extractedSize.Add(size)

	if e.progress != nil {
		e.progress(e.extractedSize.Load(), e.nodeSize, filename)
	}
}

func isSubPath(path, base string) bool {
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

func (e *Extractor) writeTo(ctx context.Context, nd files.Node, path string, allowOverwrite bool, relativePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if e.isInterrupted {
		return errors.New("extraction was interrupted")
	}

	if _, err := os.Lstat(path); err == nil {
		if !allowOverwrite {
			return ErrPathExistsOverwrite
		}

		fi, e1 := os.Stat(path)
		if e1 != nil {
			return e1
		}

		if !(fi.IsDir() && e.isDir(nd)) {
			if err = os.RemoveAll(path); err != nil {
				return fmt.Errorf("failed to remove existing path: %w", err)
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	switch node := nd.(type) {
	case *files.Symlink:
		target := node.Target
		if !isValidSymlinkTarget(target) {
			return fmt.Errorf("invalid symlink target: %s", target)
		}

		return os.Symlink(target, path)
	case files.File:
		return e.writeFileWithBuffer(ctx, node, path, relativePath)
	case files.Directory:
		if err := os.MkdirAll(path, 0o755); err != nil {
			return err
		}

		entries := node.Entries()

		return e.processDirectory(ctx, entries, path, allowOverwrite, relativePath)
	default:
		return fmt.Errorf("file type %T at %q is not supported", node, path)
	}
}

func (e *Extractor) writeFileWithBuffer(ctx context.Context, node files.File, path string, relativePath string) error {
	f, err := e.createNewFile(path)
	if err != nil {
		return err
	}

	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	buf := bufio.NewWriterSize(f, writeBufferSize)

	pr := &extractReader{
		r: node,
		onProgress: func(n int64) {
			e.updateProgress(n, relativePath)
		},
	}

	done := make(chan struct{})
	var copyErr error

	go func() {
		defer close(done)
		_, copyErr = io.Copy(buf, pr)
	}()

	select {
	case <-ctx.Done():
		e.isInterrupted = true
		return ctx.Err()
	case <-done:
		if copyErr != nil {
			return copyErr
		}
	}

	return buf.Flush()
}

func (e *Extractor) processDirectory(ctx context.Context, entries files.DirIterator, path string, allowOverwrite bool, relativePath string) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	sem := make(chan struct{}, maxConcurrency)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for entries.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		entryName := entries.Name()
		if entryName == "" || entryName == "." || entryName == ".." || !e.isValidFilename(entryName) {
			return ErrInvalidDirectoryEntry
		}

		childPath := filepath.Join(path, entryName)
		childRelPath := filepath.Join(relativePath, entryName)
		entryNode := entries.Node()

		fileNode, isFile := entryNode.(files.File)
		if _, isSymlink := entryNode.(*files.Symlink); isSymlink || (isFile && e.isLargeFile(fileNode)) {
			if err := e.writeTo(ctx, entryNode, childPath, allowOverwrite, childRelPath); err != nil {
				return err
			}

			continue
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(n files.Node, p, rp string) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := e.writeTo(ctx, n, p, allowOverwrite, rp); err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
			}
		}(entryNode, childPath, childRelPath)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	if err := entries.Err(); err != nil {
		return err
	}

	if err, ok := <-errCh; ok {
		return err
	}

	return nil
}

func (e *Extractor) isLargeFile(file files.File) bool {
	size, err := file.Size()
	if err != nil {
		return false
	}

	return size > largeFileThreshold
}

func (*Extractor) createNewFile(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return os.OpenFile(path, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0o644)
}

func (*Extractor) isValidFilename(filename string) bool {
	if strings.ContainsAny(filename, invalidChars) {
		return false
	}

	for _, r := range filename {
		if !unicode.IsPrint(r) {
			return false
		}
	}

	lowerName := strings.ToLower(filename)
	if lowerName == "con" || lowerName == "prn" || lowerName == "aux" ||
		lowerName == "nul" || strings.HasPrefix(lowerName, "com") ||
		strings.HasPrefix(lowerName, "lpt") {
		if len(lowerName) <= 4 {
			n := lowerName
			if strings.HasPrefix(n, "com") || strings.HasPrefix(n, "lpt") {
				n = n[3:]
				if n == "" || (len(n) == 1 && n[0] >= '0' && n[0] <= '9') {
					return false
				}
			} else {
				return false
			}
		}
	}

	return true
}

func (*Extractor) isDir(nd files.Node) bool {
	_, isDir := nd.(files.Directory)
	return isDir
}

func isValidSymlinkTarget(target string) bool {
	targetPath := filepath.Clean(target)

	return !filepath.IsAbs(targetPath) && !strings.HasPrefix(targetPath, "..")
}

type extractReader struct {
	r          io.Reader
	onProgress func(int64)
}

func (pr *extractReader) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	if n > 0 && pr.onProgress != nil {
		pr.onProgress(int64(n))
	}

	return
}
