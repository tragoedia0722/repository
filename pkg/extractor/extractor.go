package extractor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	"github.com/ipfs/go-cid"
	"github.com/tragoedia0722/repository/pkg/helper"
)

const writeBufferSize = 4 * 1024 * 1024

var (
	ErrPathExistsOverwrite = errors.New("path already exists and overwriting is not allowed")
	ErrPathTraversal       = errors.New("extraction path escapes base directory")
)

type progressCallback func(completed, total int64, currentFile string)

type Extractor struct {
	blockStore    blockstore.Blockstore
	cid           string
	path          string
	basePath      string
	progress      progressCallback
	bufferPool    sync.Pool
	nodeSize      int64
	extractedSize atomic.Int64
	isInterrupted bool
}

func NewExtractor(blockStore blockstore.Blockstore, cid string, path string) *Extractor {
	cleanPath := filepath.Clean(path)

	pathParts := strings.Split(cleanPath, string(filepath.Separator))
	cleanedParts := make([]string, 0, len(pathParts))

	for _, part := range pathParts {
		if part == "" {
			cleanedParts = append(cleanedParts, part)
			continue
		}

		cleanPart := helper.CleanFilename(part)
		if cleanPart == "" {
			cleanPart = "cleaned_dir"
		}
		cleanedParts = append(cleanedParts, cleanPart)
	}

	finalPath := strings.Join(cleanedParts, string(filepath.Separator))

	return &Extractor{
		blockStore: blockStore,
		cid:        cid,
		path:       finalPath,
		basePath:   finalPath,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, writeBufferSize)
			},
		},
	}
}

func (ext *Extractor) WithProgress(progressFn progressCallback) *Extractor {
	ext.progress = progressFn
	return ext
}

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
	ext.nodeSize = size

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
	ext.extractedSize.Add(size)

	if ext.progress != nil {
		ext.progress(ext.extractedSize.Load(), ext.nodeSize, filename)
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

	if ext.isInterrupted {
		return errors.New("extraction was interrupted")
	}

	if _, err := os.Lstat(path); err == nil {
		if !allowOverwrite {
			return ErrPathExistsOverwrite
		}

		fi, e1 := os.Lstat(path)
		if e1 != nil {
			return e1
		}

		isNodeDir := ext.isDir(nd)

		if fi.IsDir() && isNodeDir {
			// pass
		} else {
			if fi.Mode().IsRegular() && !isNodeDir {
				if size, _ := nd.Size(); fi.Size() == size {
					ext.updateProgress(size, relativePath)
					return nil
				}
			}
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
		if !ext.isValidSymlinkTarget(target) {
			return fmt.Errorf("invalid symlink target: %s", target)
		}
		return os.Symlink(target, path)

	case files.File:
		return ext.writeFileWithBuffer(ctx, node, path, relativePath)

	case files.Directory:
		if err := os.MkdirAll(path, 0o755); err != nil {
			return err
		}
		entries := node.Entries()
		return ext.processDirectory(ctx, entries, path, allowOverwrite, relativePath)

	default:
		return fmt.Errorf("file type %T at %q is not supported", node, path)
	}
}

func (*Extractor) createPartFile(finalPath string) (*os.File, string, error) {
	dir := filepath.Dir(finalPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", err
	}

	partPath := finalPath + ".part"

	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err == nil {
		return f, partPath, nil
	}
	if !os.IsExist(err) {
		return nil, "", err
	}

	if remErr := os.Remove(partPath); remErr != nil && !os.IsNotExist(remErr) {
		return nil, "", remErr
	}
	f, err = os.OpenFile(partPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
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
			return fmt.Errorf("invalid directory entry name: %s", entryName)
		}

		var childPath, childRelPath string

		if strings.Contains(entryName, "\\") {
			normalizedPath := strings.ReplaceAll(entryName, "\\", "/")
			pathParts := strings.Split(normalizedPath, "/")

			cleanedParts := make([]string, 0, len(pathParts))
			for _, part := range pathParts {
				if part == "" || part == "." {
					continue
				}
				if part == ".." {
					return fmt.Errorf("path traversal attempt: %s", entryName)
				}

				cleanPart := helper.CleanFilename(part)
				if cleanPart == "" {
					return fmt.Errorf("invalid path component: %s", part)
				}
				cleanedParts = append(cleanedParts, cleanPart)
			}

			if len(cleanedParts) == 0 {
				return fmt.Errorf("no valid path components: %s", entryName)
			}

			cleanedPath := strings.Join(cleanedParts, string(filepath.Separator))
			childPath = filepath.Join(path, cleanedPath)
			childRelPath = filepath.Join(relativePath, cleanedPath)

			parentDir := filepath.Dir(childPath)
			if err := os.MkdirAll(parentDir, 0o755); err != nil {
				return fmt.Errorf("failed to create parent directory %s: %w", parentDir, err)
			}
		} else {
			cleanName := helper.CleanFilename(entryName)
			if cleanName == "" {
				return fmt.Errorf("invalid directory entry name: %s", entryName)
			}

			childPath = filepath.Join(path, cleanName)
			childRelPath = filepath.Join(relativePath, cleanName)
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return os.OpenFile(path, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0o644)
}

func (*Extractor) isDir(nd files.Node) bool {
	_, isDir := nd.(files.Directory)
	return isDir
}

func (*Extractor) isValidSymlinkTarget(target string) bool {
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
