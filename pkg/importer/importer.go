// Package importer provides functionality for importing files and directories
// into IPFS using content-addressable storage. It supports:
//
//   - Single file and directory import
//   - Progress tracking with callbacks
//   - Context cancellation for graceful interruption
//   - Automatic filename cleaning for Windows compatibility
//   - Efficient chunking for large files (1MB default)
//   - Concurrent DAG traversal for performance
//
// The importer organizes blocks into packages of 100 blocks each, computing
// a SHA-256 hash for each package to enable efficient deduplication and verification.
//
// Example usage:
//
//	importer := NewImporter(blockStore, "/path/to/file")
//	importer.WithProgress(func(completed, total int64, file string) {
//	    fmt.Printf("Progress: %d/%d - %s\n", completed, total, file)
//	})
//	result, err := importer.Import(context.Background())
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Imported: %s (CID: %s)\n", result.FileName, result.RootCid)
//
// Thread Safety:
//
// The Importer is NOT safe for concurrent use. Create a new instance for each
// import operation. Internal state uses atomic operations for the progress tracker
// to ensure safe callback execution from multiple goroutines.
package importer

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/mfs"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multicodec"
)

// Constants are defined in constants.go
// Callback types are defined in progress.go

// Result contains the output of an import operation.
type Result struct {
	FileName string    // Cleaned name of the imported file/directory
	Size     int64     // Total size in bytes
	RootCid  string    // Content-addressed identifier of the root DAG node
	Packages []Package // Block packages with their hashes
	Contents []Content // List of all imported files with their sizes
}

// Package represents a collection of blocks with their computed hash.
type Package struct {
	Hash   string   // SHA-256 hash of concatenated block CIDs
	Blocks []string // List of block CIDs in this package
}

// Content represents a single file's metadata within an import.
type Content struct {
	Name string // Cleaned filename
	Size int64  // File size in bytes
}

type Importer struct {
	blockStore blockstore.Blockstore
	path       string
	dagService ipld.DAGService
	bufferedDS *ipld.BufferedDAG
	cidBuilder cid.Builder
	root       *mfs.Root
	liveNodes  atomic.Uint64    // Atomic counter for cache management
	progress   progressCallback // Callback to be stored until tracker is created
	tracker    *progressTracker // Created when total size is known
	Contents   []Content
}

// NewImporter creates a new Importer for the given path.
// The path should point to a file or directory to be imported.
func NewImporter(blockStore blockstore.Blockstore, path string) *Importer {
	return &Importer{
		blockStore: blockStore,
		path:       filepath.Clean(path),
		cidBuilder: cid.V1Builder{
			Codec:    uint64(multicodec.DagPb),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: -1,
		},
	}
}

// WithProgress sets a callback function to track import progress.
// The callback receives (completed_bytes, total_bytes, current_filename).
// Returns the importer for method chaining.
func (imp *Importer) WithProgress(progressFn progressCallback) *Importer {
	imp.progress = progressFn
	return imp
}

func (imp *Importer) updateProgress(size int64, filename string) {
	if imp.tracker != nil {
		imp.tracker.update(size, filename)
	}
}

// Import imports the file or directory into IPFS and returns the result.
// It supports cancellation through the context.
func (imp *Importer) Import(ctx context.Context) (*Result, error) {
	// Initialize services
	if err := imp.initServices(ctx); err != nil {
		return nil, err
	}

	// Prepare content
	dir, err := imp.sliceDirectory(imp.path)
	if err != nil {
		return nil, err
	}

	// Get root node
	it := dir.Entries()
	if !it.Next() {
		return nil, ErrNoContent
	}

	// Calculate total size and initialize tracker
	size, err := it.Node().Size()
	if err != nil {
		return nil, err
	}
	imp.tracker = newProgressTracker(size, imp.progress)

	// Add content to DAG
	node, err := imp.addContent(ctx, it.Node())
	if err != nil {
		return nil, err
	}

	// Commit all changes
	if err = imp.commitChanges(ctx); err != nil {
		return nil, err
	}

	// Build result
	return imp.buildResult(ctx, node, size)
}

// initServices initializes DAG service and buffered DAG
func (imp *Importer) initServices(ctx context.Context) error {
	bs := blockservice.New(imp.blockStore, nil)
	imp.dagService = merkledag.NewDAGService(bs)
	imp.bufferedDS = ipld.NewBufferedDAG(ctx, imp.dagService, ipld.MaxSizeBatchOption(defaultBatchSize))
	return nil
}

// commitChanges commits all buffered changes and flushes MFS
func (imp *Importer) commitChanges(ctx context.Context) error {
	if err := imp.bufferedDS.Commit(); err != nil {
		return err
	}
	return imp.flushMFSRoot(ctx)
}

// buildResult collects blocks, creates packages, and builds the final result
func (imp *Importer) buildResult(ctx context.Context, node ipld.Node, size int64) (*Result, error) {
	blocks, err := imp.collectBlocks(ctx, node)
	if err != nil {
		return nil, err
	}

	packages := imp.createPackages(blocks)

	return &Result{
		FileName: cleanFilename(filepath.Base(imp.path)),
		Size:     size,
		RootCid:  node.Cid().String(),
		Packages: packages,
		Contents: imp.Contents,
	}, nil
}

func (imp *Importer) sliceDirectory(filename string) (files.Directory, error) {
	lstat, err := os.Lstat(filename)
	if err != nil {
		return nil, err
	}

	if lstat.IsDir() {
		return imp.sliceDirectoryPath(filename, lstat)
	}

	return imp.sliceSingleFile(filename, lstat)
}

// sliceDirectoryPath creates a directory entry for a directory path
func (imp *Importer) sliceDirectoryPath(dirPath string, lstat os.FileInfo) (files.Directory, error) {
	node, err := files.NewSerialFile(dirPath, false, lstat)
	if err != nil {
		return nil, err
	}

	cleanDirName := cleanDirname(filepath.Base(dirPath))

	entries := []files.DirEntry{
		files.FileEntry(cleanDirName, node),
	}

	return files.NewSliceDirectory(entries), nil
}

// sliceSingleFile creates a directory entry for a single file
func (imp *Importer) sliceSingleFile(filePath string, lstat os.FileInfo) (files.Directory, error) {
	open, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	node := files.NewReaderStatFile(open, lstat)
	cleanFileName := cleanFilename(filepath.Base(filePath))

	entries := []files.DirEntry{
		files.FileEntry(cleanFileName, node),
	}

	return files.NewSliceDirectory([]files.DirEntry{
		files.FileEntry("folder", files.NewSliceDirectory(entries)),
	}), nil
}

// progressReader wraps a reader and calls a callback on each read operation
type progressReader struct {
	reader     io.Reader
	onProgress func(int64)
}

// newProgressReader creates a new progress reader
func newProgressReader(reader io.Reader, onProgress func(int64)) *progressReader {
	return &progressReader{
		reader:     reader,
		onProgress: onProgress,
	}
}

// Read implements io.Reader
func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	if n > 0 && pr.onProgress != nil {
		pr.onProgress(int64(n))
	}

	return
}

// addContent adds a node to the DAG and returns the root
func (imp *Importer) addContent(ctx context.Context, node files.Node) (ipld.Node, error) {
	if err := imp.addNode(ctx, "", node, true); err != nil {
		return nil, err
	}

	mr, err := imp.mfsRoot(ctx)
	if err != nil {
		return nil, err
	}
	defer mr.Close()

	fsNode, err := imp.getRootFSNode(ctx, mr, node)
	if err != nil {
		return nil, err
	}

	return fsNode.GetNode()
}

// getRootFSNode retrieves the root filesystem node from MFS
func (imp *Importer) getRootFSNode(ctx context.Context, mr *mfs.Root, node files.Node) (mfs.FSNode, error) {
	mrDir := mr.GetDirectory()

	if err := mrDir.Flush(); err != nil {
		return nil, err
	}

	_, isDir := node.(files.Directory)
	if !isDir {
		children, err := mrDir.ListNames(ctx)
		if err != nil {
			return nil, err
		}

		if len(children) == 0 {
			return nil, fmt.Errorf("expected at least one child directory, got none")
		}

		return mrDir.Child(children[0])
	}

	return mrDir, nil
}

// addNode adds a node to the DAG with interruption checking
func (imp *Importer) addNode(ctx context.Context, path string, node files.Node, isRoot bool) error {
	if err := imp.checkInterruption(ctx); err != nil {
		return err
	}

	defer func() {
		_ = node.Close()
	}()

	if err := imp.maybeFlushCache(ctx); err != nil {
		return err
	}

	return imp.dispatchNode(ctx, path, node, isRoot)
}

// checkInterruption checks if the import was interrupted
func (imp *Importer) checkInterruption(ctx context.Context) error {
	select {
	case <-ctx.Done():
		if imp.tracker != nil {
			imp.tracker.interrupt()
		}
		return ctx.Err()
	default:
	}

	if imp.tracker != nil && imp.tracker.checkInterrupted() {
		return ErrInterrupted
	}

	return nil
}

// maybeFlushCache flushes the cache if it's full
func (imp *Importer) maybeFlushCache(ctx context.Context) error {
	if imp.liveNodes.Load() < liveCacheSize {
		return nil
	}

	if err := imp.flushMFSRoot(ctx); err != nil {
		return err
	}

	imp.liveNodes.Store(0)
	return nil
}

// dispatchNode routes nodes to appropriate handlers based on type
func (imp *Importer) dispatchNode(ctx context.Context, path string, node files.Node, isRoot bool) error {
	imp.liveNodes.Add(1)

	switch nd := node.(type) {
	case files.Directory:
		return imp.addDir(ctx, path, nd, isRoot)
	case *files.Symlink:
		return imp.addSymlink(ctx, path, nd)
	case files.File:
		return imp.addFile(ctx, path, nd)
	default:
		return ErrInvalidNodeType
	}
}

func (imp *Importer) addDir(ctx context.Context, dirPath string, dir files.Directory, isRoot bool) error {
	if !(isRoot && dirPath == "") {
		mr, err := imp.mfsRoot(ctx)
		if err != nil {
			return err
		}

		err = mfs.Mkdir(mr, dirPath, mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: imp.cidBuilder,
		})
		if err != nil {
			return err
		}
	}

	it := dir.Entries()
	seenNames := make(map[string]string)
	for it.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		originalName := it.Name()
		entryNode := it.Node()
		_, isDir := entryNode.(files.Directory)
		cleanName := cleanEntryName(originalName, isDir)
		if previous, exists := seenNames[cleanName]; exists {
			return fmt.Errorf("duplicate cleaned entry name %q from %q and %q", cleanName, previous, originalName)
		}
		seenNames[cleanName] = originalName

		entryPath := filepath.Join(dirPath, cleanName)
		if err := imp.addNode(ctx, entryPath, entryNode, false); err != nil {
			return err
		}
	}

	return it.Err()
}

func (imp *Importer) addSymlink(ctx context.Context, path string, l *files.Symlink) error {
	data, err := unixfs.SymlinkData(l.Target)
	if err != nil {
		return err
	}

	node := merkledag.NodeWithData(data)
	if err = node.SetCidBuilder(imp.cidBuilder); err != nil {
		return err
	}

	if err = imp.dagService.Add(ctx, node); err != nil {
		return err
	}

	return imp.putNode(ctx, node, path)
}

// addFile imports a file into the DAG
func (imp *Importer) addFile(ctx context.Context, path string, file files.File) error {
	size, err := file.Size()
	if err != nil {
		return err
	}

	displayName := cleanFilename(filepath.Base(path))

	// Record content metadata
	imp.Contents = append(imp.Contents, Content{
		Name: displayName,
		Size: size,
	})

	// Create progress reader
	pr := newProgressReader(file, func(n int64) {
		imp.updateProgress(n, displayName)
	})

	// Build DAG from file
	node, err := imp.buildDAGFromFile(ctx, pr)
	if err != nil {
		return err
	}

	// Put node in MFS
	return imp.putNode(ctx, node, path)
}

func (imp *Importer) putNode(ctx context.Context, node ipld.Node, filePath string) error {
	if filePath == "" {
		filePath = filepath.Base(imp.path)
	}

	return imp.putNodeToMFS(ctx, node, filePath)
}
