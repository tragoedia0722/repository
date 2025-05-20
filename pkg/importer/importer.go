package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/boxo/mfs"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multicodec"
)

const (
	liveCacheSize = uint64(256 << 10)
	chunkSize     = 1024 * 1024
)

type progressCallback func(completed, total int64, currentFile string)

type Result struct {
	FileName string
	Size     int64
	RootCid  string
	Packages []Package
	Contents []Content
}

type Package struct {
	Hash   string
	Blocks []string
}

type Content struct {
	Name string
	Size int64
}

type Importer struct {
	blockStore    blockstore.Blockstore
	path          string
	dagService    ipld.DAGService
	bufferedDS    *ipld.BufferedDAG
	cidBuilder    cid.Builder
	root          *mfs.Root
	liveNodes     uint64
	progress      progressCallback
	processedSize atomic.Int64
	totalSize     int64
	isInterrupted bool
	Contents      []Content
}

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

func (imp *Importer) WithProgress(progressFn progressCallback) *Importer {
	imp.progress = progressFn
	return imp
}

func (imp *Importer) updateProgress(size int64, filename string) {
	imp.processedSize.Add(size)

	if imp.progress != nil {
		imp.progress(imp.processedSize.Load(), imp.totalSize, filename)
	}
}

func (imp *Importer) Import(ctx context.Context) (*Result, error) {
	bs := blockservice.New(imp.blockStore, nil)
	filename := filepath.Base(imp.path)

	imp.dagService = merkledag.NewDAGService(bs)
	imp.bufferedDS = ipld.NewBufferedDAG(ctx, imp.dagService, ipld.MaxSizeBatchOption(100<<20))

	dir, err := imp.sliceDirectory(imp.path)
	if err != nil {
		return nil, err
	}

	it := dir.Entries()
	if !it.Next() {
		return nil, errors.New("no file")
	}

	size, err := it.Node().Size()
	if err != nil {
		return nil, err
	}
	imp.totalSize = size

	node, err := imp.addContent(ctx, it.Node())
	if err != nil {
		return nil, err
	}

	cidSet := cid.NewSet()
	if err = merkledag.Walk(ctx, merkledag.GetLinksWithDAG(imp.dagService), node.Cid(), func(c cid.Cid) bool {
		return cidSet.Visit(c)
	}, merkledag.Concurrent()); err != nil {
		return nil, err
	}

	links := make([]string, 0, cidSet.Len())
	_ = cidSet.ForEach(func(c cid.Cid) error {
		links = append(links, c.String())
		return nil
	})
	sort.Strings(links)

	blocks := make([]string, 0, 100)
	packages := make([]Package, 0)

	for _, link := range links {
		blocks = append(blocks, link)

		if len(blocks) >= 100 {
			packages = append(packages, imp.calcPackage(blocks))
			blocks = make([]string, 0, 100)
		}
	}

	if len(blocks) > 0 {
		packages = append(packages, imp.calcPackage(blocks))
	}

	return &Result{
		FileName: filename,
		Size:     size,
		RootCid:  node.Cid().String(),
		Packages: packages,
		Contents: imp.Contents,
	}, nil
}

func (imp *Importer) calcPackage(blocks []string) Package {
	builder := strings.Builder{}
	builder.Grow(len(blocks) * 64)

	for _, block := range blocks {
		builder.WriteString(block)
	}

	hash := sha256.Sum256([]byte(builder.String()))

	return Package{
		Hash:   hex.EncodeToString(hash[:]),
		Blocks: blocks,
	}
}

func (imp *Importer) sliceDirectory(filename string) (files.Directory, error) {
	lstat, err := os.Lstat(filename)
	if err != nil {
		return nil, err
	}

	if lstat.IsDir() {
		node, e1 := files.NewSerialFile(filename, false, lstat)
		if e1 != nil {
			return nil, e1
		}

		entries := []files.DirEntry{
			files.FileEntry(filename, node),
		}

		return files.NewSliceDirectory(entries), nil
	}

	open, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	node := files.NewReaderStatFile(open, lstat)
	entries := []files.DirEntry{
		files.FileEntry(filename, node),
	}

	return files.NewSliceDirectory([]files.DirEntry{
		files.FileEntry("folder", files.NewSliceDirectory(entries)),
	}), nil
}

type progressReader struct {
	reader     io.Reader
	onProgress func(int64)
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	if n > 0 && pr.onProgress != nil {
		pr.onProgress(int64(n))
	}

	return
}

func (imp *Importer) addContent(ctx context.Context, node files.Node) (ipld.Node, error) {
	if err := imp.addNode(ctx, "", node, true); err != nil {
		return nil, err
	}

	mr, err := imp.mfsRoot(ctx)
	if err != nil {
		return nil, err
	}

	var fsNode mfs.FSNode
	mrDir := mr.GetDirectory()
	fsNode = mrDir

	if err = fsNode.Flush(); err != nil {
		return nil, err
	}

	_, isDir := node.(files.Directory)
	if !isDir {
		children, e1 := mrDir.ListNames(ctx)
		if e1 != nil {
			return nil, e1
		}

		if len(children) == 0 {
			return nil, fmt.Errorf("expected at least one child dir, got none")
		}

		fsNode, err = mrDir.Child(children[0])
		if err != nil {
			return nil, err
		}
	}

	if err = mr.Close(); err != nil {
		return nil, err
	}

	return fsNode.GetNode()
}

func (imp *Importer) addNode(ctx context.Context, path string, node files.Node, isRoot bool) error {
	select {
	case <-ctx.Done():
		imp.isInterrupted = true
		return ctx.Err()
	default:
	}

	if imp.isInterrupted {
		return errors.New("import was interrupted")
	}

	defer func() {
		_ = node.Close()
	}()

	if imp.liveNodes >= liveCacheSize {
		mr, err := imp.mfsRoot(ctx)
		if err != nil {
			return err
		}

		if err = mr.FlushMemFree(ctx); err != nil {
			return err
		}

		imp.liveNodes = 0
	}

	imp.liveNodes++

	switch nd := node.(type) {
	case files.Directory:
		return imp.addDir(ctx, path, nd, isRoot)
	case *files.Symlink:
		return imp.addSymlink(ctx, path, nd)
	case files.File:
		return imp.addFile(ctx, path, nd)
	default:
		return errors.New("unknown file type")
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
	for it.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		entryPath := filepath.Join(dirPath, it.Name())
		if err := imp.addNode(ctx, entryPath, it.Node(), false); err != nil {
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

func (imp *Importer) addFile(ctx context.Context, path string, file files.File) error {
	size, err := file.Size()
	if err != nil {
		return err
	}

	name := path
	if name == "" {
		name = filepath.Base(path)
	}

	imp.Contents = append(imp.Contents, Content{
		Name: name,
		Size: size,
	})

	pr := &progressReader{
		reader: file,
		onProgress: func(n int64) {
			imp.updateProgress(n, name)
		},
	}

	node, err := imp.add(ctx, pr)
	if err != nil {
		return err
	}

	return imp.putNode(ctx, node, path)
}

func (imp *Importer) add(ctx context.Context, reader io.Reader) (ipld.Node, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	splitter := chunk.NewSizeSplitter(reader, chunkSize)

	params := helpers.DagBuilderParams{
		Maxlinks:   helpers.DefaultLinksPerBlock,
		RawLeaves:  true,
		CidBuilder: imp.cidBuilder,
		Dagserv:    imp.bufferedDS,
		NoCopy:     false,
	}

	helper, err := params.New(splitter)
	if err != nil {
		return nil, err
	}

	nd, err := balanced.Layout(helper)
	if err != nil {
		return nil, err
	}

	return nd, imp.bufferedDS.Commit()
}

func (imp *Importer) putNode(ctx context.Context, node ipld.Node, filePath string) error {
	if filePath == "" {
		filePath = filepath.Base(imp.path)
	}

	mr, err := imp.mfsRoot(ctx)
	if err != nil {
		return err
	}

	dir := path.Dir(filePath)
	if dir != "." {
		opts := mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: imp.cidBuilder,
		}

		if err = mfs.Mkdir(mr, dir, opts); err != nil {
			return err
		}
	}

	return mfs.PutNode(mr, filePath, node)
}

func (imp *Importer) mfsRoot(ctx context.Context) (*mfs.Root, error) {
	if imp.root != nil {
		return imp.root, nil
	}

	protoNode := unixfs.EmptyDirNode()
	if err := protoNode.SetCidBuilder(imp.cidBuilder); err != nil {
		return nil, err
	}

	mr, err := mfs.NewRoot(ctx, imp.dagService, protoNode, nil)
	if err != nil {
		return nil, err
	}

	imp.root = mr
	return imp.root, nil
}
