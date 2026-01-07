package importer

import (
	"context"
	"path/filepath"

	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/mfs"
	ipld "github.com/ipfs/go-ipld-format"
)

// mfsRoot initializes or returns the cached MFS root
func (imp *Importer) mfsRoot(ctx context.Context) (*mfs.Root, error) {
	if imp.root != nil {
		return imp.root, nil
	}

	protoNode := unixfs.EmptyDirNode()
	if err := protoNode.SetCidBuilder(imp.cidBuilder); err != nil {
		return nil, err
	}

	mr, err := mfs.NewRoot(ctx, imp.dagService, protoNode, nil, nil)
	if err != nil {
		return nil, err
	}

	imp.root = mr
	return imp.root, nil
}

// flushMFSRoot flushes the MFS root to DAG service
func (imp *Importer) flushMFSRoot(ctx context.Context) error {
	if imp.root == nil {
		return ErrMfsRootNil
	}
	return imp.root.FlushMemFree(ctx)
}

// putNodeToMFS places a node at the given path in MFS
func (imp *Importer) putNodeToMFS(ctx context.Context, node ipld.Node, filePath string) error {
	mr, err := imp.mfsRoot(ctx)
	if err != nil {
		return err
	}

	dir := filepath.Dir(filePath)
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
