package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"io"
)

// calcPackage creates a package hash from a list of block CIDs
func (imp *Importer) calcPackage(blocks []string) Package {
	builder := strings.Builder{}
	builder.Grow(len(blocks) * cidStringBufferSize)

	for _, block := range blocks {
		builder.WriteString(block)
	}

	hash := sha256.Sum256([]byte(builder.String()))

	return Package{
		Hash:   hex.EncodeToString(hash[:]),
		Blocks: blocks,
	}
}

// collectBlocks walks the DAG and collects all block CIDs
func (imp *Importer) collectBlocks(ctx context.Context, root ipld.Node) ([]string, error) {
	cidSet := cid.NewSet()
	if err := merkledag.Walk(ctx, merkledag.GetLinksWithDAG(imp.dagService), root.Cid(), func(c cid.Cid) bool {
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

	return links, nil
}

// createPackages creates packages from collected blocks
func (imp *Importer) createPackages(blocks []string) []Package {
	packages := make([]Package, 0)
	currentBlocks := make([]string, 0, initialBlockCapacity)

	for _, link := range blocks {
		currentBlocks = append(currentBlocks, link)

		if len(currentBlocks) >= blocksPerPackage {
			packages = append(packages, imp.calcPackage(currentBlocks))
			currentBlocks = make([]string, 0, initialBlockCapacity)
		}
	}

	if len(currentBlocks) > 0 {
		packages = append(packages, imp.calcPackage(currentBlocks))
	}

	return packages
}

// buildDAGFromFile chunks a file reader and builds a DAG
func (imp *Importer) buildDAGFromFile(ctx context.Context, reader io.Reader) (ipld.Node, error) {
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

	param, err := params.New(splitter)
	if err != nil {
		return nil, err
	}

	nd, err := balanced.Layout(param)
	if err != nil {
		return nil, err
	}

	return nd, imp.bufferedDS.Commit()
}
