package validator

import (
	"context"
	"fmt"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

type Validator struct {
	blockStore blockstore.Blockstore
	dagService ipld.DAGService
}

type Result struct {
	IsComplete    bool
	MissingBlocks []string
	InvalidBlocks []string
	TotalSize     int64
	ReachableSize int64
	CanRestore    bool
	ErrorDetails  []string
}

func NewValidator(blockStore blockstore.Blockstore) *Validator {
	bs := blockservice.New(blockStore, nil)
	return &Validator{
		blockStore: blockStore,
		dagService: merkledag.NewDAGService(bs),
	}
}

func (v *Validator) Validate(ctx context.Context, rootCid string, blocks []string) (*Result, error) {
	result := &Result{
		MissingBlocks: make([]string, 0),
		InvalidBlocks: make([]string, 0),
		ErrorDetails:  make([]string, 0),
	}

	theRootCid, err := cid.Decode(rootCid)
	if err != nil {
		return nil, fmt.Errorf("invalid root CID: %v", err)
	}

	blocksSet := make(map[string]bool)
	for _, cidStr := range blocks {
		c, e1 := cid.Decode(cidStr)
		if e1 != nil {
			result.InvalidBlocks = append(result.InvalidBlocks, cidStr)
			result.ErrorDetails = append(result.ErrorDetails, fmt.Sprintf("invalid CID: %s", cidStr))
			continue
		}

		has, e2 := v.blockStore.Has(ctx, c)
		if e2 != nil {
			result.ErrorDetails = append(result.ErrorDetails, fmt.Sprintf("error checking block %s: %v", cidStr, e2))
			continue
		}

		if !has {
			result.MissingBlocks = append(result.MissingBlocks, cidStr)
		}
		blocksSet[cidStr] = has
	}

	requiredBlocks := make(map[string]bool)
	reachableSize, err := v.walkDAG(ctx, theRootCid, requiredBlocks)
	if err != nil {
		result.ErrorDetails = append(result.ErrorDetails, fmt.Sprintf("DAG traversal failed: %v", err))
		result.CanRestore = false
		return result, nil
	}

	result.ReachableSize = reachableSize

	for requiredCid := range requiredBlocks {
		if !blocksSet[requiredCid] {
			result.MissingBlocks = append(result.MissingBlocks, requiredCid)
		}
	}

	result.IsComplete = len(result.MissingBlocks) == 0 && len(result.InvalidBlocks) == 0
	result.CanRestore = result.IsComplete

	return result, nil
}

func (v *Validator) walkDAG(ctx context.Context, rootCid cid.Cid, requiredBlocks map[string]bool) (int64, error) {
	var totalSize int64

	err := merkledag.Walk(ctx, merkledag.GetLinksWithDAG(v.dagService), rootCid, func(c cid.Cid) bool {
		cidStr := c.String()
		if requiredBlocks[cidStr] {
			return false
		}
		requiredBlocks[cidStr] = true

		if block, err := v.blockStore.Get(ctx, c); err == nil {
			totalSize += int64(len(block.RawData()))
		}

		return true
	}, merkledag.Concurrent())

	return totalSize, err
}
