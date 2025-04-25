package repository

import (
	"context"
	"fmt"
	"github.com/Tragoedia0722/repository/internal/storage"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	cid2 "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
	"os"
)

type Repository struct {
	storage    *storage.Storage
	blockStore blockstore.Blockstore
	builder    cid2.Builder
}

func NewRepository(path string) (*Repository, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, os.ModePerm); err != nil {
			return nil, err
		}
	}

	s, err := storage.NewStorage(path)
	if err != nil {
		return nil, err
	}

	return &Repository{
		storage:    s,
		blockStore: blockstore.NewBlockstore(s.Datastore()),
		builder: cid2.V1Builder{
			Codec:    uint64(multicodec.DagPb),
			MhType:   mh.SHA2_256,
			MhLength: -1,
		},
	}, nil

}

func (r *Repository) BlockStore() blockstore.Blockstore {
	return r.blockStore
}

func (r *Repository) DataStore() storage.Datastore {
	return r.storage.Datastore()
}

func (r *Repository) Usage(ctx context.Context) (uint64, error) {
	return r.storage.GetStorageUsage(ctx)
}

func (r *Repository) Close() error {
	if r.storage == nil {
		return nil
	}

	return r.storage.Close()
}

func (r *Repository) Destroy() error {
	if r.storage == nil {
		return nil
	}

	return r.storage.Destroy()
}

func (r *Repository) PutBlockWithCid(ctx context.Context, cid string, bytes []byte) error {
	c, err := cid2.Parse(cid)
	if err != nil {
		return err
	}

	sum, err := r.builder.Sum(bytes)
	if err != nil {
		return err
	}

	if sum.Hash().String() != c.Hash().String() {
		return fmt.Errorf("cid hash mismatch: expected %s, got %s", c.Hash(), sum.Hash())
	}

	blk, err := blocks.NewBlockWithCid(bytes, sum)
	if err != nil {
		return err
	}

	err = r.blockStore.Put(ctx, blk)
	if err != nil {
		return err
	}

	return nil
}

func (r *Repository) PutBlock(ctx context.Context, bytes []byte) (*cid2.Cid, error) {
	sum, err := r.builder.Sum(bytes)
	if err != nil {
		return nil, err
	}

	blk, err := blocks.NewBlockWithCid(bytes, sum)
	if err != nil {
		return nil, err
	}

	err = r.blockStore.Put(ctx, blk)
	if err != nil {
		return nil, err
	}

	return &sum, nil
}

func (r *Repository) PutManyBlocks(ctx context.Context, bytes [][]byte) ([]*cid2.Cid, error) {
	blks := make([]blocks.Block, 0, len(bytes))
	cids := make([]*cid2.Cid, 0, len(bytes))

	for _, b := range bytes {
		sum, err := r.builder.Sum(b)
		if err != nil {
			return nil, err
		}
		cids = append(cids, &sum)

		blk, err := blocks.NewBlockWithCid(b, sum)
		if err != nil {
			return nil, err
		}
		blks = append(blks, blk)
	}

	err := r.blockStore.PutMany(ctx, blks)
	if err != nil {
		return nil, err
	}

	return cids, nil
}

func (r *Repository) HasBlock(ctx context.Context, cid string) (bool, error) {
	c, err := cid2.Parse(cid)
	if err != nil {
		return false, err
	}

	return r.blockStore.Has(ctx, c)
}

func (r *Repository) HasAllBlocks(ctx context.Context, cids []string) (bool, error) {
	for _, cid := range cids {
		c, err := cid2.Parse(cid)
		if err != nil {
			return false, err
		}

		has, err := r.blockStore.Has(ctx, c)
		if err != nil {
			return false, err
		}
		if !has {
			return false, nil
		}
	}

	return true, nil
}

func (r *Repository) GetRawData(ctx context.Context, cid string) ([]byte, error) {
	c, err := cid2.Parse(cid)
	if err != nil {
		return nil, err
	}

	blk, err := r.blockStore.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	return blk.RawData(), nil
}

func (r *Repository) DelBlock(ctx context.Context, cid string) error {
	c, err := cid2.Parse(cid)
	if err != nil {
		return err
	}

	return r.blockStore.DeleteBlock(ctx, c)
}
