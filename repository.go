package main

import (
	"context"
	"git.hificloud.net/nas2024/cloud/demeter/repository/pkg/storage"
	"github.com/ipfs/boxo/blockstore"
	"os"
)

type Repository struct {
	storage    *storage.Storage
	blockStore blockstore.Blockstore
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
