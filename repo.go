package main

import (
	"context"
	"git.hificloud.net/nas2024/cloud/demeter/repo/pkg/storage"
	"github.com/ipfs/boxo/blockstore"
)

type Repo struct {
	ctx        context.Context
	cancel     context.CancelFunc
	storage    storage.Storage
	blockStore blockstore.Blockstore
	chunkSize  int64
}

func (r *Repo) BlockStore() blockstore.Blockstore {
	return r.blockStore
}

func (r *Repo) DataStore() storage.Datastore {
	return r.storage.Datastore()
}
