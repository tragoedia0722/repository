package storage

import (
	"fmt"
	flatfs "github.com/ipfs/go-ds-flatfs"
	"path/filepath"
)

type flatFsDatastoreConfig struct {
	path     string
	shardFun *flatfs.ShardIdV1
	sync     bool
}

func (c *flatFsDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type":      "flatfs",
		"path":      c.path,
		"shardFunc": c.shardFun.String(),
	}
}

func (c *flatFsDatastoreConfig) Create(path string) (Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	return flatfs.CreateOrOpen(p, c.shardFun, c.sync)
}

func FlatFsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	path, ok := params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not boolean")
	}

	shardFunc, ok := params["shardFunc"].(string)
	if !ok {
		return nil, fmt.Errorf("'shardFunc' field is missing or not a string")
	}

	shardFun, err := flatfs.ParseShardFunc(shardFunc)
	if err != nil {
		return nil, err
	}

	sync, ok := params["sync"].(bool)
	if !ok {
		return nil, fmt.Errorf("'sync' field is missing or not boolean")
	}

	return &flatFsDatastoreConfig{
		path:     path,
		shardFun: shardFun,
		sync:     sync,
	}, nil
}
