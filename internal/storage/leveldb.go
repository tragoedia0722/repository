package storage

import (
	"fmt"
	"path/filepath"

	levelds "github.com/ipfs/go-ds-leveldb"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
)

type levelDBDatastoreConfig struct {
	path        string
	compression ldbopts.Compression
}

func LevelDBDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	path, ok := params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not string")
	}

	var compression ldbopts.Compression
	switch cm := params["compression"]; cm {
	case "none":
		compression = ldbopts.NoCompression
	case "snappy":
		compression = ldbopts.SnappyCompression
	case "", nil:
		compression = ldbopts.DefaultCompression
	default:
		return nil, fmt.Errorf("unrecognized value for compression: %s", cm)
	}

	return &levelDBDatastoreConfig{
		path:        path,
		compression: compression,
	}, nil
}

func (c *levelDBDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "levelds",
		"path": c.path,
	}
}

func (c *levelDBDatastoreConfig) Create(path string) (Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	return levelds.NewDatastore(p, &levelds.Options{
		Compression: c.compression,
	})
}
