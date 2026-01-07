package storage

import (
	"fmt"

	levelds "github.com/ipfs/go-ds-leveldb"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
)

// levelDBDatastoreConfig LevelDB datastore 的配置。
type levelDBDatastoreConfig struct {
	path        string
	compression ldbopts.Compression
}

// LevelDBDatastoreConfig 从配置映射创建 LevelDB datastore 配置。
//
// 参数：
//
//	params - 配置映射，必须包含：
//	  - "path" (string): 数据库路径
//	  - "compression" (string, 可选): 压缩类型 ("none", "snappy", 或默认)
//
// 返回：
//
//	DatastoreConfig - LevelDB 配置对象
//	error - 如果配置无效，返回错误
func LevelDBDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	path, ok := params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not string")
	}

	var compression ldbopts.Compression
	switch compressionValue := params["compression"]; compressionValue {
	case "none":
		compression = ldbopts.NoCompression
	case "snappy":
		compression = ldbopts.SnappyCompression
	case "", nil:
		compression = ldbopts.DefaultCompression
	default:
		return nil, fmt.Errorf("unrecognized value for compression: %s", compressionValue)
	}

	return &levelDBDatastoreConfig{
		path:        path,
		compression: compression,
	}, nil
}

// DiskSpec 返回 LevelDB 的磁盘配置规范。
func (cfg *levelDBDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "levelds",
		"path": cfg.path,
	}
}

// Create 使用此配置创建 LevelDB datastore 实例。
//
// 参数：
//
//	path - 根路径（如果是相对路径，会与此配置的 path 结合）
//
// 返回：
//
//	Datastore - LevelDB datastore 实例
//	error - 如果创建失败，返回错误
func (cfg *levelDBDatastoreConfig) Create(path string) (Datastore, error) {
	fullPath := resolvePath(path, cfg.path)

	return levelds.NewDatastore(fullPath, &levelds.Options{
		Compression: cfg.compression,
	})
}
