package storage

import (
	"fmt"

	flatfs "github.com/ipfs/go-ds-flatfs"
)

// flatFsDatastoreConfig FlatFS datastore 的配置。
type flatFsDatastoreConfig struct {
	path     string
	shardFun *flatfs.ShardIdV1
	sync     bool
}

// DiskSpec 返回 FlatFS 的磁盘配置规范。
func (cfg *flatFsDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type":      "flatfs",
		"path":      cfg.path,
		"shardFunc": cfg.shardFun.String(),
	}
}

// Create 使用此配置创建 FlatFS datastore 实例。
//
// 参数：
//
//	path - 根路径（如果是相对路径，会与此配置的 path 结合）
//
// 返回：
//
//	Datastore - FlatFS datastore 实例
//	error - 如果创建失败，返回错误
func (cfg *flatFsDatastoreConfig) Create(path string) (Datastore, error) {
	fullPath := resolvePath(path, cfg.path)

	return flatfs.CreateOrOpen(fullPath, cfg.shardFun, cfg.sync)
}

// FlatFsDatastoreConfig 从配置映射创建 FlatFS datastore 配置。
//
// 参数：
//
//	params - 配置映射，必须包含：
//	  - "path" (string): 数据目录路径
//	  - "shardFunc" (string): 分片函数
//	  - "sync" (bool): 是否在每次写入后同步到磁盘
//
// 返回：
//
//	DatastoreConfig - FlatFS 配置对象
//	error - 如果配置无效，返回错误
func FlatFsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	path, ok := params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not a string")
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
