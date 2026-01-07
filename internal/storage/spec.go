// Package storage 提供了持久化存储的抽象层。
//
// Storage 支持多种后端实现：
//   - LevelDB: 高性能键值存储，用于元数据
//   - FlatFS: 基于文件的块存储
//   - Mount: 将多个存储挂载到不同路径
//
// 基本使用：
//
//	store, err := storage.NewStorage("/path/to/repo")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer store.Close()
//
//	ds := store.Datastore()
//	// 使用 datastore...
package storage

import (
	"bytes"
	"encoding/json"
)

// DiskSpec 表示存储配置的磁盘规范。
//
// DiskSpec 是一个键值对映射，用于序列化存储配置到 JSON。
// 它包含 datastore 类型、路径和其他配置参数。
type DiskSpec map[string]interface{}

// DefaultDiskSpec 返回默认的存储配置。
//
// 默认配置使用 mount 结构：
//   - /blocks: FlatFS 存储用于 IPFS 块
//   - /: LevelDB 存储用于元数据
//
// 返回的配置可以序列化为 JSON 并写入 datastore_spec 文件。
func DefaultDiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			map[string]interface{}{
				"mountpoint": "/blocks",
				"type":       "measure",
				"prefix":     "flatfs.datastore",
				"child": map[string]interface{}{
					"type":      "flatfs",
					"path":      "blocks",
					"sync":      true,
					"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
				},
			},
			map[string]interface{}{
				"mountpoint": "/",
				"type":       "measure",
				"prefix":     "leveldb.datastore",
				"child": map[string]interface{}{
					"type":        "levelds",
					"path":        "datastore",
					"compression": "none",
				},
			},
		},
	}
}

// Bytes 将 DiskSpec 序列化为 JSON 字节数组。
//
// 返回的字节已经过 TrimSpace 处理，适合写入文件。
// 如果序列化失败，会 panic（因为默认配置不应失败）。
func (s DiskSpec) Bytes() []byte {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}

	return bytes.TrimSpace(b)
}

// String 将 DiskSpec 序列化为 JSON 字符串。
//
// 这是 Bytes() 方法的便捷包装，返回字符串而非字节切片。
func (s DiskSpec) String() string {
	return string(s.Bytes())
}
