package storage

import (
	"fmt"
	"sort"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/mount"
)

// mountDatastoreConfig 挂载点 datastore 的配置。
type mountDatastoreConfig struct {
	mounts []mountItem
}

// mountItem 表示单个挂载点。
type mountItem struct {
	ds     DatastoreConfig
	prefix ds.Key
}

// MountDatastoreConfig 从配置映射创建 mount datastore 配置。
//
// Mount datastore 将多个 datastore 挂载到不同的路径前缀。
//
// 参数：
//
//	params - 配置映射，必须包含：
//	  - "mounts" ([]map): 挂载点数组，每个包含：
//	    - "mountpoint" (string): 挂载路径
//	    - 以及 datastore 配置
//
// 返回：
//
//	DatastoreConfig - Mount 配置对象
//	error - 如果配置无效，返回错误
func MountDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var config mountDatastoreConfig

	mounts, ok := params["mounts"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("'mounts' field is missing or not an array")
	}

	for _, item := range mounts {
		mountParams, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected map for mountpoint")
		}

		child, err := AnyDatastoreConfig(mountParams)
		if err != nil {
			return nil, err
		}

		prefix, found := mountParams["mountpoint"]
		if !found {
			return nil, fmt.Errorf("no 'mountpoint' on mount")
		}

		prefixStr, ok := prefix.(string)
		if !ok {
			return nil, fmt.Errorf("'mountpoint' must be a string, got %T", prefix)
		}

		config.mounts = append(config.mounts, mountItem{
			ds:     child,
			prefix: ds.NewKey(prefixStr),
		})
	}

	sort.Slice(config.mounts,
		func(i, j int) bool {
			return config.mounts[i].prefix.String() > config.mounts[j].prefix.String()
		})

	return &config, nil
}

// DiskSpec 返回 mount 的磁盘配置规范。
func (cfg *mountDatastoreConfig) DiskSpec() DiskSpec {
	spec := map[string]interface{}{"type": "mount"}
	mounts := make([]interface{}, len(cfg.mounts))

	for i, m := range cfg.mounts {
		mountSpec := m.ds.DiskSpec()
		if mountSpec == nil {
			mountSpec = make(map[string]interface{})
		}

		mountSpec["mountpoint"] = m.prefix.String()
		mounts[i] = mountSpec
	}

	spec["mounts"] = mounts

	return spec
}

// Create 使用此配置创建 mount datastore 实例。
func (cfg *mountDatastoreConfig) Create(path string) (Datastore, error) {
	mounts := make([]mount.Mount, len(cfg.mounts))

	for i, m := range cfg.mounts {
		store, err := m.ds.Create(path)
		if err != nil {
			return nil, err
		}

		mounts[i].Datastore = store
		mounts[i].Prefix = m.prefix
	}

	return mount.New(mounts), nil
}
