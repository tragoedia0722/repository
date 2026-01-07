package storage

import (
	"fmt"

	"github.com/ipfs/go-ds-measure"
)

// measureDatastoreConfig 带度量功能的 datastore 配置。
type measureDatastoreConfig struct {
	child  DatastoreConfig
	prefix string
}

// MeasureDatastoreConfig 从配置映射创建 measure datastore 配置。
//
// Measure datastore 包装另一个 datastore 并收集度量信息。
//
// 参数：
//
//	params - 配置映射，必须包含：
//	  - "child" (map): 子 datastore 配置
//	  - "prefix" (string): 度量前缀
//
// 返回：
//
//	DatastoreConfig - Measure 配置对象
//	error - 如果配置无效，返回错误
func MeasureDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	childField, ok := params["child"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'child' field is missing or not a map")
	}

	child, err := AnyDatastoreConfig(childField)
	if err != nil {
		return nil, err
	}

	prefix, ok := params["prefix"].(string)
	if !ok {
		return nil, fmt.Errorf("'prefix' field was missing or not a string")
	}

	return &measureDatastoreConfig{child, prefix}, nil
}

// DiskSpec 返回子 datastore 的磁盘配置（measure 不改变配置）。
func (cfg *measureDatastoreConfig) DiskSpec() DiskSpec {
	return cfg.child.DiskSpec()
}

// Create 使用此配置创建带度量的 datastore 实例。
func (cfg *measureDatastoreConfig) Create(path string) (Datastore, error) {
	child, err := cfg.child.Create(path)
	if err != nil {
		return nil, err
	}

	return measure.New(cfg.prefix, child), nil
}
