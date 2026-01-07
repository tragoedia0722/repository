package storage

import (
	"fmt"
	"strings"
	"sync"

	ds "github.com/ipfs/go-datastore"
)

// Datastore 是数据存储接口。
//
// 它嵌入 ds.Batching 接口，提供批处理操作能力。
type Datastore interface {
	ds.Batching
}

// DatastoreConfig 定义了创建 datastore 所需的配置。
//
// 实现此接口的类型需要提供：
//   - DiskSpec(): 返回磁盘配置规范
//   - Create(): 使用配置创建 datastore 实例
type DatastoreConfig interface {
	DiskSpec() DiskSpec
	Create(path string) (Datastore, error)
}

// ConfigFactory 是从配置映射创建 DatastoreConfig 的工厂函数。
type ConfigFactory func(map[string]interface{}) (DatastoreConfig, error)

// configRegistry 管理 datastore 类型配置的注册表。
type configRegistry struct {
	mu        sync.RWMutex
	factories map[string]ConfigFactory
}

// globalConfigRegistry 是全局的配置注册表实例。
var globalConfigRegistry = &configRegistry{
	factories: make(map[string]ConfigFactory),
}

var registryOnce sync.Once

// ensureInitialized 确保注册表已初始化（使用 sync.Once 保证只执行一次）。
func ensureInitialized() {
	registryOnce.Do(func() {
		globalConfigRegistry.registerDefaults()
	})
}

// registerDefaults 注册所有默认的 datastore 类型。
func (r *configRegistry) registerDefaults() {
	r.factories["mount"] = MountDatastoreConfig
	r.factories["measure"] = MeasureDatastoreConfig
	r.factories["levelds"] = LevelDBDatastoreConfig
	r.factories["flatfs"] = FlatFsDatastoreConfig
}

// register 注册一个新的 datastore 配置工厂。
//
// 如果同名类型已存在，会覆盖原有的工厂。
// 此方法是线程安全的。
func (r *configRegistry) register(name string, factory ConfigFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = factory
}

// get 获取指定类型的配置工厂。
//
// 返回的工厂函数可能是 nil，如果类型不存在。
// 此方法是线程安全的。
func (r *configRegistry) get(name string) ConfigFactory {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.factories[name]
}

// list 返回所有已注册的 datastore 类型名称。
//
// 此方法是线程安全的。
func (r *configRegistry) list() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.factories))
	for name := range r.factories {
		types = append(types, name)
	}
	return types
}

// AnyDatastoreConfig 根据配置映射创建任意类型的 datastore 配置。
//
// 参数：
//
//	params - 配置映射，必须包含 "type" 字段指定 datastore 类型
//
// 返回：
//
//	DatastoreConfig - 对应类型的配置对象
//	error - 如果类型缺失或未知，返回错误
//
// 支持的类型：mount, measure, levelds, flatfs
func AnyDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	// 确保注册表已初始化
	ensureInitialized()

	datastoreType, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("'type' field missing or not a string")
	}

	// 标准化类型名称（小写）
	datastoreType = strings.ToLower(datastoreType)

	configFactory := globalConfigRegistry.get(datastoreType)
	if configFactory == nil {
		availableTypes := globalConfigRegistry.list()
		return nil, fmt.Errorf("unknown datastore type: %s (available: %v)",
			datastoreType, availableTypes)
	}

	return configFactory(params)
}
