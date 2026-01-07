package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	ds "github.com/ipfs/go-datastore"
	measure "github.com/ipfs/go-ds-measure"
	"github.com/mitchellh/go-homedir"
	"github.com/rogpeppe/go-internal/lockedfile"
)

const LockFile = ".storage.lock"

// Storage 表示一个持久化存储实例。
//
// Storage 管理底层数据存储的生命周期，包括：
//   - 创建和初始化存储
//   - 管理锁文件防止并发访问
//   - 提供统一的 datastore 访问
//   - 优雅关闭和清理
//
// 使用示例：
//
//	store, err := NewStorage("/path/to/repo")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer store.Close()
//
//	ds := store.Datastore()
//	// 使用 datastore...
type Storage struct {
	mu        sync.Mutex
	closed    atomic.Bool
	path      string
	lockFile  *lockedfile.File
	datastore Datastore
}

// Datastore 返回底层的数据存储实例。
//
// 注意：返回的是同一个实例，调用者不应关闭它。
// 此方法是线程安全的。datastore 在初始化后不会改变，
// 因此无需加锁保护。
func (s *Storage) Datastore() Datastore {
	return s.datastore
}

// GetStorageUsage 返回存储使用的磁盘空间。
//
// 参数：
//
//	ctx - 用于取消操作的上下文
//
// 返回：
//
//	uint64 - 使用的字节数
//	error - 如果获取失败，返回错误
func (s *Storage) GetStorageUsage(ctx context.Context) (uint64, error) {
	return ds.DiskUsage(ctx, s.Datastore())
}

// Close 关闭存储并释放资源。
//
// Close 是幂等的，多次调用不会返回错误。
// 关闭后，Storage 对象不再可用。
//
// 此方法使用 context.Background()。如果需要超时或取消控制，
// 请使用 CloseWithContext。
//
// 返回：
//
//	error - 如果关闭过程中出现错误，返回错误汇总
func (s *Storage) Close() error {
	return s.CloseWithContext(context.Background())
}

// CloseWithContext 关闭存储并释放资源，支持上下文控制。
//
// CloseWithContext 是幂等的，多次调用不会返回错误。
// 关闭后，Storage 对象不再可用。
//
// 参数：
//
//	ctx - 用于取消操作或设置超时的上下文
//
// 返回：
//
//	error - 如果关闭过程中出现错误或上下文取消，返回错误
func (s *Storage) CloseWithContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 快速检查是否已关闭（双重检查锁定模式）
	if s.closed.Load() {
		return nil
	}

	// 获取 datastore 并标记为关闭
	s.mu.Lock()
	var ds Datastore
	if !s.closed.Load() {
		ds = s.datastore
		s.closed.Store(true)
	}
	s.mu.Unlock()

	if ds == nil {
		return nil
	}

	// 在锁外执行关闭操作，避免持有锁过久
	var errs []error
	if err := ds.Close(); err != nil {
		errs = append(errs, fmt.Errorf("datastore close error: %v", err))
	}

	appendErrors(&errs, s.closeLockFile())

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}

// Destroy 销毁存储并删除所有数据。
//
// 如果存储已经关闭，只删除数据目录。
// 此操作不可逆，请谨慎使用。
//
// 此方法使用 context.Background()。如果需要超时或取消控制，
// 请使用 DestroyWithContext。
//
// 返回：
//
//	error - 如果销毁失败，返回错误
func (s *Storage) Destroy() error {
	return s.DestroyWithContext(context.Background())
}

// DestroyWithContext 销毁存储并删除所有数据，支持上下文控制。
//
// 如果存储已经关闭，只删除数据目录。
// 此操作不可逆，请谨慎使用。
//
// 参数：
//
//	ctx - 用于取消操作或设置超时的上下文
//
// 返回：
//
//	error - 如果销毁失败或上下文取消，返回错误
func (s *Storage) DestroyWithContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() {
		return os.RemoveAll(s.path)
	}

	if err := s.datastore.Close(); err != nil {
		return err
	}

	s.closed.Store(true)

	err := s.closeLockFile()
	if err != nil {
		return fmt.Errorf("failed to close lock file: %v", err)
	}

	return os.RemoveAll(s.path)
}

// NewStorage 创建或打开一个存储实例。
//
// 如果存储目录不存在，会创建新的存储并初始化配置。
// 如果存储已存在，会打开现有存储并验证配置匹配。
//
// 此方法使用 context.Background()。如果需要超时或取消控制，
// 请使用 NewStorageWithContext。
//
// 参数：
//
//	path - 存储目录路径
//
// 返回：
//
//	*Storage - 存储实例
//	error - 如果创建或打开失败，返回错误
func NewStorage(path string) (*Storage, error) {
	return NewStorageWithContext(context.Background(), path)
}

// NewStorageWithContext 创建或打开一个存储实例，支持上下文控制。
//
// 如果存储目录不存在，会创建新的存储并初始化配置。
// 如果存储已存在，会打开现有存储并验证配置匹配。
//
// 参数：
//
//	ctx - 用于取消操作或设置超时的上下文
//	path - 存储目录路径
//
// 返回：
//
//	*Storage - 存储实例
//	error - 如果创建或打开失败或上下文取消，返回错误
func NewStorageWithContext(ctx context.Context, path string) (*Storage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err := initSpec(path, DefaultDiskSpec()); err != nil {
		return nil, err
	}

	return openWithContext(ctx, path)
}

// initSpec 初始化存储配置文件。
//
// 如果配置文件已存在，不做任何事。
// 否则创建默认配置并写入文件。
func initSpec(path string, conf map[string]interface{}) error {
	specPath := DatastoreSpecPath(path)
	if FileExists(specPath) {
		return nil
	}

	dsc, err := AnyDatastoreConfig(conf)
	if err != nil {
		return err
	}

	bytes := dsc.DiskSpec().Bytes()

	return os.WriteFile(specPath, bytes, 0o600)
}

// open 打开现有存储实例。
//
// 创建存储结构、获取锁文件、验证可写性并打开 datastore。
func open(path string) (*Storage, error) {
	return openWithContext(context.Background(), path)
}

// openWithContext 打开现有存储实例，支持上下文控制。
//
// 创建存储结构、获取锁文件、验证可写性并打开 datastore。
func openWithContext(ctx context.Context, path string) (*Storage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s, err := newStorage(path)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	lockPath := filepath.Join(s.path, LockFile)

	lockFile, err := createLockFile(lockPath)
	if err != nil {
		return nil, err
	}
	s.lockFile = lockFile

	defer func(e *error) {
		if *e != nil {
			_ = s.lockFile.Close()
		}
	}(&err)

	if err = Writable(s.path); err != nil {
		return nil, err
	}

	if err = s.openDatastore(); err != nil {
		return nil, err
	}

	return s, nil
}

// newStorage 创建未初始化的存储结构。
func newStorage(path string) (*Storage, error) {
	if path == "" {
		return nil, &InvalidPathError{
			Path:   path,
			Reason: "path cannot be empty",
		}
	}

	expPath, err := homedir.Expand(filepath.Clean(path))
	if err != nil {
		return nil, &InvalidPathError{
			Path:   path,
			Reason: err.Error(),
		}
	}

	return &Storage{path: expPath}, nil
}

// openDatastore 打开底层数据存储。
//
// 读取现有配置，验证与默认配置匹配，然后创建 datastore。
func (s *Storage) openDatastore() error {
	if err := s.validateConfig(); err != nil {
		return err
	}

	dsc, err := AnyDatastoreConfig(DefaultDiskSpec())
	if err != nil {
		return &ConfigError{
			Field: "type",
			Err:   err,
		}
	}

	d, err := dsc.Create(s.path)
	if err != nil {
		return &StorageError{
			Operation: "create datastore",
			Path:      s.path,
			Err:       err,
		}
	}

	s.datastore = measure.New("ipfs.storage.datastore", d)
	return nil
}

// validateConfig 验证现有配置与默认配置匹配。
func (s *Storage) validateConfig() error {
	defaultConfig, err := AnyDatastoreConfig(DefaultDiskSpec())
	if err != nil {
		return &ConfigError{
			Field: "default",
			Err:   err,
		}
	}

	expectedSpec := defaultConfig.DiskSpec().String()
	actualSpec, err := s.readSpec()
	if err != nil {
		return &StorageError{
			Operation: "read config",
			Path:      s.path,
			Err:       err,
		}
	}

	if actualSpec != expectedSpec {
		return &ConfigError{
			Field: "datastore_spec",
			Value: actualSpec,
			Err:   fmt.Errorf("does not match expected config: %s", expectedSpec),
		}
	}

	return nil
}

// readSpec 从磁盘读取存储配置。
func (s *Storage) readSpec() (string, error) {
	path := DatastoreSpecPath(s.path)

	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(b)), nil
}

// createLockFile 创建并初始化锁文件。
//
// 锁文件用于防止多个进程同时访问同一存储。
// 将当前进程 PID 写入锁文件。
func createLockFile(lockPath string) (*lockedfile.File, error) {
	lockfile, err := lockedfile.Create(lockPath)
	if err != nil {
		if os.IsExist(err) {
			return nil, &LockError{
				Path: lockPath,
				Err:  fmt.Errorf("lock file already exists: %w", err),
			}
		}

		return nil, &LockError{
			Path: lockPath,
			Err:  err,
		}
	}

	pidBytes := []byte(strconv.Itoa(os.Getpid()))
	if _, err = lockfile.Write(pidBytes); err != nil {
		_ = lockfile.Close()
		_ = os.Remove(lockPath)
		return nil, &LockError{
			Path: lockPath,
			Err:  fmt.Errorf("failed to write PID: %w", err),
		}
	}

	return lockfile, nil
}

// closeLockFile 关闭并删除锁文件。
func (s *Storage) closeLockFile() error {
	if s.lockFile != nil {
		if err := s.lockFile.Close(); err != nil {
			return &LockError{
				Path: s.lockFile.Name(),
				Err:  fmt.Errorf("failed to close: %w", err),
			}
		}

		lockPath := s.lockFile.Name()
		if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
			return &LockError{
				Path: lockPath,
				Err:  fmt.Errorf("failed to remove: %w", err),
			}
		}
	}
	return nil
}

// appendErrors 将错误添加到错误列表（如果非 nil）。
func appendErrors(errs *[]error, newErr error) {
	if newErr != nil {
		*errs = append(*errs, newErr)
	}
}
