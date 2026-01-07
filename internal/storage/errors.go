package storage

import (
	"fmt"
)

// StorageError 表示存储操作期间的错误。
type StorageError struct {
	// Operation 是正在执行的操作
	Operation string
	// Path 是相关的存储路径
	Path string
	// Err 是底层错误
	Err error
}

// Error 实现 error 接口。
func (e *StorageError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("%s failed at %s: %v", e.Operation, e.Path, e.Err)
	}
	return fmt.Sprintf("%s failed: %v", e.Operation, e.Err)
}

// Unwrap 返回底层错误，支持 errors.Is 和 errors.As。
func (e *StorageError) Unwrap() error {
	return e.Err
}

// ConfigError 表示配置相关的错误。
type ConfigError struct {
	// Field 是出错的配置字段
	Field string
	// Value 是字段值（可选）
	Value interface{}
	// Err 是底层错误
	Err error
}

// Error 实现 error 接口。
func (e *ConfigError) Error() string {
	if e.Value != nil {
		return fmt.Sprintf("config field '%s' (value: %v): %v", e.Field, e.Value, e.Err)
	}
	return fmt.Sprintf("config field '%s': %v", e.Field, e.Err)
}

// Unwrap 返回底层错误，支持 errors.Is 和 errors.As。
func (e *ConfigError) Unwrap() error {
	return e.Err
}

// LockError 表示锁文件相关的错误。
type LockError struct {
	// Path 是锁文件路径
	Path string
	// Err 是底层错误
	Err error
}

// Error 实现 error 接口。
func (e *LockError) Error() string {
	return fmt.Sprintf("lock file error at %s: %v", e.Path, e.Err)
}

// Unwrap 返回底层错误，支持 errors.Is 和 errors.As。
func (e *LockError) Unwrap() error {
	return e.Err
}

// InvalidPathError 表示无效的路径错误。
type InvalidPathError struct {
	Path   string
	Reason string
}

// Error 实现 error 接口。
func (e *InvalidPathError) Error() string {
	return fmt.Sprintf("invalid path '%s': %s", e.Path, e.Reason)
}
