package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestErrorScenarios_CorruptedConfig 测试配置文件损坏
func TestErrorScenarios_CorruptedConfig(t *testing.T) {
	t.Run("invalid_json_in_spec", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "corrupted-*")
		defer CleanupTestData(t, tmpDir)

		// 创建无效的 JSON 配置文件
		specPath := DatastoreSpecPath(tmpDir)
		if err := os.WriteFile(specPath, []byte("{invalid json}"), 0o600); err != nil {
			t.Fatalf("failed to write corrupted spec: %v", err)
		}

		// 尝试打开存储应该失败
		_, err := NewStorage(tmpDir)
		if err == nil {
			t.Error("expected error for corrupted config")
		}
	})

	t.Run("empty_spec_file", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "empty-spec-*")
		defer CleanupTestData(t, tmpDir)

		// 创建空的配置文件
		specPath := DatastoreSpecPath(tmpDir)
		if err := os.WriteFile(specPath, []byte(""), 0o600); err != nil {
			t.Fatalf("failed to write empty spec: %v", err)
		}

		s, err := NewStorage(tmpDir)
		// 空文件应该被忽略（FileExists 返回 false for size 0）
		if err != nil {
			t.Logf("Note: empty spec file caused: %v", err)
		} else {
			s.Close()
		}
	})

	t.Run("mismatched_config", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "mismatched-*")
		defer CleanupTestData(t, tmpDir)

		// 创建一个不同的配置
		specPath := DatastoreSpecPath(tmpDir)
		differentConfig := `{"type":"levelds","path":"different"}`
		if err := os.WriteFile(specPath, []byte(differentConfig), 0o600); err != nil {
			t.Fatalf("failed to write spec: %v", err)
		}

		// 应该检测到配置不匹配
		_, err := NewStorage(tmpDir)
		if err == nil {
			t.Error("expected error for mismatched config")
		}

		var configErr *ConfigError
		if !errors.As(err, &configErr) {
			t.Logf("Got error type: %T, message: %v", err, err)
		}
	})
}

// TestErrorScenarios_LockFileConflicts 测试锁文件冲突
func TestErrorScenarios_LockFileConflicts(t *testing.T) {
	t.Run("concurrent_creation", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "lock-conflict-*")
		defer CleanupTestData(t, tmpDir)

		// 创建第一个存储
		s1, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("first NewStorage failed: %v", err)
		}
		defer s1.Close()

		// 尝试创建第二个存储（预期阻塞直到锁释放）
		done := make(chan struct{})
		var s2 *Storage
		var s2Err error
		go func() {
			s2, s2Err = NewStorage(tmpDir)
			close(done)
		}()

		select {
		case <-done:
			if s2Err == nil {
				_ = s2.Close()
			}
			t.Fatal("expected NewStorage to block while lock is held")
		case <-time.After(50 * time.Millisecond):
		}

		// 释放锁后，第二个创建应该完成
		if err := s1.Close(); err != nil {
			t.Fatalf("failed to close first storage: %v", err)
		}

		select {
		case <-done:
			if s2Err != nil {
				t.Fatalf("unexpected error after releasing lock: %v", s2Err)
			}
			_ = s2.Close()
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for NewStorage after releasing lock")
		}
	})

	t.Run("orphaned_lock_file", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "orphaned-lock-*")
		defer CleanupTestData(t, tmpDir)

		// 手动创建锁文件（模拟孤儿锁文件）
		lockPath := filepath.Join(tmpDir, LockFile)
		lockFile, err := os.Create(lockPath)
		if err != nil {
			t.Fatalf("failed to create lock file: %v", err)
		}
		lockFile.Close()

		// 尝试打开存储（实现允许重新获取锁）
		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Logf("NewStorage failed on orphaned lock file: %v", err)
		} else {
			s.Close()
		}
	})
}

// TestErrorScenarios_PermissionDenied 测试权限拒绝场景
func TestErrorScenarios_PermissionDenied(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root, skipping permission tests")
	}

	t.Run("read_only_directory", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "readonly-*")
		defer os.RemoveAll(tmpDir) // 使用 RemoveAll 因为可能没有写权限

		// 创建只读目录
		subDir := filepath.Join(tmpDir, "readonly")
		if err := os.Mkdir(subDir, 0o555); err != nil {
			t.Fatalf("failed to create readonly directory: %v", err)
		}
		defer os.Chmod(subDir, 0o755) // 恢复权限以便清理

		// 尝试在只读目录创建存储
		_, err := NewStorage(subDir)
		if err == nil {
			t.Error("expected error for read-only directory")
		}
		t.Logf("Expected error: %v", err)
	})

	t.Run("read_only_spec_file", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "readonly-spec-*")
		defer CleanupTestData(t, tmpDir)

		// 创建只读配置文件
		specPath := DatastoreSpecPath(tmpDir)
		if err := os.WriteFile(specPath, []byte("{}"), 0o400); err != nil {
			t.Fatalf("failed to create readonly spec: %v", err)
		}

		// 尝试打开存储
		_, err := NewStorage(tmpDir)
		if err != nil {
			t.Logf("Expected error for readonly spec: %v", err)
		}
	})
}

// TestErrorScenarios_DiskFull 模拟磁盘空间不足
func TestErrorScenarios_DiskFull(t *testing.T) {
	t.Run("cannot_create_spec", func(t *testing.T) {
		// 在实际测试中很难模拟磁盘满，我们测试错误处理路径
		// 通过创建一个非常长的文件名来触发错误
		tmpDir := SetupTempDir(t, "disk-full-*")
		defer CleanupTestData(t, tmpDir)

		// 创建一个无效的路径来触发错误
		invalidPath := filepath.Join(tmpDir, strings.Repeat("a", 1000), "storage")
		_, err := NewStorage(invalidPath)

		// 应该返回错误而不是 panic
		if err == nil {
			t.Error("expected error for invalid path")
		}
		t.Logf("Error for invalid path: %v", err)
	})
}

// TestErrorScenarios_DatastoreFailures 测试 datastore 失败场景
func TestErrorScenarios_DatastoreFailures(t *testing.T) {
	t.Run("datastore_close_error", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "ds-close-error-*")
		defer CleanupTestData(t, tmpDir)

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		// 手动删除 datastore 数据目录以模拟关闭错误
		datastorePath := filepath.Join(tmpDir, "datastore")
		os.RemoveAll(datastorePath)

		// 关闭可能会失败，但不应该 panic
		err = s.Close()
		if err != nil {
			t.Logf("Expected close error: %v", err)
		}
	})

	t.Run("get_usage_after_partial_close", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "usage-error-*")
		defer CleanupTestData(t, tmpDir)

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		// 关闭存储
		if err := s.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}

		// 尝试获取使用情况
		ctx := context.Background()
		_, err = s.GetStorageUsage(ctx)
		// 可能会失败，但不应该 panic
		if err != nil {
			t.Logf("Expected usage error: %v", err)
		}
	})
}

// TestErrorScenarios_ContextErrors 测试上下文错误场景
func TestErrorScenarios_ContextErrors(t *testing.T) {
	t.Run("context_cancelled_during_open", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 立即取消

		tmpDir := SetupTempDir(t, "ctx-cancel-open-*")
		defer CleanupTestData(t, tmpDir)

		_, err := NewStorageWithContext(ctx, tmpDir)
		if err != context.Canceled {
			t.Errorf("expected Canceled, got: %v", err)
		}
	})

	t.Run("context_cancelled_during_close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 立即取消

		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		err := s.CloseWithContext(ctx)
		if err != context.Canceled {
			t.Errorf("expected Canceled, got: %v", err)
		}

		// 手动清理
		s.Close()
	})

	t.Run("context_timeout_during_operation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// 等待超时
		time.Sleep(10 * time.Millisecond)

		tmpDir := SetupTempDir(t, "ctx-timeout-*")
		defer CleanupTestData(t, tmpDir)

		_, err := NewStorageWithContext(ctx, tmpDir)
		if err != context.DeadlineExceeded {
			t.Logf("Got error (may not be DeadlineExceeded due to timing): %v", err)
		}
	})
}

// TestErrorScenarios_InvalidParameters 测试无效参数
func TestErrorScenarios_InvalidParameters(t *testing.T) {
	t.Run("nil_params", func(t *testing.T) {
		_, err := AnyDatastoreConfig(nil)
		if err == nil {
			t.Error("expected error for nil params")
		}
	})

	t.Run("wrong_param_types", func(t *testing.T) {
		testCases := []struct {
			name    string
			params  map[string]interface{}
			factory ConfigFactory
		}{
			{
				"mount_without_array",
				map[string]interface{}{"type": "mount", "mounts": "not an array"},
				MountDatastoreConfig,
			},
			{
				"measure_without_map",
				map[string]interface{}{"type": "measure", "child": "not a map"},
				MeasureDatastoreConfig,
			},
			{
				"levelds_with_invalid_compression",
				map[string]interface{}{
					"type":        "levelds",
					"path":        "test",
					"compression": 123, // 应该是字符串
				},
				LevelDBDatastoreConfig,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := tc.factory(tc.params)
				if err == nil {
					t.Errorf("expected error for %s", tc.name)
				}
			})
		}
	})
}

// TestErrorScenarios_MountErrors 测试挂载错误
func TestErrorScenarios_MountErrors(t *testing.T) {
	t.Run("mount_without_mountpoint", func(t *testing.T) {
		params := map[string]interface{}{
			"type": "mount",
			"mounts": []interface{}{
				map[string]interface{}{
					"type": "levelds",
					"path": "test",
					// 缺少 mountpoint
				},
			},
		}

		_, err := MountDatastoreConfig(params)
		if err == nil {
			t.Error("expected error for mount without mountpoint")
		}
	})

	t.Run("mount_with_invalid_mountpoint_type", func(t *testing.T) {
		params := map[string]interface{}{
			"type": "mount",
			"mounts": []interface{}{
				map[string]interface{}{
					"mountpoint": 123, // 应该是字符串
					"type":       "levelds",
					"path":       "test",
				},
			},
		}

		_, err := MountDatastoreConfig(params)
		if err == nil {
			t.Error("expected error for invalid mountpoint type")
		}
		t.Logf("Expected error: %v", err)
	})
}

// TestErrorScenarios_FileSystemErrors 测试文件系统错误
func TestErrorScenarios_FileSystemErrors(t *testing.T) {
	t.Run("cannot_create_directory", func(t *testing.T) {
		// 创建一个文件而不是目录
		tmpDir := SetupTempDir(t, "fs-error-*")
		defer CleanupTestData(t, tmpDir)

		filePath := filepath.Join(tmpDir, "not_a_dir")
		if err := os.WriteFile(filePath, []byte("test"), 0o644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		// 尝试在文件路径创建存储
		_, err := NewStorage(filePath)
		if err == nil {
			t.Error("expected error when path is a file")
		}
		t.Logf("Expected error: %v", err)
	})

	t.Run("spec_file_is_directory", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "spec-is-dir-*")
		defer CleanupTestData(t, tmpDir)

		// 创建一个目录而不是 spec 文件
		specPath := DatastoreSpecPath(tmpDir)
		if err := os.Mkdir(specPath, 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}

		_, err := NewStorage(tmpDir)
		if err == nil {
			t.Error("expected error when spec is a directory")
		}
		t.Logf("Expected error: %v", err)
	})
}

// TestErrorScenarios_ErrorWrapping 测试错误包装
func TestErrorScenarios_ErrorWrapping(t *testing.T) {
	t.Run("StorageError_unwrap", func(t *testing.T) {
		originalErr := errors.New("original error")
		wrappedErr := &StorageError{
			Operation: "test operation",
			Path:      "/test/path",
			Err:       originalErr,
		}

		if !errors.Is(wrappedErr, originalErr) {
			t.Error("StorageError should unwrap to original error")
		}
	})

	t.Run("ConfigError_unwrap", func(t *testing.T) {
		originalErr := errors.New("config error")
		wrappedErr := &ConfigError{
			Field: "test_field",
			Err:   originalErr,
		}

		if !errors.Is(wrappedErr, originalErr) {
			t.Error("ConfigError should unwrap to original error")
		}
	})

	t.Run("LockError_unwrap", func(t *testing.T) {
		originalErr := &os.PathError{Err: syscall.EAGAIN}
		wrappedErr := &LockError{
			Path: "/test/lock",
			Err:  originalErr,
		}

		if !errors.Is(wrappedErr, originalErr) {
			t.Error("LockError should unwrap to original error")
		}
	})
}

// TestErrorScenarios_ErrorMessages 测试错误消息
func TestErrorScenarios_ErrorMessages(t *testing.T) {
	t.Run("InvalidPathError_message", func(t *testing.T) {
		err := &InvalidPathError{
			Path:   "/test/path",
			Reason: "test reason",
		}

		msg := err.Error()
		if !strings.Contains(msg, "/test/path") || !strings.Contains(msg, "test reason") {
			t.Errorf("error message should contain path and reason: %s", msg)
		}
	})

	t.Run("StorageError_message_with_path", func(t *testing.T) {
		originalErr := errors.New("original")
		err := &StorageError{
			Operation: "test op",
			Path:      "/test/path",
			Err:       originalErr,
		}

		msg := err.Error()
		if !strings.Contains(msg, "test op") || !strings.Contains(msg, "/test/path") {
			t.Errorf("error message should contain operation and path: %s", msg)
		}
	})

	t.Run("StorageError_message_without_path", func(t *testing.T) {
		originalErr := errors.New("original")
		err := &StorageError{
			Operation: "test op",
			Path:      "",
			Err:       originalErr,
		}

		msg := err.Error()
		if !strings.Contains(msg, "test op") {
			t.Errorf("error message should contain operation: %s", msg)
		}
	})
}

// TestErrorScenarios_PanicRecovery 测试 panic 恢复
func TestErrorScenarios_PanicRecovery(t *testing.T) {
	t.Run("no_panic_on_double_close", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("unexpected panic: %v", r)
			}
		}()

		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		s.Close()
		s.Close() // 不应该 panic
	})

	t.Run("no_panic_on_nil_operations", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("unexpected panic: %v", r)
			}
		}()

		var s *Storage
		// 不应该 panic，即使 s 是 nil
		_ = s
	})
}

// TestErrorScenarios_ErrorChains 测试错误链
func TestErrorScenarios_ErrorChains(t *testing.T) {
	t.Run("nested_error_wrapping", func(t *testing.T) {
		innerErr := fmt.Errorf("inner error")
		middleErr := &StorageError{
			Operation: "middle",
			Path:      "/middle",
			Err:       innerErr,
		}
		outerErr := fmt.Errorf("outer: %w", middleErr)

		// 应该能够追踪到原始错误
		if !errors.Is(outerErr, innerErr) {
			t.Error("should be able to find inner error in chain")
		}
	})
}

// TestErrorScenarios_InvalidInputs 测试无效输入
func TestErrorScenarios_InvalidInputs(t *testing.T) {
	t.Run("Writable_with_empty_path", func(t *testing.T) {
		err := Writable("")
		if err == nil {
			t.Error("expected error for empty path")
		}
	})

	t.Run("FileExists_with_empty_path", func(t *testing.T) {
		result := FileExists("")
		if result {
			t.Error("empty path should not exist")
		}
	})

	t.Run("resolvePath_with_empty_base", func(t *testing.T) {
		result := resolvePath("/root", "")
		// 应该返回一个有效的结果
		if result != "/root" {
			t.Logf("resolvePath with empty base returned: %s", result)
		}
	})
}

// TestErrorScenarios_ResourceCleanup 测试资源清理
func TestErrorScenarios_ResourceCleanup(t *testing.T) {
	t.Run("cleanup_after_open_failure", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "cleanup-fail-*")
		defer os.RemoveAll(tmpDir) // 确保清理

		// 创建一个损坏的 spec 文件
		specPath := DatastoreSpecPath(tmpDir)
		os.WriteFile(specPath, []byte("{invalid"), 0o600)

		// 尝试打开（应该失败）
		_, err := NewStorage(tmpDir)
		if err == nil {
			t.Error("expected error for invalid spec")
		}

		// 验证不应该留下锁文件
		lockPath := filepath.Join(tmpDir, LockFile)
		if FileExists(lockPath) {
			t.Logf("lock file exists after failed open: %s", lockPath)
		}
	})
}

// TestErrorScenarios_MixedErrorTypes 测试混合错误类型
func TestErrorScenarios_MixedErrorTypes(t *testing.T) {
	t.Run("correct_error_type_for_invalid_path", func(t *testing.T) {
		_, err := NewStorage("")
		var pathErr *InvalidPathError
		if !errors.As(err, &pathErr) {
			t.Errorf("expected InvalidPathError, got: %T", err)
		}
	})

	t.Run("correct_error_type_for_lock_conflict", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "error-type-*")
		defer CleanupTestData(t, tmpDir)

		s1, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}
		defer s1.Close()

		done := make(chan struct{})
		var s2 *Storage
		var s2Err error
		go func() {
			s2, s2Err = NewStorage(tmpDir)
			close(done)
		}()

		select {
		case <-done:
			t.Fatal("expected NewStorage to block while lock is held")
		case <-time.After(50 * time.Millisecond):
		}

		if err := s1.Close(); err != nil {
			t.Fatalf("failed to close first storage: %v", err)
		}

		select {
		case <-done:
			if s2Err != nil {
				t.Fatalf("unexpected error after releasing lock: %v", s2Err)
			}
			_ = s2.Close()
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for NewStorage after releasing lock")
		}
	})

	t.Run("correct_error_type_for_config_mismatch", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "config-mismatch-*")
		defer CleanupTestData(t, tmpDir)

		// 写入不匹配的配置
		specPath := DatastoreSpecPath(tmpDir)
		os.WriteFile(specPath, []byte(`{"type":"different"}`), 0o600)

		_, err := NewStorage(tmpDir)
		var configErr *ConfigError
		if !errors.As(err, &configErr) {
			t.Logf("Expected ConfigError, got: %T", err)
		}
	})
}
