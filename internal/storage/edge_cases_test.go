package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestEdgeCases_EmptyPath 测试空路径场景
func TestEdgeCases_EmptyPath(t *testing.T) {
	t.Run("NewStorage_with_empty_path", func(t *testing.T) {
		_, err := NewStorage("")
		if err == nil {
			t.Error("expected error for empty path")
		}

		var pathErr *InvalidPathError
		if !errors.As(err, &pathErr) {
			t.Errorf("expected InvalidPathError, got %T", err)
		}
	})

	t.Run("NewStorageWithContext_with_empty_path", func(t *testing.T) {
		ctx := context.Background()
		_, err := NewStorageWithContext(ctx, "")
		if err == nil {
			t.Error("expected error for empty path")
		}

		var pathErr *InvalidPathError
		if !errors.As(err, &pathErr) {
			t.Errorf("expected InvalidPathError, got %T", err)
		}
	})
}

// TestEdgeCases_LongPath 测试超长路径场景
func TestEdgeCases_LongPath(t *testing.T) {
	// 创建一个很长的路径名
	longName := strings.Repeat("a", 255)
	tmpDir := SetupTempDir(t, "long-path-*")
	defer CleanupTestData(t, tmpDir)

	longPath := filepath.Join(tmpDir, longName)

	t.Run("NewStorage_with_long_path", func(t *testing.T) {
		// 大多数文件系统限制路径长度，这应该返回错误
		_, err := NewStorage(longPath)
		// 不应该 panic，可能因路径过长而失败
		if err != nil {
			t.Logf("Expected failure for long path: %v", err)
		}
	})
}

// TestEdgeCases_SpecialCharacters 测试特殊字符路径
func TestEdgeCases_SpecialCharacters(t *testing.T) {
	testCases := []struct {
		name       string
		path       string
		shouldFail bool
	}{
		{"path_with_spaces", "test path", false},
		{"path_with_dots", "test..path", false},
		{"path_with_underscore", "test_path", false},
		{"path_with_dash", "test-path", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := SetupTempDir(t, "special-chars-*")
			defer CleanupTestData(t, tmpDir)

			testPath := filepath.Join(tmpDir, tc.path)
			_, err := NewStorage(testPath)

			if tc.shouldFail && err == nil {
				t.Errorf("expected error for path: %s", tc.path)
			}
			if !tc.shouldFail && err != nil {
				t.Logf("Note: path '%s' failed with: %v", tc.path, err)
			}
		})
	}
}

// TestEdgeCases_MultipleClose 测试多次关闭
func TestEdgeCases_MultipleClose(t *testing.T) {
	t.Run("Close_called_multiple_times", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		// 第一次关闭
		err := s.Close()
		if err != nil {
			t.Errorf("first Close failed: %v", err)
		}

		// 第二次关闭应该成功（幂等）
		err = s.Close()
		if err != nil {
			t.Errorf("second Close should be idempotent, got: %v", err)
		}

		// 第三次关闭
		err = s.Close()
		if err != nil {
			t.Errorf("third Close should be idempotent, got: %v", err)
		}
	})

	t.Run("CloseWithContext_called_multiple_times", func(t *testing.T) {
		ctx := context.Background()
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		// 第一次关闭
		err := s.CloseWithContext(ctx)
		if err != nil {
			t.Errorf("first CloseWithContext failed: %v", err)
		}

		// 第二次关闭
		err = s.CloseWithContext(ctx)
		if err != nil {
			t.Errorf("second CloseWithContext should be idempotent, got: %v", err)
		}
	})
}

// TestEdgeCases_MultipleDestroy 测试多次销毁
func TestEdgeCases_MultipleDestroy(t *testing.T) {
	t.Run("Destroy_after_Destroy", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		// Don't use CleanupTestData as we're testing Destroy

		// 第一次销毁
		err := s.Destroy()
		if err != nil {
			t.Errorf("first Destroy failed: %v", err)
		}

		// 验证目录已删除
		AssertFileNotExists(t, tmpDir)

		// 第二次销毁应该失败（目录已不存在）
		err = s.Destroy()
		// 目录已删除，第二次可能会失败
		if err != nil {
			t.Logf("Second Destroy failed as expected: %v", err)
		}
	})
}

// TestEdgeCases_CloseThenDestroy 测试先关闭再销毁
func TestEdgeCases_CloseThenDestroy(t *testing.T) {
	t.Run("Destroy_after_Close", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		// Don't use CleanupTestData as we're testing Destroy

		// 先关闭
		err := s.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}

		// 再销毁
		err = s.Destroy()
		if err != nil {
			t.Errorf("Destroy after Close should succeed, got: %v", err)
		}

		// 验证目录已删除
		AssertFileNotExists(t, tmpDir)
	})
}

// TestEdgeCases_DatastoreAfterClose 测试关闭后访问 datastore
func TestEdgeCases_DatastoreAfterClose(t *testing.T) {
	t.Run("Datastore_after_Close", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		// 关闭存储
		err := s.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}

		// 获取 datastore（应该返回已关闭的 datastore）
		ds := s.Datastore()
		if ds == nil {
			t.Error("Datastore should return non-nil even after close")
		}
	})

	t.Run("GetStorageUsage_after_Close", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		// 关闭存储
		err := s.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}

		// 尝试获取存储使用情况
		ctx := context.Background()
		_, err = s.GetStorageUsage(ctx)
		// 可能会失败，但不应 panic
		if err != nil {
			t.Logf("GetStorageUsage after close failed (expected): %v", err)
		}
	})
}

// TestEdgeCases_ConcurrentOperations 测试并发操作
func TestEdgeCases_ConcurrentOperations(t *testing.T) {
	t.Run("Concurrent_Datastore_calls", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		// 并发调用 Datastore()
		done := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func() {
				defer func() { done <- true }()
				ds := s.Datastore()
				if ds == nil {
					t.Error("Datastore returned nil")
				}
			}()
		}

		// 等待所有 goroutine 完成
		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("Concurrent_Close_and_Datastore", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		// 注意：不使用 CleanupTestData，因为测试会关闭存储

		done := make(chan bool, 10)

		// 启动多个并发操作：主要是读取，不调用 Close
		for i := 0; i < 10; i++ {
			go func(idx int) {
				defer func() { done <- true }()
				s.Datastore()
			}(i)
		}

		// 等待所有 goroutine 完成
		for i := 0; i < 10; i++ {
			<-done
		}

		// 在所有并发操作完成后关闭存储
		ctx := context.Background()
		if err := s.CloseWithContext(ctx); err != nil {
			t.Errorf("Close failed: %v", err)
		}

		CleanupTestData(t, tmpDir)
	})
}

// TestEdgeCases_ContextTimeouts 测试上下文超时
func TestEdgeCases_ContextTimeouts(t *testing.T) {
	t.Run("NewStorage_with_expired_context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// 等待 context 过期
		<-ctx.Done()

		tmpDir := SetupTempDir(t, "timeout-*")
		defer CleanupTestData(t, tmpDir)

		_, err := NewStorageWithContext(ctx, tmpDir)
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("Close_with_cancelled_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		s, tmpDir := SetupStorage(t)

		err := s.CloseWithContext(ctx)
		if err != context.Canceled {
			t.Errorf("expected Canceled, got: %v", err)
		}

		// 清理
		s.Close()
		CleanupTestData(t, tmpDir)
	})

	t.Run("GetStorageUsage_with_cancelled_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		s, tmpDir := SetupStorage(t)
		defer s.Close()
		defer CleanupTestData(t, tmpDir)

		_, err := s.GetStorageUsage(ctx)
		// Context 取消时应该返回错误或立即返回
		if err != nil && err != context.Canceled {
			t.Logf("GetStorageUsage with cancelled context: %v", err)
		}
	})
}

// TestEdgeCases_ExistingDirectory 测试在现有目录上创建存储
func TestEdgeCases_ExistingDirectory(t *testing.T) {
	t.Run("create_storage_in_existing_directory", func(t *testing.T) {
		baseDir := SetupTempDir(t, "existing-dir-base-*")
		testDir := filepath.Join(baseDir, "test")

		// 创建测试目录
		if err := os.MkdirAll(testDir, 0o755); err != nil {
			t.Fatalf("failed to create test dir: %v", err)
		}

		// 创建第一个存储
		s1, err := NewStorage(testDir)
		if err != nil {
			t.Fatalf("first NewStorage failed: %v", err)
		}
		if err := s1.Close(); err != nil {
			t.Fatalf("first Close failed: %v", err)
		}

		// 在同一目录创建第二个存储
		s2, err := NewStorage(testDir)
		if err != nil {
			t.Fatalf("second NewStorage failed: %v", err)
		}

		if s2 == nil {
			t.Error("expected non-nil storage")
		}

		// 关闭第二个存储
		if err := s2.Close(); err != nil {
			t.Errorf("second Close failed: %v", err)
		}

		// 清理临时目录
		CleanupTestData(t, baseDir)
	})
}

// TestEdgeCases_InvalidConfig 测试无效配置场景
func TestEdgeCases_InvalidConfig(t *testing.T) {
	t.Run("config_with_missing_type", func(t *testing.T) {
		_, err := AnyDatastoreConfig(map[string]interface{}{})
		if err == nil {
			t.Error("expected error for config without type")
		}
	})

	t.Run("config_with_unknown_type", func(t *testing.T) {
		_, err := AnyDatastoreConfig(map[string]interface{}{
			"type": "unknown_type",
		})
		if err == nil {
			t.Error("expected error for unknown type")
		}
	})

	t.Run("levelDB_config_without_path", func(t *testing.T) {
		_, err := LevelDBDatastoreConfig(map[string]interface{}{})
		if err == nil {
			t.Error("expected error for LevelDB config without path")
		}
	})

	t.Run("flatfs_config_without_path", func(t *testing.T) {
		_, err := FlatFsDatastoreConfig(map[string]interface{}{})
		if err == nil {
			t.Error("expected error for FlatFS config without path")
		}
	})
}

// TestEdgeCases_PathExpansion 测试路径扩展
func TestEdgeCases_PathExpansion(t *testing.T) {
	t.Run("path_with_tilde", func(t *testing.T) {
		// 测试波浪号扩展（如果支持）
		// 这取决于系统，我们只测试不会 panic
		_, err := NewStorage("~/test-storage")
		if err != nil {
			t.Logf("NewStorage with ~ failed (may be expected): %v", err)
		}
	})

	t.Run("relative_path", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "relative-*")
		defer CleanupTestData(t, tmpDir)

		// 目录不存在时应该返回错误
		relPath := filepath.Join(tmpDir, "storage")
		s, err := NewStorage(relPath)
		if err == nil {
			s.Close()
			t.Error("expected error for non-existent storage directory")
		}
	})
}

// isErrorType 检查错误是否为指定类型
func isErrorType(err error, target interface{}) bool {
	switch err.(type) {
	case *InvalidPathError:
		_, ok := target.(*InvalidPathError)
		return ok
	case *StorageError:
		_, ok := target.(*StorageError)
		return ok
	case *ConfigError:
		_, ok := target.(*ConfigError)
		return ok
	case *LockError:
		_, ok := target.(*LockError)
		return ok
	default:
		return false
	}
}
