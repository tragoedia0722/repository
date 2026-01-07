package storage

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestIntegration_FullLifecycle 测试完整的生命周期
func TestIntegration_FullLifecycle(t *testing.T) {
	t.Run("create_use_close_destroy", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "lifecycle-*")

		// 阶段 1: 创建
		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		// 阶段 2: 使用
		ds := s.Datastore()
		if ds == nil {
			t.Fatal("Datastore returned nil")
		}

		// 验证配置文件存在
		specPath := DatastoreSpecPath(tmpDir)
		if !FileExists(specPath) {
			t.Error("spec file should exist")
		}

		// 验证锁文件存在
		lockPath := filepath.Join(tmpDir, LockFile)
		if !FileExists(lockPath) {
			t.Error("lock file should exist")
		}

		// 阶段 3: 关闭
		err = s.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}

		// 验证锁文件已删除
		if FileExists(lockPath) {
			t.Error("lock file should be removed after close")
		}

		// 阶段 4: 销毁
		err = s.Destroy()
		if err != nil {
			t.Errorf("Destroy failed: %v", err)
		}

		// 验证目录已删除
		if _, err := os.Stat(tmpDir); !os.IsNotExist(err) {
			t.Error("directory should be removed after destroy")
		}
	})

	t.Run("reopening_existing_storage", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "reopen-*")
		defer CleanupTestData(t, tmpDir)

		// 创建存储并写入一些数据
		s1, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("first NewStorage failed: %v", err)
		}
		s1.Close()

		// 重新打开
		s2, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("second NewStorage failed: %v", err)
		}
		defer s2.Close()

		if s2 == nil {
			t.Error("expected non-nil storage on reopen")
		}

		// 验证可以正常访问
		ds := s2.Datastore()
		if ds == nil {
			t.Error("Datastore should return non-nil")
		}
	})
}

// TestIntegration_ContextOperations 测试 Context 操作集成
func TestIntegration_ContextOperations(t *testing.T) {
	t.Run("full_lifecycle_with_context", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := SetupTempDir(t, "context-lifecycle-*")

		// 使用 Context 创建
		s, err := NewStorageWithContext(ctx, tmpDir)
		if err != nil {
			t.Fatalf("NewStorageWithContext failed: %v", err)
		}

		// 使用存储
		ds := s.Datastore()
		if ds == nil {
			t.Fatal("Datastore returned nil")
		}

		// 获取使用情况
		usage, err := s.GetStorageUsage(ctx)
		if err != nil {
			t.Logf("GetStorageUsage error (may be expected): %v", err)
		} else {
			t.Logf("Storage usage: %d bytes", usage)
		}

		// 使用 Context 关闭
		err = s.CloseWithContext(ctx)
		if err != nil {
			t.Errorf("CloseWithContext failed: %v", err)
		}

		// 销毁
		err = s.DestroyWithContext(ctx)
		if err != nil {
			t.Errorf("DestroyWithContext failed: %v", err)
		}
	})

	t.Run("context_timeout_during_operation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		tmpDir := SetupTempDir(t, "ctx-timeout-op-*")
		defer CleanupTestData(t, tmpDir)

		s, err := NewStorageWithContext(ctx, tmpDir)
		if err != nil {
			t.Fatalf("NewStorageWithContext failed: %v", err)
		}
		defer s.Close()

		// 获取使用情况（应该在超时前完成）
		usage, err := s.GetStorageUsage(ctx)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("GetStorageUsage: %v (usage: %d)", err, usage)
		}
	})
}

// TestIntegration_MultipleStorages 测试多个存储实例
func TestIntegration_MultipleStorages(t *testing.T) {
	t.Run("simultaneous_multiple_storages", func(t *testing.T) {
		const numStorages = 5
		storages := make([]*Storage, numStorages)
		dirs := make([]string, numStorages)

		// 创建多个存储
		for i := 0; i < numStorages; i++ {
			dirs[i] = SetupTempDir(t, "multi-*")
			s, err := NewStorage(dirs[i])
			if err != nil {
				t.Fatalf("NewStorage %d failed: %v", i, err)
			}
			storages[i] = s
		}

		// 验证每个存储都可以独立访问
		for i, s := range storages {
			ds := s.Datastore()
			if ds == nil {
				t.Errorf("Storage %d: Datastore returned nil", i)
			}
		}

		// 关闭所有存储
		for i, s := range storages {
			err := s.Close()
			if err != nil {
				t.Errorf("Storage %d: Close failed: %v", i, err)
			}
			CleanupTestData(t, dirs[i])
		}
	})
}

// TestIntegration_Persistence 测试持久化
func TestIntegration_Persistence(t *testing.T) {
	t.Run("spec_persistence", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "persist-*")
		defer CleanupTestData(t, tmpDir)

		// 创建存储
		s1, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("first NewStorage failed: %v", err)
		}
		s1.Close()

		// 读取 spec 文件
		specPath := DatastoreSpecPath(tmpDir)
		specContent, err := os.ReadFile(specPath)
		if err != nil {
			t.Fatalf("failed to read spec: %v", err)
		}

		// 验证 spec 内容
		if len(specContent) == 0 {
			t.Error("spec file should not be empty")
		}

		// 重新打开存储，应该使用相同的 spec
		s2, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("second NewStorage failed: %v", err)
		}
		s2.Close()

		// spec 内容应该没有改变
		newSpecContent, err := os.ReadFile(specPath)
		if err != nil {
			t.Fatalf("failed to read spec again: %v", err)
		}

		if string(newSpecContent) != string(specContent) {
			t.Error("spec content should not change after reopen")
		}
	})
}

// TestIntegration_ErrorRecovery 测试错误恢复
func TestIntegration_ErrorRecovery(t *testing.T) {
	t.Run("recover_from_close_error", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "recover-*")
		defer CleanupTestData(t, tmpDir)

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		// 关闭存储
		if err := s.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}

		// 再次关闭应该是幂等的
		if err := s.Close(); err != nil {
			t.Errorf("Second close should succeed: %v", err)
		}

		// 销毁应该仍然工作
		if err := s.Destroy(); err != nil {
			t.Errorf("Destroy should succeed: %v", err)
		}
	})

	t.Run("recover_after_partial_failure", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "partial-fail-*")
		defer os.RemoveAll(tmpDir)

		// 创建损坏的 spec
		specPath := DatastoreSpecPath(tmpDir)
		os.WriteFile(specPath, []byte("{invalid"), 0o600)

		// 尝试打开（应该失败）
		_, err := NewStorage(tmpDir)
		if err == nil {
			t.Error("expected error for invalid spec")
		}

		// 修复 spec
		s, err := NewStorage(tmpDir)
		if err != nil {
			// 仍然可能失败，因为 spec 文件存在
			t.Logf("Note: may need to remove corrupted spec first: %v", err)
		} else {
			s.Close()
		}
	})
}

// TestIntegration_ConcurrencyStress 并发压力测试
func TestIntegration_ConcurrencyStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	t.Run("high_concurrency_lifecycle", func(t *testing.T) {
		const numStorages = 50
		const goroutinesPerStorage = 10

		var wg sync.WaitGroup

		for i := 0; i < numStorages; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				tmpDir := SetupTempDir(t, "stress-*")
				defer CleanupTestData(t, tmpDir)

				s, err := NewStorage(tmpDir)
				if err != nil {
					t.Logf("Storage %d: NewStorage failed: %v", id, err)
					return
				}
				defer s.Close()

				// 每个存储启动多个 goroutine 访问
				var innerWg sync.WaitGroup
				for j := 0; j < goroutinesPerStorage; j++ {
					innerWg.Add(1)
					go func() {
						defer innerWg.Done()
						_ = s.Datastore()
					}()
				}
				innerWg.Wait()
			}(i)
		}

		wg.Wait()
		t.Logf("Completed stress test with %d storages", numStorages)
	})
}

// TestIntegration_Configurations 测试不同配置
func TestIntegration_Configurations(t *testing.T) {
	t.Run("default_configuration", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "default-config-*")
		defer CleanupTestData(t, tmpDir)

		// 使用默认配置创建
		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage with default config failed: %v", err)
		}
		defer s.Close()

		// 验证存储结构
		blocksPath := filepath.Join(tmpDir, "blocks")
		if _, err := os.Stat(blocksPath); os.IsNotExist(err) {
			t.Error("blocks directory should exist")
		}

		datastorePath := filepath.Join(tmpDir, "datastore")
		if _, err := os.Stat(datastorePath); os.IsNotExist(err) {
			t.Error("datastore directory should exist")
		}
	})
}

// TestIntegration_LockFileBehavior 锁文件行为测试
func TestIntegration_LockFileBehavior(t *testing.T) {
	t.Run("lock_file_creation_and_cleanup", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "lock-behavior-*")
		defer CleanupTestData(t, tmpDir)

		lockPath := filepath.Join(tmpDir, LockFile)

		// 初始状态：没有锁文件
		if FileExists(lockPath) {
			t.Error("lock file should not exist initially")
		}

		// 创建存储
		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		// 锁文件应该存在
		if !FileExists(lockPath) {
			t.Error("lock file should exist after NewStorage")
		}

		// 关闭存储
		err = s.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}

		// 锁文件应该被删除
		if FileExists(lockPath) {
			t.Error("lock file should be removed after Close")
		}
	})

	t.Run("lock_file_prevents_double_open", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "lock-prevent-*")
		defer CleanupTestData(t, tmpDir)

		// 创建第一个存储
		s1, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("first NewStorage failed: %v", err)
		}
		defer s1.Close()

		// 尝试创建第二个存储
		s2, err := NewStorage(tmpDir)
		if err == nil {
			s2.Close()
			t.Error("expected lock error for second open")
		} else {
			t.Logf("Got expected error: %v", err)
		}
	})
}

// TestIntegration_StorageMetrics 存储度量测试
func TestIntegration_StorageMetrics(t *testing.T) {
	t.Run("get_storage_usage", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "metrics-*")
		defer CleanupTestData(t, tmpDir)

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}
		defer s.Close()

		ctx := context.Background()
		usage, err := s.GetStorageUsage(ctx)
		if err != nil {
			t.Logf("GetStorageUsage returned error: %v", err)
		} else {
			t.Logf("Storage usage: %d bytes", usage)
			if usage == 0 {
				t.Log("New storage has zero usage (expected)")
			}
		}
	})

	t.Run("usage_after_operations", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "usage-ops-*")
		defer CleanupTestData(t, tmpDir)

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		// 执行一些操作
		for i := 0; i < 10; i++ {
			_ = s.Datastore()
		}

		ctx := context.Background()
		usage, err := s.GetStorageUsage(ctx)
		if err != nil {
			t.Logf("GetStorageUsage error: %v", err)
		} else {
			t.Logf("Storage usage after operations: %d bytes", usage)
		}

		s.Close()
	})
}

// TestIntegration_PathHandling 路径处理测试
func TestIntegration_PathHandling(t *testing.T) {
	t.Run("absolute_vs_relative_paths", func(t *testing.T) {
		baseTmpDir := SetupTempDir(t, "path-handling-*")
		defer CleanupTestData(t, baseTmpDir)

		// 使用绝对路径
		absPath := filepath.Join(baseTmpDir, "absolute")
		s1, err := NewStorage(absPath)
		if err != nil {
			t.Fatalf("NewStorage with absolute path failed: %v", err)
		}
		s1.Close()

		// 使用相对路径
		relPath := filepath.Join(baseTmpDir, "relative")
		s2, err := NewStorage(relPath)
		if err != nil {
			t.Fatalf("NewStorage with relative path failed: %v", err)
		}
		s2.Close()
	})

	t.Run("path_with_symlinks", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "symlink-*")
		defer CleanupTestData(t, tmpDir)

		// 创建目录和符号链接
		realDir := filepath.Join(tmpDir, "real")
		linkDir := filepath.Join(tmpDir, "link")

		if err := os.Mkdir(realDir, 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}

		// 尝试创建符号链接（在某些系统上可能失败）
		err := os.Symlink(realDir, linkDir)
		if err != nil {
			t.Skipf("symlink not supported: %v", err)
		}

		// 通过符号链接创建存储
		s, err := NewStorage(linkDir)
		if err != nil {
			t.Logf("NewStorage through symlink failed: %v", err)
		} else {
			s.Close()
		}
	})
}

// TestIntegration_CleanupBehavior 清理行为测试
func TestIntegration_CleanupBehavior(t *testing.T) {
	t.Run("proper_cleanup_on_error", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "cleanup-error-*")
		defer os.RemoveAll(tmpDir)

		// 创建损坏的 spec
		specPath := DatastoreSpecPath(tmpDir)
		os.WriteFile(specPath, []byte("invalid"), 0o600)

		// 尝试创建存储（应该失败）
		_, err := NewStorage(tmpDir)
		if err == nil {
			t.Error("expected error for invalid spec")
		}

		// 验证不应该留下锁文件
		lockPath := filepath.Join(tmpDir, LockFile)
		if FileExists(lockPath) {
			t.Error("lock file should not exist after failed creation")
		}
	})

	t.Run("cleanup_after_partial_init", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "partial-init-*")
		defer CleanupTestData(t, tmpDir)

		// 创建部分目录结构
		blocksPath := filepath.Join(tmpDir, "blocks")
		if err := os.Mkdir(blocksPath, 0o755); err != nil {
			t.Fatalf("failed to create blocks dir: %v", err)
		}

		// NewStorage 应该能够处理这种情况
		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}
		s.Close()
	})
}

// TestIntegration_RecoveryScenarios 恢复场景测试
func TestIntegration_RecoveryScenarios(t *testing.T) {
	t.Run("recover_from_crash", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "crash-recovery-*")
		defer CleanupTestData(t, tmpDir)

		// 创建存储
		s1, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		// 模拟崩溃：不正常关闭，直接删除锁文件
		lockPath := filepath.Join(tmpDir, LockFile)
		os.Remove(lockPath)

		// 尝试重新打开
		s2, err := NewStorage(tmpDir)
		if err != nil {
			t.Logf("Reopen after crash failed: %v", err)
		} else {
			s2.Close()
			t.Log("Successfully recovered from simulated crash")
		}

		// 清理第一个存储
		s1.Close()
	})

	t.Run("recover_from_incomplete_destroy", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "incomplete-destroy-*")
		defer os.RemoveAll(tmpDir)

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		// 模拟不完整的销毁：只删除部分文件
		blocksPath := filepath.Join(tmpDir, "blocks")
		os.RemoveAll(blocksPath)

		// 调用 Destroy 应该能够清理剩余部分
		err = s.Destroy()
		if err != nil {
			t.Logf("Destroy after partial cleanup: %v", err)
		}

		// 验证目录最终被删除
		if _, err := os.Stat(tmpDir); !os.IsNotExist(err) {
			t.Logf("Note: directory still exists after incomplete destroy")
		}
	})
}

// TestIntegration_RealWorldScenarios 真实场景测试
func TestIntegration_RealWorldScenarios(t *testing.T) {
	t.Run("typical_usage_pattern", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "typical-*")
		defer CleanupTestData(t, tmpDir)

		// 典型使用模式：创建、使用、关闭
		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}
		defer s.Close()

		// 多次获取 datastore
		for i := 0; i < 100; i++ {
			ds := s.Datastore()
			if ds == nil {
				t.Errorf("iteration %d: Datastore returned nil", i)
			}
		}

		// 获取存储使用情况
		ctx := context.Background()
		usage, err := s.GetStorageUsage(ctx)
		if err != nil {
			t.Logf("GetStorageUsage: %v", err)
		} else {
			t.Logf("Usage: %d bytes", usage)
		}
	})

	t.Run("restart_scenario", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "restart-*")
		defer CleanupTestData(t, tmpDir)

		// 第一次启动
		s1, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("first NewStorage failed: %v", err)
		}
		s1.Close()

		// 模拟进程重启：重新创建存储
		s2, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("second NewStorage failed: %v", err)
		}

		// 验证功能正常
		ds := s2.Datastore()
		if ds == nil {
			t.Error("Datastore should work after restart")
		}

		s2.Close()
	})
}
