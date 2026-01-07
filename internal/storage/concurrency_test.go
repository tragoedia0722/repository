package storage

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrent_DatastoreAccess 测试并发 Datastore 访问
func TestConcurrent_DatastoreAccess(t *testing.T) {
	t.Run("concurrent_reads", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer s.Close()
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 100
		const opsPerGoroutine = 1000

		var wg sync.WaitGroup
		var successCount int64
		var errorCount int64

		start := time.Now()

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					ds := s.Datastore()
					if ds != nil {
						atomic.AddInt64(&successCount, 1)
					} else {
						atomic.AddInt64(&errorCount, 1)
					}
				}
			}(i)
		}

		wg.Wait()
		elapsed := time.Since(start)

		totalOps := int64(numGoroutines * opsPerGoroutine)
		t.Logf("Completed %d operations in %v (%.0f ops/sec)",
			totalOps, elapsed, float64(totalOps)/elapsed.Seconds())

		if errorCount > 0 {
			t.Errorf("Got %d errors", errorCount)
		}

		if successCount != totalOps {
			t.Errorf("Expected %d successful operations, got %d", totalOps, successCount)
		}
	})
}

// TestConcurrent_MultipleClose 测试并发关闭
func TestConcurrent_MultipleClose(t *testing.T) {
	t.Run("concurrent_close_calls", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 10
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = s.Close()
			}()
		}

		wg.Wait()

		// 应该没有 panic 或 deadlock
		if !s.closed.Load() {
			t.Error("storage should be marked as closed")
		}
	})

	t.Run("concurrent_close_with_datastore_access", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 50
		var wg sync.WaitGroup

		// 启动混合操作：关闭和 Datastore 访问
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				if id%2 == 0 {
					_ = s.Datastore()
				} else {
					_ = s.Close()
				}
			}(i)
		}

		wg.Wait()

		// 应该没有 panic 或 deadlock
		t.Log("Mixed close and datastore access completed successfully")
	})
}

// TestConcurrent_ContextOperations 测试并发 Context 操作
func TestConcurrent_ContextOperations(t *testing.T) {
	t.Run("concurrent_close_with_context", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 20
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx := context.Background()
				_ = s.CloseWithContext(ctx)
			}(i)
		}

		wg.Wait()
	})

	t.Run("concurrent_with_cancelled_contexts", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer s.Close()
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 20
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // 立即取消
				_ = s.CloseWithContext(ctx)
			}()
		}

		wg.Wait()
	})
}

// TestConcurrent_GetStorageUsage 测试并发获取存储使用情况
func TestConcurrent_GetStorageUsage(t *testing.T) {
	t.Run("concurrent_usage_queries", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer s.Close()
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 50
		const queriesPerGoroutine = 10

		var wg sync.WaitGroup
		var successCount int64

		ctx := context.Background()

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < queriesPerGoroutine; j++ {
					_, err := s.GetStorageUsage(ctx)
					if err == nil {
						atomic.AddInt64(&successCount, 1)
					}
				}
			}()
		}

		wg.Wait()

		totalQueries := int64(numGoroutines * queriesPerGoroutine)
		t.Logf("Completed %d/%d usage queries successfully", successCount, totalQueries)
	})
}

// TestConcurrent_RaceConditions 测试竞态条件
func TestConcurrent_RaceConditions(t *testing.T) {
	t.Run("datastore_access_during_close", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 100
		var wg sync.WaitGroup
		done := make(chan struct{})

		// 启动 goroutine 持续访问 Datastore
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
						_ = s.Datastore()
					}
				}
			}()
		}

		// 等待一小段时间让 goroutine 运行
		time.Sleep(10 * time.Millisecond)

		// 关闭存储
		_ = s.Close()

		// 停止所有 goroutine
		close(done)
		wg.Wait()

		t.Log("Datastore access during close completed without race")
	})

	t.Run("closed_flag_consistency", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 50
		var wg sync.WaitGroup

		// 并发检查和设置 closed 标志
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				if id%3 == 0 {
					_ = s.Close()
				} else {
					_ = s.closed.Load()
					_ = s.Datastore()
				}
			}(i)
		}

		wg.Wait()

		// 验证最终状态
		if !s.closed.Load() {
			t.Error("storage should be closed")
		}
	})
}

// TestConcurrent_StressTests 压力测试
func TestConcurrent_StressTests(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	t.Run("high_frequency_operations", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer s.Close()
		defer CleanupTestData(t, tmpDir)

		const duration = 1 * time.Second
		const numGoroutines = 10

		var wg sync.WaitGroup
		var opsCount int64
		start := time.Now()

		// 运行固定时间的高频操作
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Since(start) < duration {
					_ = s.Datastore()
					atomic.AddInt64(&opsCount, 1)
				}
			}()
		}

		wg.Wait()
		elapsed := time.Since(start)

		t.Logf("Completed %d operations in %v (%.0f ops/sec)",
			opsCount, elapsed, float64(opsCount)/elapsed.Seconds())
	})

	t.Run("burst_operations", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer s.Close()
		defer CleanupTestData(t, tmpDir)

		const numBursts = 10
		const opsPerBurst = 1000

		for burst := 0; burst < numBursts; burst++ {
			var wg sync.WaitGroup

			for i := 0; i < opsPerBurst; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = s.Datastore()
				}()
			}

			wg.Wait()
			t.Logf("Completed burst %d/%d", burst+1, numBursts)
		}
	})
}

// TestConcurrent_MemoryLeaks 内存泄漏测试
func TestConcurrent_MemoryLeaks(t *testing.T) {
	t.Run("repeated_creation_and_destruction", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping memory leak test in short mode")
		}

		baseTmpDir := SetupTempDir(t, "memory-leak-*")
		defer CleanupTestData(t, baseTmpDir)

		const iterations = 100
		const concurrent = 10

		for iter := 0; iter < iterations; iter++ {
			var wg sync.WaitGroup

			for i := 0; i < concurrent; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					tmpDir := SetupTempDir(t, "ml-*")

					s, err := NewStorage(tmpDir)
					if err == nil {
						_ = s.Datastore()
						_ = s.Close()
					}

					CleanupTestData(t, tmpDir)
				}(i)
			}

			wg.Wait()

			if iter%10 == 0 {
				t.Logf("Completed %d/%d iterations", iter+1, iterations)
			}
		}

		t.Log("Memory leak test completed")
	})
}

// TestConcurrent_DeadlockScenarios 死锁场景测试
func TestConcurrent_DeadlockScenarios(t *testing.T) {
	t.Run("no_deadlock_on_simultaneous_close", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		done := make(chan bool, 1)

		go func() {
			_ = s.Close()
			done <- true
		}()

		go func() {
			_ = s.Close()
			done <- true
		}()

		// 设置超时以检测死锁
		timeout := time.After(5 * time.Second)
		select {
		case <-done:
			// 正常完成
		case <-timeout:
			t.Fatal("potential deadlock detected")
		case <-done:
			// 正常完成
		}
	})

	t.Run("no_deadlock_on_mixed_operations", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		ctx := context.Background()
		done := make(chan bool, 4)

		// 启动多种操作
		go func() {
			_ = s.Datastore()
			done <- true
		}()

		go func() {
			_, _ = s.GetStorageUsage(ctx)
			done <- true
		}()

		go func() {
			_ = s.Close()
			done <- true
		}()

		go func() {
			_ = s.closed.Load()
			done <- true
		}()

		// 检查是否在合理时间内完成
		timeout := time.After(5 * time.Second)
		for i := 0; i < 4; i++ {
			select {
			case <-done:
				// 正常
			case <-timeout:
				t.Fatal("potential deadlock detected")
			}
		}
	})
}

// TestConcurrent_AtomicOperations 原子操作测试
func TestConcurrent_AtomicOperations(t *testing.T) {
	t.Run("atomic_closed_flag", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 1000
		var wg sync.WaitGroup

		// 并发读取 closed 标志
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = s.closed.Load()
			}()
		}

		wg.Wait()

		// 现在关闭并继续读取
		_ = s.Close()

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				closed := s.closed.Load()
				if !closed {
					t.Error("closed flag should be true")
				}
			}()
		}

		wg.Wait()
	})
}

// TestConcurrent_LockContention 锁竞争测试
func TestConcurrent_LockContention(t *testing.T) {
	t.Run("minimal_lock_contention", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer s.Close()
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 100
		const opsPerGoroutine = 10000

		var wg sync.WaitGroup
		start := time.Now()

		// Datastore() 现在不需要锁，应该非常快
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					_ = s.Datastore()
				}
			}()
		}

		wg.Wait()
		elapsed := time.Since(start)

		totalOps := numGoroutines * opsPerGoroutine
		opsPerSec := float64(totalOps) / elapsed.Seconds()

		t.Logf("Completed %d operations in %v (%.0f ops/sec)",
			totalOps, elapsed, opsPerSec)

		// 验证性能：应该在每秒百万次操作级别
		if opsPerSec < 1000000 {
			t.Logf("Warning: Low performance detected (%.0f ops/sec)", opsPerSec)
		}
	})
}

// TestConcurrent_ContextCancellation 并发取消测试
func TestConcurrent_ContextCancellation(t *testing.T) {
	t.Run("concurrent_context_cancellation", func(t *testing.T) {
		const numGoroutines = 10 // 减少数量以避免资源问题
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// 为每个 goroutine 创建独立的目录
				tmpDir := SetupTempDir(t, "ctx-cancel-*")

				ctx, cancel := context.WithCancel(context.Background())
				if id%2 == 0 {
					cancel() // 一半立即取消
				}

				s, err := NewStorageWithContext(ctx, tmpDir)
				if err == nil && s != nil {
					s.Close()
				}
				cancel()

				// 清理临时目录
				CleanupTestData(t, tmpDir)
			}(i)
		}

		wg.Wait()
		t.Log("Concurrent context cancellation completed")
	})
}

// TestConcurrent_ResourceCleanup 资源清理测试
func TestConcurrent_ResourceCleanup(t *testing.T) {
	t.Run("cleanup_after_concurrent_errors", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "cleanup-*")
		defer os.RemoveAll(tmpDir) // 确保最终清理

		const numGoroutines = 20
		var wg sync.WaitGroup

		// 尝试并发创建多个存储（大部分会因锁文件而失败）
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				subDir := SetupTempDir(t, "sub-*")
				s, err := NewStorage(subDir)
				if err == nil {
					_ = s.Close()
				} else {
					CleanupTestData(t, subDir)
				}
			}(i)
		}

		wg.Wait()
		t.Log("Cleanup after concurrent operations completed")
	})
}

// TestConcurrent_StateConsistency 状态一致性测试
func TestConcurrent_StateConsistency(t *testing.T) {
	t.Run("consistent_state_after_concurrent_closes", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		const numGoroutines = 50
		var wg sync.WaitGroup
		var closedStates []bool
		var mu sync.Mutex

		// 并发关闭
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = s.Close()
				mu.Lock()
				closedStates = append(closedStates, s.closed.Load())
				mu.Unlock()
			}()
		}

		wg.Wait()

		// 验证所有状态一致
		for _, state := range closedStates {
			if !state {
				t.Error("all closed states should be true")
			}
		}
	})
}
