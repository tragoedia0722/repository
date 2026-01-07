package repository

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrent_RepositoryOperations 测试并发操作
func TestConcurrent_RepositoryOperations(t *testing.T) {
	t.Run("concurrent_put_and_get", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-concurrent-putget")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()
		const numGoroutines = 100
		const opsPerGoroutine = 50

		var wg sync.WaitGroup
		var successCount atomic.Int64
		var errorCount atomic.Int64

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < opsPerGoroutine; j++ {
					data := []byte(fmt.Sprintf("goroutine-%d-operation-%d", id, j))

					cid, err := repo.PutBlock(ctx, data)
					if err != nil {
						errorCount.Add(1)
						t.Logf("goroutine %d: PutBlock failed: %v", id, err)
						continue
					}

					retrieved, err := repo.GetRawData(ctx, cid.String())
					if err != nil {
						errorCount.Add(1)
						t.Logf("goroutine %d: GetRawData failed: %v", id, err)
						continue
					}

					if string(retrieved) != string(data) {
						errorCount.Add(1)
						t.Logf("goroutine %d: data mismatch", id)
						continue
					}

					successCount.Add(1)
				}
			}(i)
		}

		wg.Wait()

		totalOps := int64(numGoroutines * opsPerGoroutine)
		success := successCount.Load()
		errors := errorCount.Load()

		t.Logf("Concurrent operations: %d total, %d success, %d errors", totalOps, success, errors)

		if errors > 0 {
			t.Errorf("Expected all operations to succeed, got %d errors", errors)
		}
	})

	t.Run("concurrent_has_all_blocks", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-concurrent-hasall")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()

		// Pre-populate with blocks
		const numBlocks = 100
		data := make([][]byte, numBlocks)
		for i := range data {
			data[i] = []byte(fmt.Sprintf("block-%d", i))
		}

		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}

		cidStrings := make([]string, len(cids))
		for i, c := range cids {
			cidStrings[i] = c.String()
		}

		const numGoroutines = 50
		const checksPerGoroutine = 20

		var wg sync.WaitGroup
		var successCount atomic.Int64

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < checksPerGoroutine; j++ {
					hasAll, err := repo.HasAllBlocks(ctx, cidStrings)
					if err != nil {
						t.Logf("goroutine %d: HasAllBlocks failed: %v", id, err)
						continue
					}
					if !hasAll {
						t.Logf("goroutine %d: expected all blocks to exist", id)
						continue
					}
					successCount.Add(1)
				}
			}(i)
		}

		wg.Wait()

		totalChecks := int64(numGoroutines * checksPerGoroutine)
		success := successCount.Load()

		t.Logf("Concurrent HasAllBlocks: %d total, %d success", totalChecks, success)

		if success != totalChecks {
			t.Errorf("Expected all checks to succeed, got %d/%d", success, totalChecks)
		}
	})

	t.Run("concurrent_delete", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-concurrent-delete")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()

		// Create blocks to delete
		const numBlocks = 100
		data := make([][]byte, numBlocks)
		cids := make([]string, numBlocks)

		for i := 0; i < numBlocks; i++ {
			data[i] = []byte(fmt.Sprintf("delete-block-%d", i))
			cid, err := repo.PutBlock(ctx, data[i])
			if err != nil {
				t.Fatalf("PutBlock %d failed: %v", i, err)
			}
			cids[i] = cid.String()
		}

		// Delete concurrently
		const numGoroutines = 20
		var wg sync.WaitGroup

		for i := 0; i < numBlocks; i += numBlocks / numGoroutines {
			wg.Add(1)
			go func(start int) {
				defer wg.Done()

				end := start + numBlocks/numGoroutines
				if end > numBlocks {
					end = numBlocks
				}

				for j := start; j < end; j++ {
					err := repo.DelBlock(ctx, cids[j])
					if err != nil {
						t.Logf("DelBlock %d failed: %v", j, err)
					}
				}
			}(i)
		}

		wg.Wait()

		// Verify all blocks were deleted
		for i, cid := range cids {
			has, _ := repo.HasBlock(ctx, cid)
			if has {
				t.Errorf("Block %d should have been deleted", i)
			}
		}
	})
}

// TestConcurrent_RaceConditions 测试竞态条件
func TestConcurrent_RaceConditions(t *testing.T) {
	t.Run("concurrent_close_and_operations", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-concurrent-close")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		ctx := context.Background()

		// Create some blocks first
		const numBlocks = 10
		cids := make([]string, numBlocks)
		for i := 0; i < numBlocks; i++ {
			data := []byte(fmt.Sprintf("block-%d", i))
			cid, err := repo.PutBlock(ctx, data)
			if err != nil {
				t.Fatalf("PutBlock %d failed: %v", i, err)
			}
			cids[i] = cid.String()
		}

		// Start operations in goroutines
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Try various operations
				for j := 0; j < 10; j++ {
					switch j % 4 {
					case 0:
						_, _ = repo.GetRawData(ctx, cids[id%len(cids)])
					case 1:
						_, _ = repo.HasBlock(ctx, cids[id%len(cids)])
					case 2:
						_, _ = repo.Usage(ctx)
					case 3:
						_ = repo.DataStore()
					}
					time.Sleep(time.Millisecond)
				}
			}(i)
		}

		// Close while operations are running
		time.Sleep(5 * time.Millisecond)
		repo.Close()

		wg.Wait()
		// If we get here without panic, the test passes
	})

	t.Run("concurrent_put_and_delete_same_block", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-concurrent-putdel")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()
		data := []byte("test data")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		const numGoroutines = 20
		const opsPerGoroutine = 50
		var wg sync.WaitGroup

		// Half will delete, half will put
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < opsPerGoroutine; j++ {
					if id%2 == 0 {
						// Try to delete
						_ = repo.DelBlock(ctx, cid.String())
					} else {
						// Try to get
						_, _ = repo.GetRawData(ctx, cid.String())
					}
				}
			}(i)
		}

		wg.Wait()
		// If we get here without panic or deadlock, the test passes
	})
}

// TestConcurrent_HasAllBlocksConcurrency 测试并发限制
func TestConcurrent_HasAllBlocksConcurrency(t *testing.T) {
	t.Run("respects_concurrency_limit", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-concurrent-limit")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()

		// Create many blocks
		const numBlocks = 500
		data := make([][]byte, numBlocks)
		for i := range data {
			data[i] = []byte(fmt.Sprintf("block-%d", i))
		}

		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}

		cidStrings := make([]string, len(cids))
		for i, c := range cids {
			cidStrings[i] = c.String()
		}

		// Check that we can handle more than the concurrency limit
		hasAll, err := repo.HasAllBlocks(ctx, cidStrings)
		if err != nil {
			t.Fatalf("HasAllBlocks failed: %v", err)
		}
		if !hasAll {
			t.Error("Expected all blocks to exist")
		}
	})

	t.Run("many_concurrent_hasall_calls", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-concurrent-manyhasall")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()

		// Create some blocks
		const numBlocks = 50
		data := make([][]byte, numBlocks)
		for i := range data {
			data[i] = []byte(fmt.Sprintf("block-%d", i))
		}

		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}

		cidStrings := make([]string, len(cids))
		for i, c := range cids {
			cidStrings[i] = c.String()
		}

		// Run many HasAllBlocks calls concurrently
		const numGoroutines = 20
		const checksPerGoroutine = 10

		var wg sync.WaitGroup
		var successCount atomic.Int64

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < checksPerGoroutine; j++ {
					hasAll, err := repo.HasAllBlocks(ctx, cidStrings)
					if err != nil {
						t.Logf("goroutine %d check %d failed: %v", id, j, err)
						continue
					}
					if !hasAll {
						t.Logf("goroutine %d check %d: expected all blocks", id, j)
						continue
					}
					successCount.Add(1)
				}
			}(i)
		}

		wg.Wait()

		total := int64(numGoroutines * checksPerGoroutine)
		success := successCount.Load()

		t.Logf("Many concurrent HasAllBlocks: %d/%d succeeded", success, total)

		if success < total*9/10 { // Allow 10% failure rate
			t.Errorf("Too many failures: %d/%d", total-success, total)
		}
	})
}

// TestConcurrent_PanicRecovery 测试 panic 恢复机制
func TestConcurrent_PanicRecovery(t *testing.T) {
	t.Run("hasallblocks_handles_invalid_cids", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-concurrent-panic")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()

		// Create some valid blocks first
		const numValid = 10
		data := make([][]byte, numValid)
		for i := range data {
			data[i] = []byte(fmt.Sprintf("valid-%d", i))
		}

		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}

		cidStrings := make([]string, len(cids))
		for i, c := range cids {
			cidStrings[i] = c.String()
		}

		// Add some invalid CIDs
		cidStrings = append(cidStrings, "invalid-cid-1", "invalid-cid-2")

		// Should handle invalid CIDs gracefully (return error, not panic)
		_, err = repo.HasAllBlocks(ctx, cidStrings)
		if err == nil {
			t.Error("expected error for invalid CIDs")
		}
		t.Logf("Got expected error: %v", err)
	})
}

// TestConcurrent_StressTest 压力测试
func TestConcurrent_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	t.Run("high_frequency_operations", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-stress-highfreq")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()
		const numGoroutines = 100
		const opsPerGoroutine = 100

		var wg sync.WaitGroup
		startTime := time.Now()
		var opsCompleted atomic.Int64

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < opsPerGoroutine; j++ {
					data := []byte(fmt.Sprintf("stress-%d-%d", id, j))

					cid, err := repo.PutBlock(ctx, data)
					if err != nil {
						t.Logf("PutBlock failed: %v", err)
						continue
					}

					_, err = repo.GetRawData(ctx, cid.String())
					if err != nil {
						t.Logf("GetRawData failed: %v", err)
						continue
					}

					opsCompleted.Add(1)
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(startTime)

		completed := opsCompleted.Load()
		totalOps := int64(numGoroutines * opsPerGoroutine)

		t.Logf("Stress test completed: %d/%d ops in %v (%.0f ops/sec)",
			completed, totalOps, duration,
			float64(completed)/duration.Seconds())

		if completed < totalOps*9/10 {
			t.Errorf("Too many operations failed: %d/%d", totalOps-completed, totalOps)
		}
	})

	t.Run("mixed_operations", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-stress-mixed")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()

		// Pre-populate with some blocks
		const numBlocks = 100
		data := make([][]byte, numBlocks)
		cids := make([]string, numBlocks)

		for i := 0; i < numBlocks; i++ {
			data[i] = []byte(fmt.Sprintf("mixed-block-%d", i))
			cid, err := repo.PutBlock(ctx, data[i])
			if err != nil {
				t.Fatalf("PutBlock %d failed: %v", i, err)
			}
			cids[i] = cid.String()
		}

		const numGoroutines = 50
		const opsPerGoroutine = 50

		var wg sync.WaitGroup
		var opsCompleted atomic.Int64

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < opsPerGoroutine; j++ {
					op := j % 5
					switch op {
					case 0: // Put
						newData := []byte(fmt.Sprintf("new-%d-%d", id, j))
						_, err := repo.PutBlock(ctx, newData)
						if err != nil {
							t.Logf("PutBlock failed: %v", err)
						} else {
							opsCompleted.Add(1)
						}
					case 1: // Get
						idx := (id + j) % numBlocks
						_, err := repo.GetRawData(ctx, cids[idx])
						if err != nil {
							t.Logf("GetRawData failed: %v", err)
						} else {
							opsCompleted.Add(1)
						}
					case 2: // Has
						idx := (id + j) % numBlocks
						_, err := repo.HasBlock(ctx, cids[idx])
						if err != nil {
							t.Logf("HasBlock failed: %v", err)
						} else {
							opsCompleted.Add(1)
						}
					case 3: // HasAll
						subsetSize := 10
						startIdx := (id + j) % (len(cids) - subsetSize + 1)
						subset := cids[startIdx : startIdx+subsetSize]
						_, err := repo.HasAllBlocks(ctx, subset)
						if err != nil {
							t.Logf("HasAllBlocks failed: %v", err)
						} else {
							opsCompleted.Add(1)
						}
					case 4: // Usage
						_, err := repo.Usage(ctx)
						if err != nil {
							t.Logf("Usage failed: %v", err)
						} else {
							opsCompleted.Add(1)
						}
					}
				}
			}(i)
		}

		wg.Wait()

		completed := opsCompleted.Load()
		totalOps := int64(numGoroutines * opsPerGoroutine)

		t.Logf("Mixed operations: %d/%d completed", completed, totalOps)

		if completed < totalOps*9/10 {
			t.Errorf("Too many operations failed: %d/%d", totalOps-completed, totalOps)
		}
	})
}

// TestConcurrent_ContextCancellation 测试并发 context 取消
func TestConcurrent_ContextCancellation(t *testing.T) {
	t.Run("cancel_during_concurrent_operations", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-concurrent-cancel")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		// Create a context that will be cancelled
		ctx, cancel := context.WithCancel(context.Background())

		const numGoroutines = 20
		var wg sync.WaitGroup

		// Start goroutines with long-running operations
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Create large CID list to trigger longer processing
				cids := make([]string, 1000)
				for j := range cids {
					cids[j] = fmt.Sprintf("QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsX%04d", j)
				}

				_, err := repo.HasAllBlocks(ctx, cids)
				t.Logf("goroutine %d: HasAllBlocks returned: %v", id, err)
			}(i)
		}

		// Cancel after a short delay
		time.Sleep(10 * time.Millisecond)
		cancel()

		wg.Wait()
		// If we get here without deadlock, the test passes
	})

	t.Run("timeout_during_concurrent_operations", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-concurrent-timeout")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		const numGoroutines = 10
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < 100; j++ {
					data := []byte(fmt.Sprintf("timeout-test-%d-%d", id, j))

					select {
					case <-ctx.Done():
						return
					default:
						_, err := repo.PutBlock(ctx, data)
						if err != nil {
							return
						}
					}
				}
			}(i)
		}

		wg.Wait()
		// If we get here without deadlock, the test passes
	})
}
