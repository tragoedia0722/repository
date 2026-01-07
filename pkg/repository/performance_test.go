package repository

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPerformance_PutBlockPerformance 测试 PutBlock 性能
func TestPerformance_PutBlockPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	tmpDir := filepath.Join(os.TempDir(), "test-perf-putblock")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("small_blocks", func(t *testing.T) {
		const blockSize = 1024 // 1KB
		const numBlocks = 1000

		start := time.Now()
		for i := 0; i < numBlocks; i++ {
			data := make([]byte, blockSize)
			data[0] = byte(i)
			data[blockSize-1] = byte(i >> 8)

			_, err := repo.PutBlock(ctx, data)
			if err != nil {
				t.Fatalf("PutBlock %d failed: %v", i, err)
			}
		}
		duration := time.Since(start)

		opsPerSec := float64(numBlocks) / duration.Seconds()
		mbPerSec := (float64(numBlocks*blockSize) / (1024 * 1024)) / duration.Seconds()

		t.Logf("PutBlock %d small blocks: %v (%.0f ops/sec, %.2f MB/sec)",
			numBlocks, duration, opsPerSec, mbPerSec)
	})

	t.Run("medium_blocks", func(t *testing.T) {
		const blockSize = 100 * 1024 // 100KB
		const numBlocks = 100

		start := time.Now()
		for i := 0; i < numBlocks; i++ {
			data := make([]byte, blockSize)
			data[0] = byte(i)

			_, err := repo.PutBlock(ctx, data)
			if err != nil {
				t.Fatalf("PutBlock %d failed: %v", i, err)
			}
		}
		duration := time.Since(start)

		opsPerSec := float64(numBlocks) / duration.Seconds()
		mbPerSec := (float64(numBlocks*blockSize) / (1024 * 1024)) / duration.Seconds()

		t.Logf("PutBlock %d medium blocks: %v (%.0f ops/sec, %.2f MB/sec)",
			numBlocks, duration, opsPerSec, mbPerSec)
	})

	t.Run("large_blocks", func(t *testing.T) {
		const blockSize = 1024 * 1024 // 1MB
		const numBlocks = 10

		start := time.Now()
		for i := 0; i < numBlocks; i++ {
			data := make([]byte, blockSize)
			data[0] = byte(i)

			_, err := repo.PutBlock(ctx, data)
			if err != nil {
				t.Fatalf("PutBlock %d failed: %v", i, err)
			}
		}
		duration := time.Since(start)

		mbPerSec := (float64(numBlocks*blockSize) / (1024 * 1024)) / duration.Seconds()

		t.Logf("PutBlock %d large blocks: %v (%.2f MB/sec)", numBlocks, duration, mbPerSec)
	})
}

// TestPerformance_PutManyBlocksPerformance 测试批量操作性能
func TestPerformance_PutManyBlocksPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	tmpDir := filepath.Join(os.TempDir(), "test-perf-putmany")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("batch_vs_individual", func(t *testing.T) {
		const blockSize = 1024
		const numBlocks = 500

		// Test individual puts
		data := make([][]byte, numBlocks)
		for i := range data {
			data[i] = make([]byte, blockSize)
			data[i][0] = byte(i)
		}

		start := time.Now()
		for _, d := range data {
			_, err := repo.PutBlock(ctx, d)
			if err != nil {
				t.Fatalf("PutBlock failed: %v", err)
			}
		}
		individualDuration := time.Since(start)

		// Create new repo for batch test
		tmpDir2 := filepath.Join(os.TempDir(), "test-perf-putmany2")
		defer cleanupRepo(t, tmpDir2)

		repo2, err := NewRepository(tmpDir2)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo2.Close()

		start = time.Now()
		_, err = repo2.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}
		batchDuration := time.Since(start)

		speedup := float64(individualDuration) / float64(batchDuration)

		t.Logf("Individual puts: %v", individualDuration)
		t.Logf("Batch puts: %v", batchDuration)
		t.Logf("Speedup: %.2fx", speedup)

		if speedup < 1.5 {
			t.Logf("Warning: Batch is only %.2fx faster than individual (expected > 1.5x)", speedup)
		}
	})
}

// TestPerformance_GetRawDataPerformance 测试读取性能
func TestPerformance_GetRawDataPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	tmpDir := filepath.Join(os.TempDir(), "test-perf-getraw")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// Pre-populate with blocks
	const blockSize = 100 * 1024 // 100KB
	const numBlocks = 500

	data := make([][]byte, numBlocks)
	cids := make([]string, numBlocks)

	for i := 0; i < numBlocks; i++ {
		data[i] = make([]byte, blockSize)
		data[i][0] = byte(i)

		cid, err := repo.PutBlock(ctx, data[i])
		if err != nil {
			t.Fatalf("PutBlock %d failed: %v", i, err)
		}
		cids[i] = cid.String()
	}

	t.Run("sequential_reads", func(t *testing.T) {
		start := time.Now()
		for _, cid := range cids {
			_, err := repo.GetRawData(ctx, cid)
			if err != nil {
				t.Fatalf("GetRawData failed: %v", err)
			}
		}
		duration := time.Since(start)

		opsPerSec := float64(numBlocks) / duration.Seconds()
		mbPerSec := (float64(numBlocks*blockSize) / (1024 * 1024)) / duration.Seconds()

		t.Logf("Sequential reads: %v (%.0f ops/sec, %.2f MB/sec)",
			duration, opsPerSec, mbPerSec)
	})

	t.Run("read_same_block_repeatedly", func(t *testing.T) {
		const reads = 1000
		cid := cids[0]

		start := time.Now()
		for i := 0; i < reads; i++ {
			_, err := repo.GetRawData(ctx, cid)
			if err != nil {
				t.Fatalf("GetRawData failed: %v", err)
			}
		}
		duration := time.Since(start)

		opsPerSec := float64(reads) / duration.Seconds()

		t.Logf("Repeated reads: %v (%.0f ops/sec)", duration, opsPerSec)
	})
}

// TestPerformance_HasAllBlocksPerformance 测试并发检查性能
func TestPerformance_HasAllBlocksPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	tmpDir := filepath.Join(os.TempDir(), "test-perf-hasall")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// Pre-populate with blocks
	const numBlocks = 1000

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

	t.Run("check_many_blocks", func(t *testing.T) {
		start := time.Now()
		hasAll, err := repo.HasAllBlocks(ctx, cidStrings)
		if err != nil {
			t.Fatalf("HasAllBlocks failed: %v", err)
		}
		if !hasAll {
			t.Fatal("Expected all blocks to exist")
		}
		duration := time.Since(start)

		t.Logf("HasAllBlocks %d blocks: %v", numBlocks, duration)

		// Should be fast with concurrency
		if duration > 5*time.Second {
			t.Errorf("HasAllBlocks took too long: %v (expected < 5s)", duration)
		}
	})

	t.Run("check_large_list_performance", func(t *testing.T) {
		// Test with different sizes
		sizes := []int{10, 50, 100, 500, 1000}

		for _, size := range sizes {
			subset := cidStrings[:size]

			start := time.Now()
			hasAll, err := repo.HasAllBlocks(ctx, subset)
			if err != nil {
				t.Fatalf("HasAllBlocks failed for size %d: %v", size, err)
			}
			if !hasAll {
				t.Fatalf("Expected all %d blocks to exist", size)
			}
			duration := time.Since(start)

			opsPerSec := float64(size) / duration.Seconds()
			t.Logf("HasAllBlocks %d blocks: %v (%.0f checks/sec)", size, duration, opsPerSec)
		}
	})
}

// TestStress_MemoryUsage 测试内存使用
func TestStress_MemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	t.Run("large_number_of_blocks", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-stress-memory")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()

		// Force GC before starting
		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)

		// Put many blocks
		const numBlocks = 1000
		const blockSize = 1024 // 1KB

		for i := 0; i < numBlocks; i++ {
			data := make([]byte, blockSize)
			data[0] = byte(i)

			_, err := repo.PutBlock(ctx, data)
			if err != nil {
				t.Fatalf("PutBlock %d failed: %v", i, err)
			}
		}

		runtime.GC()
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)

		allocated := m2.TotalAlloc - m1.TotalAlloc
		t.Logf("Allocated %d bytes for %d blocks (%.2f bytes/block)",
			allocated, numBlocks, float64(allocated)/float64(numBlocks))
	})
}

// TestStress_MaxBlockSize 测试最大块大小
func TestStress_MaxBlockSize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	t.Run("store_max_size_blocks", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-stress-maxsize")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()

		const numBlocks = 5 // Don't use too many due to size
		start := time.Now()

		for i := 0; i < numBlocks; i++ {
			data := make([]byte, maxBlockSize)
			data[0] = byte(i)
			data[maxBlockSize-1] = byte(i >> 8)

			cid, err := repo.PutBlock(ctx, data)
			if err != nil {
				t.Fatalf("PutBlock %d failed: %v", i, err)
			}

			// Verify
			retrieved, err := repo.GetRawData(ctx, cid.String())
			if err != nil {
				t.Fatalf("GetRawData failed: %v", err)
			}

			if len(retrieved) != maxBlockSize {
				t.Errorf("Size mismatch: got %d, want %d", len(retrieved), maxBlockSize)
			}
		}

		duration := time.Since(start)
		totalMb := (float64(numBlocks*maxBlockSize) / (1024 * 1024))
		mbPerSec := totalMb / duration.Seconds()

		t.Logf("Stored %d max-size blocks (%.2f MB) in %v (%.2f MB/sec)",
			numBlocks, totalMb, duration, mbPerSec)
	})
}

// TestStress_ConcurrentMixedOperations 混合操作压力测试
func TestStress_ConcurrentMixedOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	t.Run("sustained_mixed_load", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-stress-mixed")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()
		const duration = 5 * time.Second

		// Pre-populate some blocks
		const numBlocks = 100
		data := make([][]byte, numBlocks)
		cids := make([]string, numBlocks)

		for i := 0; i < numBlocks; i++ {
			data[i] = []byte(fmt.Sprintf("stress-block-%d", i))
			cid, err := repo.PutBlock(ctx, data[i])
			if err != nil {
				t.Fatalf("PutBlock %d failed: %v", i, err)
			}
			cids[i] = cid.String()
		}

		start := time.Now()
		var putCount, getCount, hasCount, hasAllCount atomic.Int64

		// Run for specified duration
		endTime := start.Add(duration)
		var wg sync.WaitGroup

		// Start multiple goroutines with different operations
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				ops := 0
				for time.Now().Before(endTime) {
					switch id % 4 {
					case 0: // Put
						newData := []byte(fmt.Sprintf("new-%d-%d", id, ops))
						_, err := repo.PutBlock(ctx, newData)
						if err == nil {
							putCount.Add(1)
						}
					case 1: // Get
						idx := ops % numBlocks
						_, err := repo.GetRawData(ctx, cids[idx])
						if err == nil {
							getCount.Add(1)
						}
					case 2: // Has
						idx := ops % numBlocks
						_, err := repo.HasBlock(ctx, cids[idx])
						if err == nil {
							hasCount.Add(1)
						}
					case 3: // HasAll
						subsetSize := 10 + (ops % 40)
						if subsetSize <= len(cids) {
							subset := cids[:subsetSize]
							_, err := repo.HasAllBlocks(ctx, subset)
							if err == nil {
								hasAllCount.Add(1)
							}
						}
					}
					ops++
				}
			}(i)
		}

		wg.Wait()
		actualDuration := time.Since(start)

		totalOps := putCount.Load() + getCount.Load() + hasCount.Load() + hasAllCount.Load()
		opsPerSec := float64(totalOps) / actualDuration.Seconds()

		t.Logf("Sustained load test (%v):", actualDuration)
		t.Logf("  Put:      %d", putCount.Load())
		t.Logf("  Get:      %d", getCount.Load())
		t.Logf("  Has:      %d", hasCount.Load())
		t.Logf("  HasAll:   %d", hasAllCount.Load())
		t.Logf("  Total:    %d ops", totalOps)
		t.Logf("  Throughput: %.0f ops/sec", opsPerSec)

		if totalOps < 100 {
			t.Errorf("Too few operations completed: %d", totalOps)
		}
	})
}

// TestPerformance_Retries 测试重试性能
func TestPerformance_Retries(t *testing.T) {
	t.Run("non_existent_block_retry_timing", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-perf-retries")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx := context.Background()
		nonExistentCID := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"

		start := time.Now()
		_, err = repo.GetRawData(ctx, nonExistentCID)
		duration := time.Since(start)

		if err == nil {
			t.Fatal("Expected error for non-existent block")
		}

		t.Logf("Non-existent block (with retries): %v", duration)

		// Should take roughly 350ms (50 + 100 + 200) with exponential backoff
		// But allow some tolerance
		maxExpected := 500 * time.Millisecond
		if duration > maxExpected {
			t.Errorf("GetRawData took too long: %v (expected < %v)", duration, maxExpected)
		}

		// Verify it's faster than the old implementation (1500ms)
		oldImplTime := 1500 * time.Millisecond
		if duration > oldImplTime {
			t.Errorf("GetRawData slower than old implementation: %v (old was %v)", duration, oldImplTime)
		}
	})
}
