package validator

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
)

// TestPerformance_SingleValidation tests single validation performance
func TestPerformance_SingleValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create test blocks
	const numBlocks = 100
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i), byte(i >> 8)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	t.Run("single_validation", func(t *testing.T) {
		start := time.Now()

		result, err := v.Validate(context.Background(), blockList[0], blockList)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		duration := time.Since(start)

		t.Logf("Single validation of %d blocks: %v", numBlocks, duration)
		t.Logf("Result: %d missing, %d invalid",
			len(result.MissingBlocks), len(result.InvalidBlocks))
	})
}

// TestPerformance_MultipleValidations tests multiple validation performance
func TestPerformance_MultipleValidations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create test blocks
	const numBlocks = 50
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	t.Run("multiple_validations", func(t *testing.T) {
		const numValidations = 100

		start := time.Now()
		for i := 0; i < numValidations; i++ {
			_, err := v.Validate(context.Background(), blockList[0], blockList)
			if err != nil {
				t.Fatalf("validation %d failed: %v", i, err)
			}
		}
		duration := time.Since(start)

		avgDuration := duration / time.Duration(numValidations)

		t.Logf("Multiple validations: %d in %v", numValidations, duration)
		t.Logf("Average: %v per validation", avgDuration)
		t.Logf("Throughput: %.0f validations/sec", float64(numValidations)/duration.Seconds())
	})
}

// TestPerformance_LargeBlockList tests large block list performance
func TestPerformance_LargeBlockList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create test blocks
	const numBlocks = 1000
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	t.Run("large_block_list", func(t *testing.T) {
		start := time.Now()

		result, err := v.Validate(context.Background(), blockList[0], blockList)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		duration := time.Since(start)

		t.Logf("Validation of %d blocks: %v", numBlocks, duration)
		t.Logf("Result: %d missing, %d invalid",
			len(result.MissingBlocks), len(result.InvalidBlocks))

		// Performance assertion
		if duration > 5*time.Second {
			t.Errorf("Validation too slow: %v (expected < 5s)", duration)
		}
	})
}

// TestPerformance_ConcurrentValidations tests concurrent validation performance
func TestPerformance_ConcurrentValidations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create test blocks
	const numBlocks = 50
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	t.Run("concurrent_validations", func(t *testing.T) {
		const numGoroutines = 10
		const validationsPerGoroutine = 50

		var wg sync.WaitGroup
		startTime := time.Now()
		var opsCompleted atomic.Int64

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < validationsPerGoroutine; j++ {
					_, err := v.Validate(context.Background(), blockList[0], blockList)
					if err != nil {
						t.Logf("goroutine %d validation %d failed: %v", id, j, err)
						continue
					}

					opsCompleted.Add(1)
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(startTime)

		completed := opsCompleted.Load()
		totalOps := int64(numGoroutines * validationsPerGoroutine)

		t.Logf("Concurrent validations: %d/%d ops in %v", completed, totalOps, duration)
		t.Logf("Throughput: %.0f validations/sec", float64(completed)/duration.Seconds())

		if completed < totalOps*90/100 {
			t.Errorf("Too many operations failed: %d/%d", totalOps-completed, totalOps)
		}
	})
}

// TestPerformance_MemoryUsage tests memory usage during validation
func TestPerformance_MemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Force GC before starting
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Create and validate many blocks
	const numBlocks = 1000
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	// Validate
	result, err := v.Validate(context.Background(), blockList[0], blockList)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Force GC after validation
	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	allocated := m2.TotalAlloc - m1.TotalAlloc

	t.Logf("Memory allocated for %d blocks: %d bytes", numBlocks, allocated)
	t.Logf("Per block: %.2f bytes", float64(allocated)/float64(numBlocks))
	t.Logf("Result: %d missing, %d invalid",
		len(result.MissingBlocks), len(result.InvalidBlocks))
}

// TestPerformance_VaryingBlockSizes tests performance with different block sizes
func TestPerformance_VaryingBlockSizes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	sizes := []struct {
		name      string
		size      int
		numBlocks int
	}{
		{"small (1B)", 1, 1000},
		{"medium (100B)", 100, 500},
		{"large (1KB)", 1024, 200},
		{"very large (10KB)", 10240, 50},
	}

	for _, sizeInfo := range sizes {
		t.Run(sizeInfo.name, func(t *testing.T) {
			// Create blocks
			blockList := make([]string, sizeInfo.numBlocks)
			for i := 0; i < sizeInfo.numBlocks; i++ {
				data := make([]byte, sizeInfo.size)
				data[0] = byte(i)
				block := blocks.NewBlock(data)
				bs.Put(context.Background(), block)
				blockList[i] = block.Cid().String()
			}

			// Measure validation time
			start := time.Now()
			result, err := v.Validate(context.Background(), blockList[0], blockList)
			duration := time.Since(start)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			totalSize := int64(sizeInfo.numBlocks * sizeInfo.size)
			mbPerSec := (float64(totalSize) / (1024 * 1024)) / duration.Seconds()

			t.Logf("Blocks: %d × %d bytes", sizeInfo.numBlocks, sizeInfo.size)
			t.Logf("Total size: %.2f MB", float64(totalSize)/(1024*1024))
			t.Logf("Duration: %v", duration)
			t.Logf("Throughput: %.2f MB/sec", mbPerSec)
			t.Logf("Result: %d missing, %d invalid",
				len(result.MissingBlocks), len(result.InvalidBlocks))
		})
	}
}

// TestPerformance_InvalidCIDHandling tests invalid CID handling performance
func TestPerformance_InvalidCIDHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create some valid blocks
	const numValid = 10
	validBlocks := make([]string, numValid)
	for i := 0; i < numValid; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		validBlocks[i] = block.Cid().String()
	}

	// Create list with many invalid CIDs
	const totalBlocks = 1000
	blockList := make([]string, totalBlocks)
	copy(blockList, validBlocks)

	for i := numValid; i < totalBlocks; i++ {
		blockList[i] = fmt.Sprintf("invalid-cid-%d", i)
	}

	t.Run("many_invalid_cids", func(t *testing.T) {
		start := time.Now()

		result, err := v.Validate(context.Background(), validBlocks[0], blockList)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		duration := time.Since(start)

		t.Logf("Validation with %d invalid CIDs: %v", totalBlocks-numValid, duration)
		t.Logf("Invalid blocks detected: %d", len(result.InvalidBlocks))

		if len(result.InvalidBlocks) != totalBlocks-numValid {
			t.Errorf("Expected %d invalid blocks, got %d", totalBlocks-numValid, len(result.InvalidBlocks))
		}
	})
}

// TestPerformance_ResultOperations tests Result operations performance
func TestPerformance_ResultOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	t.Run("result_operations", func(t *testing.T) {
		const numOps = 10000

		result := &Result{
			MissingBlocks: make([]string, 0, numOps),
			InvalidBlocks: make([]string, 0, numOps),
			ErrorDetails:  make([]string, 0, numOps),
		}

		start := time.Now()

		for i := 0; i < numOps; i++ {
			result.addMissingBlock(fmt.Sprintf("missing-%d", i))
			result.addInvalidBlock(fmt.Sprintf("invalid-%d", i))
			result.addError("error %d", i)

			if i%100 == 0 {
				result.setCanRestore(i%2 == 0)
			}
		}

		result.finalize()
		duration := time.Since(start)

		t.Logf("Result operations: %d in %v", numOps, duration)
		t.Logf("Throughput: %.0f ops/sec", float64(numOps)/duration.Seconds())
		t.Logf("Final state: %d missing, %d invalid, %d errors",
			len(result.MissingBlocks), len(result.InvalidBlocks), len(result.ErrorDetails))
	})
}

// Benchmark_Validate_10Blocks provides baseline benchmark for validation
func Benchmark_Validate_10Blocks(b *testing.B) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create test blocks
	const numBlocks = 10
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := v.Validate(context.Background(), blockList[0], blockList)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// Benchmark_Validate_100Blocks benchmarks validation of 100 blocks
func Benchmark_Validate_100Blocks(b *testing.B) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create test blocks
	const numBlocks = 100
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := v.Validate(context.Background(), blockList[0], blockList)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// Benchmark_ResultOperations benchmarks Result operations
func Benchmark_ResultOperations(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := &Result{
			MissingBlocks: make([]string, 0, 10),
			InvalidBlocks: make([]string, 0, 10),
			ErrorDetails:  make([]string, 0, 10),
		}

		for j := 0; j < 10; j++ {
			result.addMissingBlock(fmt.Sprintf("missing-%d", j))
			result.addInvalidBlock(fmt.Sprintf("invalid-%d", j))
			result.addError("error %d", j)
			result.setCanRestore(j%2 == 0)
		}

		result.finalize()
	}
}

// Benchmark_ConcurrentValidations benchmarks concurrent validations
func Benchmark_ConcurrentValidations(b *testing.B) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create test blocks
	const numBlocks = 20
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := v.Validate(context.Background(), blockList[0], blockList)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

// TestPerformance_StressTest sustained load test
func TestPerformance_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create test blocks
	const numBlocks = 50
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	const duration = 5 * time.Second
	const numGoroutines = 10

	var wg sync.WaitGroup
	startTime := time.Now()
	var opsCompleted atomic.Int64

	endTime := startTime.Add(duration)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for time.Now().Before(endTime) {
				_, err := v.Validate(context.Background(), blockList[0], blockList[:10])
				if err != nil {
					t.Logf("goroutine %d: validation failed: %v", id, err)
					continue
				}

				opsCompleted.Add(1)
			}
		}(i)
	}

	wg.Wait()
	actualDuration := time.Since(startTime)

	completed := opsCompleted.Load()
	opsPerSec := float64(completed) / actualDuration.Seconds()

	t.Logf("Stress test (%v):", actualDuration)
	t.Logf("Total validations: %d", completed)
	t.Logf("Throughput: %.0f validations/sec", opsPerSec)

	if completed < 10 {
		t.Errorf("Too few completions: %d", completed)
	}
}

// TestPerformance_ContextCancellationOverhead tests context cancellation overhead
func TestPerformance_ContextCancellationOverhead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create test blocks
	const numBlocks = 100
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	t.Run("with_context_check", func(t *testing.T) {
		ctx := context.Background()

		start := time.Now()
		for i := 0; i < 100; i++ {
			_, err := v.Validate(ctx, blockList[0], blockList)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}
		duration := time.Since(start)

		t.Logf("100 validations with context: %v", duration)
		t.Logf("Average: %v per validation", duration/100)
	})
}

// TestPerformance_MemoryReusing tests memory usage with repeated validations
func TestPerformance_MemoryReusing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create test blocks
	const numBlocks = 100
	blockList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	// First GC
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Perform validations
	const numValidations = 500
	for i := 0; i < numValidations; i++ {
		_, err := v.Validate(context.Background(), blockList[0], blockList)
		if err != nil {
			t.Fatalf("validation %d failed: %v", i, err)
		}
	}

	// Second GC
	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	allocated := m2.TotalAlloc - m1.TotalAlloc
	avgAlloc := int64(allocated) / int64(numValidations)

	t.Logf("Memory for %d validations: %d bytes", numValidations, allocated)
	t.Logf("Average per validation: %d bytes", avgAlloc)
	t.Logf("Allocations per validation: %.2f", float64(allocated)/float64(numValidations*len(blockList)))
}
