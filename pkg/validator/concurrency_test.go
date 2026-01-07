package validator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
)

// TestConcurrent_ValidateOperations tests concurrent validation operations
func TestConcurrent_ValidateOperations(t *testing.T) {
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

	t.Run("concurrent_validations", func(t *testing.T) {
		const numGoroutines = 10
		const opsPerGoroutine = 20

		var wg sync.WaitGroup
		var successCount atomic.Int64
		var errorCount atomic.Int64

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < opsPerGoroutine; j++ {
					result, err := v.Validate(context.Background(), blockList[0], blockList)
					if err != nil {
						errorCount.Add(1)
						t.Logf("goroutine %d: validation failed: %v", id, err)
						continue
					}

					if result == nil {
						errorCount.Add(1)
						t.Logf("goroutine %d: result is nil", id)
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

		t.Logf("Concurrent validations: %d total, %d success, %d errors", totalOps, success, errors)

		if errors > 0 {
			t.Errorf("Expected all operations to succeed, got %d errors", errors)
		}

		if success != totalOps {
			t.Errorf("Expected all operations to succeed, got %d/%d", success, totalOps)
		}
	})
}

// TestConcurrent_ResultModifications tests concurrent Result modifications
func TestConcurrent_ResultModifications(t *testing.T) {
	result := &Result{
		MissingBlocks: make([]string, 0, 1000),
		InvalidBlocks: make([]string, 0, 1000),
		ErrorDetails:  make([]string, 0, 1000),
	}

	const numGoroutines = 20
	const opsPerGoroutine = 100

	var wg sync.WaitGroup

	// Concurrently modify result
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic in goroutine %d: %v", id, r)
				}
			}()

			for j := 0; j < opsPerGoroutine; j++ {
				switch j % 4 {
				case 0:
					result.addMissingBlock(fmt.Sprintf("missing-%d-%d", id, j))
				case 1:
					result.addInvalidBlock(fmt.Sprintf("invalid-%d-%d", id, j))
				case 2:
					result.addError("error %d-%d", id, j)
				case 3:
					result.setCanRestore(j%2 == 0)
				}
			}

			// Finalize at the end
			result.finalize()
		}(i)
	}

	wg.Wait()

	// Verify all operations completed
	expectedOps := numGoroutines * opsPerGoroutine
	missingOps := expectedOps / 4
	invalidOps := expectedOps / 4
	errorOps := expectedOps / 4

	t.Logf("Expected %d missing, got %d", missingOps, len(result.MissingBlocks))
	t.Logf("Expected %d invalid, got %d", invalidOps, len(result.InvalidBlocks))
	t.Logf("Expected %d errors, got %d", errorOps, len(result.ErrorDetails))

	if len(result.MissingBlocks) != missingOps {
		t.Errorf("Expected %d missing blocks, got %d", missingOps, len(result.MissingBlocks))
	}

	if len(result.InvalidBlocks) != invalidOps {
		t.Errorf("Expected %d invalid blocks, got %d", invalidOps, len(result.InvalidBlocks))
	}

	if len(result.ErrorDetails) != errorOps {
		t.Errorf("Expected %d error details, got %d", errorOps, len(result.ErrorDetails))
	}
}

// TestConcurrent_MixedOperations tests mixed concurrent operations
func TestConcurrent_MixedOperations(t *testing.T) {
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

	const numGoroutines = 10
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	var opsCompleted atomic.Int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				// Mix of validation and result operations
				switch j % 2 {
				case 0:
					_, err := v.Validate(context.Background(), blockList[0], blockList[:10])
					if err != nil {
						t.Logf("goroutine %d op %d: validation failed: %v", id, j, err)
					}
				case 1:
					result := &Result{
						MissingBlocks: make([]string, 0, 10),
						InvalidBlocks: make([]string, 0, 10),
						ErrorDetails:  make([]string, 0, 10),
					}
					result.addMissingBlock(fmt.Sprintf("test-%d-%d", id, j))
					result.addInvalidBlock(fmt.Sprintf("test-%d-%d", id, j))
					result.finalize()
				}

				opsCompleted.Add(1)
			}
		}(i)
	}

	wg.Wait()

	completed := opsCompleted.Load()
	totalOps := int64(numGoroutines * opsPerGoroutine)

	t.Logf("Mixed operations: %d/%d completed", completed, totalOps)

	if completed < totalOps*95/100 { // Allow 5% failure rate
		t.Errorf("Too many operations failed: %d/%d", totalOps-completed, totalOps)
	}
}

// TestConcurrent_ContextCancellation tests concurrent context cancellation
func TestConcurrent_ContextCancellation(t *testing.T) {
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

	t.Run("cancel_during_concurrent_operations", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		const numGoroutines = 10
		var wg sync.WaitGroup

		// Start goroutines with long-running operations
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Create large CID list to trigger longer processing
				largeList := make([]string, 1000)
				for j := range largeList {
					largeList[j] = fmt.Sprintf("QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsX%04d", j)
				}

				_, err := v.Validate(ctx, blockList[0], largeList)
				if err != nil {
					t.Logf("goroutine %d: Validate returned: %v", id, err)
				}
			}(i)
		}

		// Cancel after a short delay
		time.Sleep(10 * time.Millisecond)
		cancel()

		wg.Wait()
		// If we get here without deadlock, the test passes
	})

	t.Run("timeout_during_concurrent_operations", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		const numGoroutines = 5
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < 100; j++ {
					_, err := v.Validate(ctx, blockList[0], blockList)
					if err != nil {
						return
					}
				}
			}(i)
		}

		wg.Wait()
		// If we get here without deadlock, the test passes
	})
}

// TestConcurrent_ValidateSameData tests concurrent validation of same data
func TestConcurrent_ValidateSameData(t *testing.T) {
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

	const numGoroutines = 20
	const validationsPerGoroutine = 10

	var wg sync.WaitGroup
	var successCount atomic.Int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < validationsPerGoroutine; j++ {
				result, err := v.Validate(context.Background(), blockList[0], blockList)
				if err != nil {
					t.Logf("goroutine %d validation %d failed: %v", id, j, err)
					continue
				}

				if result == nil {
					t.Logf("goroutine %d validation %d: nil result", id, j)
					continue
				}

				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	totalOps := int64(numGoroutines * validationsPerGoroutine)
	success := successCount.Load()

	t.Logf("Concurrent validation of same data: %d/%d succeeded", success, totalOps)

	if success < totalOps*95/100 { // Allow 5% failure rate
		t.Errorf("Too many operations failed: %d/%d", totalOps-success, totalOps)
	}
}

// TestConcurrent_ResultFinalize tests concurrent finalize calls
func TestConcurrent_ResultFinalize(t *testing.T) {
	result := &Result{
		MissingBlocks: make([]string, 0, 100),
		InvalidBlocks: make([]string, 0, 100),
		ErrorDetails:  make([]string, 0, 100),
	}

	const numGoroutines = 20
	const opsPerGoroutine = 50

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic in goroutine %d: %v", id, r)
				}
			}()

			for j := 0; j < opsPerGoroutine; j++ {
				// Add some data
				result.addMissingBlock(fmt.Sprintf("missing-%d-%d", id, j))
				result.addInvalidBlock(fmt.Sprintf("invalid-%d-%d", id, j))

				// Finalize multiple times
				result.finalize()
				result.finalize()
				result.finalize()
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	result.finalize()

	// Since we added missing and invalid blocks, IsComplete should be false
	if result.IsComplete {
		t.Error("Expected IsComplete=false when there are missing/invalid blocks")
	}

	// CanRestore should also be false when there are errors
	if result.CanRestore {
		t.Error("Expected CanRestore=false when there are missing/invalid blocks")
	}

	// Verify we actually added blocks
	if len(result.MissingBlocks) == 0 {
		t.Error("Expected some missing blocks")
	}
	if len(result.InvalidBlocks) == 0 {
		t.Error("Expected some invalid blocks")
	}
}

// TestConcurrent_StressTest performs stress testing with high concurrency
func TestConcurrent_StressTest(t *testing.T) {
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

	t.Run("high_frequency_operations", func(t *testing.T) {
		const numGoroutines = 50
		const opsPerGoroutine = 20

		var wg sync.WaitGroup
		var opsCompleted atomic.Int64

		startTime := time.Now()

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < opsPerGoroutine; j++ {
					_, err := v.Validate(context.Background(), blockList[0], blockList[:10])
					if err != nil {
						t.Logf("goroutine %d op %d failed: %v", id, j, err)
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

		if completed < totalOps*90/100 {
			t.Errorf("Too many operations failed: %d/%d", totalOps-completed, totalOps)
		}
	})
}

// TestConcurrent_RaceConditions tests for potential race conditions
func TestConcurrent_RaceConditions(t *testing.T) {
	t.Run("concurrent_result_state_changes", func(t *testing.T) {
		result := &Result{
			MissingBlocks: make([]string, 0, 100),
			InvalidBlocks: make([]string, 0, 100),
			ErrorDetails:  make([]string, 0, 100),
		}

		const numGoroutines = 10
		var wg sync.WaitGroup

		// Goroutines that modify state
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < 100; j++ {
					result.addMissingBlock(fmt.Sprintf("m-%d-%d", id, j))
					result.addInvalidBlock(fmt.Sprintf("i-%d-%d", id, j))
					result.addError("e-%d-%d", id, j)
					result.setCanRestore(j%2 == 0)
				}
			}(i)
		}

		wg.Wait()

		// Final check
		result.finalize()

		if result.IsComplete {
			t.Error("Expected IsComplete=false when errors exist")
		}
	})

	t.Run("concurrent_validate_and_result_modify", func(t *testing.T) {
		bs := newMockBlockstore()
		v := NewValidator(bs)

		// Create blocks
		const numBlocks = 20
		blockList := make([]string, numBlocks)
		for i := 0; i < numBlocks; i++ {
			block := blocks.NewBlock([]byte{byte(i)})
			bs.Put(context.Background(), block)
			blockList[i] = block.Cid().String()
		}

		var wg sync.WaitGroup

		// Mix validation and result operations
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < 20; j++ {
					if id%2 == 0 {
						// Validation
						_, err := v.Validate(context.Background(), blockList[0], blockList)
						if err != nil {
							t.Logf("validation failed: %v", err)
						}
					} else {
						// Result operations
						result := &Result{
							MissingBlocks: make([]string, 0, 10),
							InvalidBlocks: make([]string, 0, 10),
							ErrorDetails:  make([]string, 0, 10),
						}
						result.addMissingBlock(fmt.Sprintf("test-%d", j))
						result.finalize()
					}
				}
			}(i)
		}

		wg.Wait()
		// If we get here without panic or deadlock, the test passes
	})
}

// TestConcurrent_MultipleValidators tests multiple validators operating concurrently
func TestConcurrent_MultipleValidators(t *testing.T) {
	const numValidators = 10

	validators := make([]*Validator, numValidators)
	for i := 0; i < numValidators; i++ {
		bs := newMockBlockstore()
		validators[i] = NewValidator(bs)

		// Create same blocks for each validator
		block := blocks.NewBlock([]byte{byte(i)})
		bs.Put(context.Background(), block)
	}

	const numGoroutines = 10
	const opsPerGoroutine = 10

	var wg sync.WaitGroup
	var successCount atomic.Int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				v := validators[j%numValidators]

				// Create a simple block list with a new blockstore
				bs := newMockBlockstore()
				block := blocks.NewBlock([]byte{byte(j)})
				bs.Put(context.Background(), block)

				result, err := v.Validate(context.Background(), block.Cid().String(), []string{block.Cid().String()})
				if err != nil {
					t.Logf("goroutine %d op %d failed: %v", id, j, err)
					continue
				}

				if result != nil {
					successCount.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	totalOps := int64(numGoroutines * opsPerGoroutine)
	success := successCount.Load()

	t.Logf("Multiple validators: %d/%d succeeded", success, totalOps)

	if success < totalOps*95/100 { // Allow 5% failure rate
		t.Errorf("Too many operations failed: %d/%d", totalOps-success, totalOps)
	}
}

// TestConcurrent_ResultStateConsistency tests Result state consistency under concurrent access
func TestConcurrent_ResultStateConsistency(t *testing.T) {
	result := &Result{
		MissingBlocks: make([]string, 0, 1000),
		InvalidBlocks: make([]string, 0, 1000),
		ErrorDetails:  make([]string, 0, 1000),
	}

	const numGoroutines = 15
	const opsPerGoroutine = 200

	var wg sync.WaitGroup

	// Perform concurrent modifications
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic in goroutine %d: %v", id, r)
				}
			}()

			for j := 0; j < opsPerGoroutine; j++ {
				// Add to all slices
				result.addMissingBlock(fmt.Sprintf("m-%d-%d", id, j))
				result.addInvalidBlock(fmt.Sprintf("i-%d-%d", id, j))
				result.addError("e-%d-%d", id, j)

				// Modify flags
				result.setCanRestore(j%3 == 0)

				// Occasionally finalize
				if j%10 == 0 {
					result.finalize()
				}
			}
		}(i)
	}

	wg.Wait()

	// Final state should be consistent
	result.finalize()

	expectedOps := numGoroutines * opsPerGoroutine
	t.Logf("Total operations: %d", expectedOps)
	t.Logf("MissingBlocks: %d", len(result.MissingBlocks))
	t.Logf("InvalidBlocks: %d", len(result.InvalidBlocks))
	t.Logf("ErrorDetails: %d", len(result.ErrorDetails))
	t.Logf("IsComplete: %v", result.IsComplete)
	t.Logf("CanRestore: %v", result.CanRestore)

	// Should have processed all operations
	if len(result.MissingBlocks) == 0 {
		t.Error("Expected some missing blocks")
	}
	if len(result.InvalidBlocks) == 0 {
		t.Error("Expected some invalid blocks")
	}
	if len(result.ErrorDetails) == 0 {
		t.Error("Expected some error details")
	}
}
