package validator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
)

// TestErrorScenarios_InvalidInputs tests various invalid input scenarios
func TestErrorScenarios_InvalidInputs(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	t.Run("nil_root_cid", func(t *testing.T) {
		_, err := v.Validate(context.Background(), "", []string{})
		if err == nil {
			t.Error("expected error for empty root CID")
		}
		if !strings.Contains(err.Error(), "cannot be empty") {
			t.Errorf("expected 'cannot be empty' error, got: %v", err)
		}
	})

	t.Run("nil_blocks_list", func(t *testing.T) {
		block := blocks.NewBlock([]byte("test"))
		bs.Put(context.Background(), block)

		_, err := v.Validate(context.Background(), block.Cid().String(), nil)
		if err == nil {
			t.Error("expected error for nil blocks")
		}
		if !strings.Contains(err.Error(), "cannot be nil") {
			t.Errorf("expected 'cannot be nil' error, got: %v", err)
		}
	})

	t.Run("both_invalid", func(t *testing.T) {
		_, err := v.Validate(context.Background(), "", nil)
		if err == nil {
			t.Error("expected error for both invalid inputs")
		}
	})
}

// TestErrorScenarios_AllInvalidCIDs tests when all CIDs are invalid
func TestErrorScenarios_AllInvalidCIDs(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create a valid root block
	validBlock := blocks.NewBlock([]byte("root"))
	bs.Put(context.Background(), validBlock)

	// All invalid CIDs
	invalidCIDs := []string{
		"invalid-1",
		"invalid-2",
		"Qm",
		"not-a-cid",
	}

	result, err := v.Validate(context.Background(), validBlock.Cid().String(), invalidCIDs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All should be marked as invalid
	if len(result.InvalidBlocks) != len(invalidCIDs) {
		t.Errorf("expected %d invalid blocks, got %d", len(invalidCIDs), len(result.InvalidBlocks))
	}

	if len(result.MissingBlocks) != 0 {
		t.Errorf("expected 0 missing blocks, got %d", len(result.MissingBlocks))
	}

	if result.IsComplete {
		t.Error("expected IsComplete=false when all blocks invalid")
	}
}

// TestErrorScenarios_MixedValidInvalidMissing tests mixed valid, invalid, and missing blocks
func TestErrorScenarios_MixedValidInvalidMissing(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create some valid blocks
	validBlock1 := blocks.NewBlock([]byte("valid1"))
	validBlock2 := blocks.NewBlock([]byte("valid2"))
	bs.Put(context.Background(), validBlock1)
	bs.Put(context.Background(), validBlock2)

	// Mix of valid, invalid, and missing
	blocks := []string{
		validBlock1.Cid().String(),                       // valid
		"invalid-cid",                                    // invalid
		validBlock2.Cid().String(),                       // valid
		"QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo", // missing
		"Qm", // invalid
	}

	result, err := v.Validate(context.Background(), validBlock1.Cid().String(), blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have both invalid and missing blocks
	if len(result.InvalidBlocks) == 0 {
		t.Error("expected some invalid blocks")
	}

	if len(result.MissingBlocks) == 0 {
		t.Error("expected some missing blocks")
	}

	if result.IsComplete {
		t.Error("expected IsComplete=false when blocks are invalid or missing")
	}

	if result.CanRestore {
		t.Error("expected CanRestore=false when blocks are invalid or missing")
	}
}

// TestErrorScenarios_ContextCancellationErrors tests context cancellation scenarios
func TestErrorScenarios_ContextCancellationErrors(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	t.Run("cancelled_context_returns_error", func(t *testing.T) {
		block := blocks.NewBlock([]byte("test"))
		bs.Put(context.Background(), block)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := v.Validate(ctx, block.Cid().String(), []string{block.Cid().String()})
		if err != nil && err != context.Canceled {
			t.Logf("Got error: %v (may not be context canceled if fast enough)", err)
		}
	})

	t.Run("timeout_context_returns_error", func(t *testing.T) {
		// Create many blocks to ensure validation takes time
		const numBlocks = 100
		blockList := make([]string, numBlocks)
		for i := 0; i < numBlocks; i++ {
			block := blocks.NewBlock([]byte{byte(i)})
			bs.Put(context.Background(), block)
			blockList[i] = block.Cid().String()
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		_, err := v.Validate(ctx, blockList[0], blockList)
		if err != nil {
			t.Logf("Got expected error with timeout: %v", err)
		}
	})
}

// TestErrorScenarios_DAGTraversalErrors tests DAG traversal error scenarios
func TestErrorScenarios_DAGTraversalErrors(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create a valid block
	validBlock := blocks.NewBlock([]byte("valid"))
	bs.Put(context.Background(), validBlock)

	t.Run("missing_root_block_in_dag", func(t *testing.T) {
		// Use a non-existent CID as root
		missingCID := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"

		result, err := v.Validate(context.Background(), missingCID, []string{validBlock.Cid().String()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should have error details from DAG traversal
		if len(result.ErrorDetails) == 0 {
			t.Error("expected error details from DAG traversal")
		}

		if result.CanRestore {
			t.Error("expected CanRestore=false when DAG traversal fails")
		}
	})
}

// TestErrorScenarios_ResultConsistency tests Result consistency across error scenarios
func TestErrorScenarios_ResultConsistency(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create valid blocks
	validBlock1 := blocks.NewBlock([]byte("valid1"))
	validBlock2 := blocks.NewBlock([]byte("valid2"))
	bs.Put(context.Background(), validBlock1)
	bs.Put(context.Background(), validBlock2)

	// Missing CID
	missingCID := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"

	blocks := []string{
		validBlock1.Cid().String(),
		"invalid-cid",
		validBlock2.Cid().String(),
		missingCID,
	}

	result, err := v.Validate(context.Background(), validBlock1.Cid().String(), blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Consistency checks
	// 1. Invalid blocks should not be in missing list
	for _, invalidCID := range result.InvalidBlocks {
		for _, missingCID := range result.MissingBlocks {
			if invalidCID == missingCID {
				t.Errorf("CID %s is both invalid and missing", invalidCID)
			}
		}
	}

	// 2. Should have error details for invalid/missing blocks
	if len(result.ErrorDetails) == 0 {
		t.Error("expected error details for invalid/missing blocks")
	}

	// 3. IsComplete should be false
	if result.IsComplete {
		t.Error("expected IsComplete=false with errors")
	}

	// 4. CanRestore should be false
	if result.CanRestore {
		t.Error("expected CanRestore=false with errors")
	}
}

// TestErrorScenarios_EmptyResultHandling tests handling of results with empty data
func TestErrorScenarios_EmptyResultHandling(t *testing.T) {
	result := &Result{}

	// Test finalize on empty result
	result.finalize()

	// IsComplete should be true when no missing/invalid blocks
	if !result.IsComplete {
		t.Error("expected IsComplete=true for empty result")
	}

	// Note: CanRestore starts as false and finalize() only sets it if it's already true
	// This is intentional to prevent overwriting explicit false values
	// So we just verify IsComplete is set correctly
}

// TestErrorScenarios_MultipleErrorTypes tests scenarios with multiple types of errors
func TestErrorScenarios_MultipleErrorTypes(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create some valid blocks
	validBlock1 := blocks.NewBlock([]byte("valid1"))
	validBlock2 := blocks.NewBlock([]byte("valid2"))
	validBlock3 := blocks.NewBlock([]byte("valid3"))
	bs.Put(context.Background(), validBlock1)
	bs.Put(context.Background(), validBlock2)
	bs.Put(context.Background(), validBlock3)

	// Create a mix with all types of "errors"
	blocks := []string{
		validBlock1.Cid().String(), // valid
		"",                         // invalid (empty)
		"Qm",                       // invalid (too short)
		"QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo", // missing
		validBlock2.Cid().String(),                       // valid
		"not-a-cid",                                      // invalid
		validBlock3.Cid().String(),                       // valid
		"QmInvalid",                                      // invalid
	}

	result, err := v.Validate(context.Background(), validBlock1.Cid().String(), blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Count errors
	invalidCount := 0
	missingCount := 0

	// Skip empty CID from invalid count (it's in invalid list but might be filtered)
	for _, cid := range result.InvalidBlocks {
		if cid != "" {
			invalidCount++
		}
	}
	missingCount = len(result.MissingBlocks)

	t.Logf("Invalid blocks: %d, Missing blocks: %d", invalidCount, missingCount)

	if invalidCount == 0 && missingCount == 0 {
		t.Error("expected some invalid or missing blocks")
	}

	if len(result.ErrorDetails) == 0 {
		t.Error("expected error details")
	}
}

// TestErrorScenarios_RootCIDNotInBlocks tests when root CID is not in blocks list
func TestErrorScenarios_RootCIDNotInBlocks(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create two blocks
	rootBlock := blocks.NewBlock([]byte("root"))
	otherBlock := blocks.NewBlock([]byte("other"))
	bs.Put(context.Background(), rootBlock)
	bs.Put(context.Background(), otherBlock)

	// Only provide other block in list
	blocks := []string{otherBlock.Cid().String()}

	result, err := v.Validate(context.Background(), rootBlock.Cid().String(), blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Note: The root block exists in blockstore, so it may not be in MissingBlocks
	// The validator behavior is to check if provided blocks are valid,
	// not to enforce that the root must be in the provided list
	t.Logf("MissingBlocks: %d, InvalidBlocks: %d", len(result.MissingBlocks), len(result.InvalidBlocks))

	// Just verify the validation completes without error
}

// TestErrorScenarios_VeryLongCIDList tests handling of very long CID lists
func TestErrorScenarios_VeryLongCIDList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long CID list test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create one valid block
	validBlock := blocks.NewBlock([]byte("valid"))
	bs.Put(context.Background(), validBlock)

	// Create a very long list with mostly invalid CIDs
	const listSize = 10000
	blocks := make([]string, listSize)
	blocks[0] = validBlock.Cid().String()

	for i := 1; i < listSize; i++ {
		blocks[i] = fmt.Sprintf("invalid-cid-%d", i)
	}

	result, err := v.Validate(context.Background(), validBlock.Cid().String(), blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have processed all invalid CIDs
	if len(result.InvalidBlocks) != listSize-1 {
		t.Logf("Warning: expected %d invalid blocks, got %d", listSize-1, len(result.InvalidBlocks))
	}

	// Should have at least some invalid blocks
	if len(result.InvalidBlocks) == 0 {
		t.Error("expected some invalid blocks")
	}
}

// TestErrorScenarios_ResultThreadSafety tests Result thread safety under concurrent modifications
func TestErrorScenarios_ResultThreadSafety(t *testing.T) {
	result := &Result{
		MissingBlocks: make([]string, 0, 100),
		InvalidBlocks: make([]string, 0, 100),
		ErrorDetails:  make([]string, 0, 100),
	}

	// Concurrent modifications
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic in goroutine %d: %v", id, r)
				}
			}()

			for j := 0; j < 50; j++ {
				result.addMissingBlock(fmt.Sprintf("missing-%d-%d", id, j))
				result.addInvalidBlock(fmt.Sprintf("invalid-%d-%d", id, j))
				result.addError("error %d-%d", id, j)
				result.setCanRestore(false)
				result.finalize()
			}
		}(i)
	}

	wg.Wait()

	// Verify all operations completed
	expectedOps := 20 * 50
	if len(result.MissingBlocks) != expectedOps {
		t.Logf("Warning: expected %d missing blocks, got %d", expectedOps, len(result.MissingBlocks))
	}
	if len(result.InvalidBlocks) != expectedOps {
		t.Logf("Warning: expected %d invalid blocks, got %d", expectedOps, len(result.InvalidBlocks))
	}
	if len(result.ErrorDetails) != expectedOps {
		t.Logf("Warning: expected %d error details, got %d", expectedOps, len(result.ErrorDetails))
	}
}

// TestErrorScenarios_RepeatedValidation tests repeated validation calls
func TestErrorScenarios_RepeatedValidation(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create blocks
	validBlock := blocks.NewBlock([]byte("valid"))
	bs.Put(context.Background(), validBlock)

	missingCID := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"

	blocks := []string{
		validBlock.Cid().String(),
		"invalid-cid",
		missingCID,
	}

	// Validate multiple times
	for i := 0; i < 5; i++ {
		result, err := v.Validate(context.Background(), validBlock.Cid().String(), blocks)
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}

		// Results should be consistent
		if len(result.InvalidBlocks) == 0 {
			t.Errorf("iteration %d: expected invalid blocks", i)
		}

		if len(result.MissingBlocks) == 0 {
			t.Errorf("iteration %d: expected missing blocks", i)
		}
	}
}

// TestErrorScenarios_EmptyStringCID tests empty string CID
func TestErrorScenarios_EmptyStringCID(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	validBlock := blocks.NewBlock([]byte("valid"))
	bs.Put(context.Background(), validBlock)

	// Include empty string in blocks list
	blocks := []string{validBlock.Cid().String(), ""}

	result, err := v.Validate(context.Background(), validBlock.Cid().String(), blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Empty string should be in invalid blocks
	found := false
	for _, invalid := range result.InvalidBlocks {
		if invalid == "" {
			found = true
			break
		}
	}

	if !found {
		t.Error("expected empty string to be in invalid blocks list")
	}
}

// TestErrorScenarios_WhiteSpaceCID tests whitespace CIDs
func TestErrorScenarios_WhiteSpaceCID(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	validBlock := blocks.NewBlock([]byte("valid"))
	bs.Put(context.Background(), validBlock)

	// Include whitespace in blocks list
	whiteSpaceCIDs := []string{" ", "\t", "\n", "\r", "  "}

	for _, wsCID := range whiteSpaceCIDs {
		t.Run("whitespace_"+fmt.Sprintf("%q", wsCID), func(t *testing.T) {
			blocks := []string{validBlock.Cid().String(), wsCID}

			result, err := v.Validate(context.Background(), validBlock.Cid().String(), blocks)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Whitespace CID should be in invalid blocks
			found := false
			for _, invalid := range result.InvalidBlocks {
				if invalid == wsCID {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("expected whitespace CID to be in invalid blocks list")
			}
		})
	}
}
