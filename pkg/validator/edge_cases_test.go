package validator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
)

// TestEdgeCases_EmptyInputs tests various empty input scenarios
func TestEdgeCases_EmptyInputs(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	t.Run("empty_blocks_slice", func(t *testing.T) {
		block := blocks.NewBlock([]byte("test data"))
		bs.Put(context.Background(), block)

		result, err := v.Validate(context.Background(), block.Cid().String(), []string{})
		if err != nil {
			t.Fatalf("unexpected error with empty blocks slice: %v", err)
		}

		// Empty slice is allowed - DAG traversal will still happen
		if result.MissingBlocks == nil {
			t.Error("MissingBlocks should be initialized")
		}
		if result.InvalidBlocks == nil {
			t.Error("InvalidBlocks should be initialized")
		}
	})

	t.Run("nil_blocks_returns_error", func(t *testing.T) {
		block := blocks.NewBlock([]byte("test data"))
		bs.Put(context.Background(), block)

		_, err := v.Validate(context.Background(), block.Cid().String(), nil)
		if err == nil {
			t.Error("expected error for nil blocks, got nil")
		}
		if !strings.Contains(err.Error(), "cannot be nil") {
			t.Errorf("expected 'cannot be nil' error, got: %v", err)
		}
	})

	t.Run("empty_root_cid_returns_error", func(t *testing.T) {
		_, err := v.Validate(context.Background(), "", []string{})
		if err == nil {
			t.Error("expected error for empty root CID, got nil")
		}
		if !strings.Contains(err.Error(), "cannot be empty") {
			t.Errorf("expected 'cannot be empty' error, got: %v", err)
		}
	})
}

// TestEdgeCases_SingleBlock tests single block scenarios
func TestEdgeCases_SingleBlock(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	t.Run("single_valid_block", func(t *testing.T) {
		block := blocks.NewBlock([]byte("single block data"))
		bs.Put(context.Background(), block)

		result, err := v.Validate(context.Background(), block.Cid().String(), []string{block.Cid().String()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.InvalidBlocks) != 0 {
			t.Errorf("expected 0 invalid blocks, got %d", len(result.InvalidBlocks))
		}
	})

	t.Run("single_byte_block", func(t *testing.T) {
		block := blocks.NewBlock([]byte{0x42})
		bs.Put(context.Background(), block)

		result, err := v.Validate(context.Background(), block.Cid().String(), []string{block.Cid().String()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.InvalidBlocks) != 0 {
			t.Errorf("expected 0 invalid blocks, got %d", len(result.InvalidBlocks))
		}
	})
}

// TestEdgeCases_SpecialCharacters tests blocks with special characters
func TestEdgeCases_SpecialCharacters(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	t.Run("block_with_null_bytes", func(t *testing.T) {
		data := []byte{0x00, 0x00, 0x00, 0x42}
		block := blocks.NewBlock(data)
		bs.Put(context.Background(), block)

		result, err := v.Validate(context.Background(), block.Cid().String(), []string{block.Cid().String()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.InvalidBlocks) != 0 {
			t.Errorf("expected 0 invalid blocks, got %d", len(result.InvalidBlocks))
		}
	})

	t.Run("block_with_unicode", func(t *testing.T) {
		data := []byte("Hello 世界 🌍")
		block := blocks.NewBlock(data)
		bs.Put(context.Background(), block)

		result, err := v.Validate(context.Background(), block.Cid().String(), []string{block.Cid().String()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.InvalidBlocks) != 0 {
			t.Errorf("expected 0 invalid blocks, got %d", len(result.InvalidBlocks))
		}
	})

	t.Run("block_with_newlines", func(t *testing.T) {
		data := []byte("line1\nline2\r\nline3\r")
		block := blocks.NewBlock(data)
		bs.Put(context.Background(), block)

		result, err := v.Validate(context.Background(), block.Cid().String(), []string{block.Cid().String()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.InvalidBlocks) != 0 {
			t.Errorf("expected 0 invalid blocks, got %d", len(result.InvalidBlocks))
		}
	})
}

// TestEdgeCases_DuplicateCIDs tests duplicate CID handling
func TestEdgeCases_DuplicateCIDs(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	block := blocks.NewBlock([]byte("test data"))
	bs.Put(context.Background(), block)

	cid := block.Cid().String()

	// Provide same CID multiple times
	blocks := []string{cid, cid, cid, cid}

	result, err := v.Validate(context.Background(), cid, blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should handle duplicates without error
	if len(result.InvalidBlocks) != 0 {
		t.Errorf("expected 0 invalid blocks, got %d", len(result.InvalidBlocks))
	}

	if len(result.MissingBlocks) != 0 {
		t.Errorf("expected 0 missing blocks, got %d", len(result.MissingBlocks))
	}
}

// TestEdgeCases_LargeBlockList tests handling of large block lists
func TestEdgeCases_LargeBlockList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large block list test in short mode")
	}

	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create 1000 blocks
	const numBlocks = 1000
	blockList := make([]string, numBlocks)

	for i := 0; i < numBlocks; i++ {
		data := []byte{byte(i), byte(i >> 8)}
		block := blocks.NewBlock(data)
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	// Use first block as root
	result, err := v.Validate(context.Background(), blockList[0], blockList)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.InvalidBlocks) != 0 {
		t.Errorf("expected 0 invalid blocks, got %d", len(result.InvalidBlocks))
	}

	if len(result.MissingBlocks) != 0 {
		t.Errorf("expected 0 missing blocks, got %d", len(result.MissingBlocks))
	}
}

// TestEdgeCases_ContextCancellation tests context cancellation behavior
func TestEdgeCases_ContextCancellation(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create a block
	block := blocks.NewBlock([]byte("test data"))
	bs.Put(context.Background(), block)

	t.Run("cancelled_context_before_validation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel before calling Validate

		_, err := v.Validate(ctx, block.Cid().String(), []string{block.Cid().String()})
		// Should return context canceled error or succeed if fast enough
		if err != nil && err != context.Canceled {
			t.Logf("Got error: %v (may be expected)", err)
		}
	})

	t.Run("timeout_during_validation", func(t *testing.T) {
		// Create a large list of blocks to ensure validation takes some time
		const numBlocks = 100
		blockList := make([]string, numBlocks)
		for i := 0; i < numBlocks; i++ {
			block := blocks.NewBlock([]byte{byte(i)})
			bs.Put(context.Background(), block)
			blockList[i] = block.Cid().String()
		}

		// Set a very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
		defer cancel()

		// Give context time to cancel
		time.Sleep(10 * time.Millisecond)

		_, err := v.Validate(ctx, blockList[0], blockList)
		if err != nil {
			t.Logf("Got expected error with timeout: %v", err)
		}
	})
}

// TestEdgeCases_MixedValidInvalidCIDs tests mixed valid and invalid CIDs
func TestEdgeCases_MixedValidInvalidCIDs(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create valid blocks
	validBlock1 := blocks.NewBlock([]byte("valid1"))
	validBlock2 := blocks.NewBlock([]byte("valid2"))
	bs.Put(context.Background(), validBlock1)
	bs.Put(context.Background(), validBlock2)

	// Mix valid and invalid CIDs
	blocks := []string{
		validBlock1.Cid().String(),
		"invalid-cid-1",
		validBlock2.Cid().String(),
		"invalid-cid-2",
		"Qm", // Too short
		"NotACID",
	}

	result, err := v.Validate(context.Background(), validBlock1.Cid().String(), blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have invalid blocks
	if len(result.InvalidBlocks) == 0 {
		t.Error("expected some invalid blocks")
	}

	// Should have some valid blocks
	if len(result.InvalidBlocks) == len(blocks) {
		t.Error("expected some valid blocks")
	}

	// Check that error details were added
	if len(result.ErrorDetails) == 0 {
		t.Error("expected error details")
	}

	// Valid blocks should not be in missing list
	for _, cid := range []string{validBlock1.Cid().String(), validBlock2.Cid().String()} {
		for _, missing := range result.MissingBlocks {
			if missing == cid {
				t.Errorf("valid block %s should not be in missing list", cid)
			}
		}
	}
}

// TestEdgeCases_ResultFinalization tests Result.finalize() behavior
func TestEdgeCases_ResultFinalization(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	t.Run("complete_validation", func(t *testing.T) {
		block := blocks.NewBlock([]byte("test"))
		bs.Put(context.Background(), block)

		result, err := v.Validate(context.Background(), block.Cid().String(), []string{block.Cid().String()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.ErrorDetails) == 0 {
			// Only check if no DAG errors
			if !result.IsComplete {
				t.Error("expected IsComplete=true when all blocks valid")
			}
			if !result.CanRestore {
				t.Error("expected CanRestore=true when all blocks valid")
			}
		}
	})

	t.Run("incomplete_validation_with_missing", func(t *testing.T) {
		validBlock := blocks.NewBlock([]byte("valid"))
		bs.Put(context.Background(), validBlock)

		// Include a missing CID
		missingCID := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"
		blocks := []string{validBlock.Cid().String(), missingCID}

		result, err := v.Validate(context.Background(), validBlock.Cid().String(), blocks)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.IsComplete {
			t.Error("expected IsComplete=false when blocks are missing")
		}
		if result.CanRestore {
			t.Error("expected CanRestore=false when blocks are missing")
		}
	})

	t.Run("incomplete_validation_with_invalid", func(t *testing.T) {
		validBlock := blocks.NewBlock([]byte("valid"))
		bs.Put(context.Background(), validBlock)

		// Include an invalid CID
		blocks := []string{validBlock.Cid().String(), "invalid-cid"}

		result, err := v.Validate(context.Background(), validBlock.Cid().String(), blocks)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.IsComplete {
			t.Error("expected IsComplete=false when blocks are invalid")
		}
		if result.CanRestore {
			t.Error("expected CanRestore=false when blocks are invalid")
		}
	})
}

// TestEdgeCases_VaryingBlockSizes tests blocks of varying sizes
func TestEdgeCases_VaryingBlockSizes(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	sizes := []int{1, 10, 100, 1024, 10240, 102400}
	blockList := make([]string, len(sizes))

	for i, size := range sizes {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(i)
		}
		block := blocks.NewBlock(data)
		bs.Put(context.Background(), block)
		blockList[i] = block.Cid().String()
	}

	result, err := v.Validate(context.Background(), blockList[0], blockList)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.InvalidBlocks) != 0 {
		t.Errorf("expected 0 invalid blocks, got %d", len(result.InvalidBlocks))
	}

	if len(result.MissingBlocks) != 0 {
		t.Errorf("expected 0 missing blocks, got %d", len(result.MissingBlocks))
	}
}

// TestEdgeCases_InvalidCIDFormats tests various invalid CID formats
func TestEdgeCases_InvalidCIDFormats(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	validBlock := blocks.NewBlock([]byte("valid"))
	bs.Put(context.Background(), validBlock)

	invalidCIDs := []string{
		"",
		"Qm",
		"Qm!!!",
		"z",
		"not-a-cid",
		"12345",
		"Qm" + strings.Repeat("a", 100),
		"INVALID",
	}

	for _, invalidCID := range invalidCIDs {
		t.Run("invalid_"+invalidCID, func(t *testing.T) {
			blocks := []string{validBlock.Cid().String(), invalidCID}

			result, err := v.Validate(context.Background(), validBlock.Cid().String(), blocks)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(result.InvalidBlocks) == 0 {
				t.Error("expected invalid block to be marked")
			}

			// Verify the invalid CID is in InvalidBlocks list
			found := false
			for _, cid := range result.InvalidBlocks {
				if cid == invalidCID {
					found = true
					break
				}
			}
			if !found && invalidCID != "" {
				t.Errorf("invalid CID %s not found in InvalidBlocks list", invalidCID)
			}
		})
	}
}

// TestEdgeCases_AllBlocksMissing tests when all provided blocks are missing
func TestEdgeCases_AllBlocksMissing(t *testing.T) {
	bs := newMockBlockstore()
	v := NewValidator(bs)

	// Create a block that exists
	existingBlock := blocks.NewBlock([]byte("exists"))
	bs.Put(context.Background(), existingBlock)

	// But only provide missing CIDs
	missingCID1 := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"
	missingCID2 := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsX1"

	blocks := []string{missingCID1, missingCID2}

	result, err := v.Validate(context.Background(), existingBlock.Cid().String(), blocks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Both should be missing
	if len(result.MissingBlocks) != 2 {
		t.Errorf("expected 2 missing blocks, got %d", len(result.MissingBlocks))
	}

	if result.IsComplete {
		t.Error("expected IsComplete=false when all blocks missing")
	}
}

// TestEdgeCases_ThreadSafeResultOperations tests that Result operations are thread-safe
func TestEdgeCases_ThreadSafeResultOperations(t *testing.T) {
	result := &Result{
		MissingBlocks: make([]string, 0, 10),
		InvalidBlocks: make([]string, 0, 10),
		ErrorDetails:  make([]string, 0, 10),
	}

	// Concurrently add to result
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				result.addMissingBlock(fmt.Sprintf("missing-%d-%d", id, j))
				result.addInvalidBlock(fmt.Sprintf("invalid-%d-%d", id, j))
				result.addError("error %d-%d", id, j)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	result.finalize()

	// Should have processed all operations without race
	expectedAdds := 10 * 100
	if len(result.MissingBlocks) != expectedAdds {
		t.Errorf("expected %d missing blocks, got %d", expectedAdds, len(result.MissingBlocks))
	}
	if len(result.InvalidBlocks) != expectedAdds {
		t.Errorf("expected %d invalid blocks, got %d", expectedAdds, len(result.InvalidBlocks))
	}
	if len(result.ErrorDetails) != expectedAdds {
		t.Errorf("expected %d error details, got %d", expectedAdds, len(result.ErrorDetails))
	}
}
