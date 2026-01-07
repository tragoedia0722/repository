package repository

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestEdgeCases_NewRepository 测试边界条件
func TestEdgeCases_NewRepository(t *testing.T) {
	t.Run("empty_path_returns_error", func(t *testing.T) {
		_, err := NewRepository("")
		if err == nil {
			t.Fatal("expected error for empty path, got nil")
		}
		if !strings.Contains(err.Error(), "cannot be empty") {
			t.Errorf("expected 'cannot be empty' error, got: %v", err)
		}
	})

	t.Run("path_with_trailing_slash", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-repo-trailing") + string(filepath.Separator)
		defer cleanupRepo(t, strings.TrimSuffix(tmpDir, string(filepath.Separator)))

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		repo.Close()
	})

	t.Run("path_with_dots", func(t *testing.T) {
		// Create a directory inside temp
		baseDir := filepath.Join(os.TempDir(), "test-repo-dots-base")
		if err := os.MkdirAll(baseDir, 0o755); err != nil {
			t.Fatalf("failed to create base dir: %v", err)
		}
		defer cleanupRepo(t, baseDir)

		// Use relative path with dots
		repoPath := filepath.Join(baseDir, "..", filepath.Base(baseDir))
		repo, err := NewRepository(repoPath)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		repo.Close()
	})
}

// TestEdgeCases_PutBlock 测试边界条件
func TestEdgeCases_PutBlock(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-edge-putblock")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("one_byte_block", func(t *testing.T) {
		data := []byte{0x42}
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed for 1 byte: %v", err)
		}

		retrieved, err := repo.GetRawData(ctx, cid.String())
		if err != nil {
			t.Fatalf("GetRawData failed: %v", err)
		}

		if len(retrieved) != 1 || retrieved[0] != 0x42 {
			t.Errorf("data mismatch: got %v, want [0x42]", retrieved)
		}
	})

	t.Run("maximum_size_block", func(t *testing.T) {
		// Create a block exactly at the limit (128MB)
		data := make([]byte, maxBlockSize)
		data[0] = 0x42
		data[maxBlockSize-1] = 0x24

		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed for max size: %v", err)
		}

		retrieved, err := repo.GetRawData(ctx, cid.String())
		if err != nil {
			t.Fatalf("GetRawData failed: %v", err)
		}

		if len(retrieved) != maxBlockSize {
			t.Errorf("size mismatch: got %d, want %d", len(retrieved), maxBlockSize)
		}
		if retrieved[0] != 0x42 || retrieved[maxBlockSize-1] != 0x24 {
			t.Error("first/last byte mismatch")
		}
	})

	t.Run("exceeds_maximum_size", func(t *testing.T) {
		data := make([]byte, maxBlockSize+1)

		_, err := repo.PutBlock(ctx, data)
		if err == nil {
			t.Fatal("expected error for block exceeding maximum size")
		}
		if !strings.Contains(err.Error(), "exceeds maximum") {
			t.Errorf("expected 'exceeds maximum' error, got: %v", err)
		}
	})

	t.Run("block_with_null_bytes", func(t *testing.T) {
		data := []byte{0x00, 0x00, 0x00}
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		retrieved, err := repo.GetRawData(ctx, cid.String())
		if err != nil {
			t.Fatalf("GetRawData failed: %v", err)
		}

		if len(retrieved) != 3 {
			t.Errorf("length mismatch: got %d, want 3", len(retrieved))
		}
		for i, b := range retrieved {
			if b != 0x00 {
				t.Errorf("byte %d mismatch: got 0x%02x, want 0x00", i, b)
			}
		}
	})

	t.Run("block_with_unicode", func(t *testing.T) {
		data := []byte("Hello 世界 🌍")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		retrieved, err := repo.GetRawData(ctx, cid.String())
		if err != nil {
			t.Fatalf("GetRawData failed: %v", err)
		}

		if string(retrieved) != string(data) {
			t.Errorf("data mismatch: got %q, want %q", retrieved, data)
		}
	})
}

// TestEdgeCases_PutManyBlocks 测试边界条件
func TestEdgeCases_PutManyBlocks(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-edge-putmany")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("single_block_in_slice", func(t *testing.T) {
		data := [][]byte{[]byte("single block")}
		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}

		if len(cids) != 1 {
			t.Errorf("expected 1 CID, got %d", len(cids))
		}
	})

	t.Run("large_batch", func(t *testing.T) {
		// Create 1000 small blocks
		data := make([][]byte, 1000)
		for i := range data {
			data[i] = []byte{byte(i % 256), byte((i / 256) % 256)}
		}

		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}

		if len(cids) != 1000 {
			t.Errorf("expected 1000 CIDs, got %d", len(cids))
		}
	})

	t.Run("blocks_with_varying_sizes", func(t *testing.T) {
		sizes := []int{1, 10, 100, 1000, 10000, 100000}
		data := make([][]byte, len(sizes))

		for i, size := range sizes {
			data[i] = make([]byte, size)
			for j := range data[i] {
				data[i][j] = byte(i)
			}
		}

		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}

		if len(cids) != len(sizes) {
			t.Errorf("expected %d CIDs, got %d", len(sizes), len(cids))
		}

		// Verify each block
		for i, cid := range cids {
			retrieved, err := repo.GetRawData(ctx, cid.String())
			if err != nil {
				t.Fatalf("GetRawData failed for block %d: %v", i, err)
			}
			if len(retrieved) != sizes[i] {
				t.Errorf("block %d size mismatch: got %d, want %d", i, len(retrieved), sizes[i])
			}
		}
	})

	t.Run("one_block_exceeds_limit", func(t *testing.T) {
		data := [][]byte{
			[]byte("valid block"),
			make([]byte, maxBlockSize+1), // exceeds limit
			[]byte("another valid block"),
		}

		_, err := repo.PutManyBlocks(ctx, data)
		if err == nil {
			t.Fatal("expected error when one block exceeds limit")
		}
		if !strings.Contains(err.Error(), "index 1") {
			t.Errorf("expected error for index 1, got: %v", err)
		}
	})
}

// TestEdgeCases_HasAllBlocks 测试边界条件
func TestEdgeCases_HasAllBlocks(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-edge-hasall")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("single_cid", func(t *testing.T) {
		data := []byte("single block")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		hasAll, err := repo.HasAllBlocks(ctx, []string{cid.String()})
		if err != nil {
			t.Fatalf("HasAllBlocks failed: %v", err)
		}
		if !hasAll {
			t.Error("expected true for single existing block")
		}
	})

	t.Run("large_list", func(t *testing.T) {
		// Create 500 blocks
		const count = 500
		data := make([][]byte, count)
		for i := range data {
			data[i] = []byte{byte(i)}
		}

		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}

		cidStrings := make([]string, len(cids))
		for i, c := range cids {
			cidStrings[i] = c.String()
		}

		hasAll, err := repo.HasAllBlocks(ctx, cidStrings)
		if err != nil {
			t.Fatalf("HasAllBlocks failed: %v", err)
		}
		if !hasAll {
			t.Error("expected true for all existing blocks")
		}
	})

	t.Run("all_missing", func(t *testing.T) {
		cidStrings := []string{
			"QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo",
			"QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsX1",
			"QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsX2",
		}

		hasAll, err := repo.HasAllBlocks(ctx, cidStrings)
		if err != nil {
			t.Fatalf("HasAllBlocks failed: %v", err)
		}
		if hasAll {
			t.Error("expected false for all missing blocks")
		}
	})

	t.Run("duplicate_cids", func(t *testing.T) {
		data := []byte("test data")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		cidStrings := []string{cid.String(), cid.String(), cid.String()}

		hasAll, err := repo.HasAllBlocks(ctx, cidStrings)
		if err != nil {
			t.Fatalf("HasAllBlocks failed: %v", err)
		}
		if !hasAll {
			t.Error("expected true for duplicate existing CID")
		}
	})
}

// TestEdgeCases_GetRawData 测试边界条件
func TestEdgeCases_GetRawData(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-edge-getraw")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("cancelled_context", func(t *testing.T) {
		data := []byte("test data")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		// Create a context that's already cancelled
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		// Should still return quickly even though context is cancelled
		// (because the block exists)
		_, err = repo.GetRawData(cancelledCtx, cid.String())
		if err != nil {
			t.Logf("GetRawData with cancelled context returned error: %v", err)
		}
	})

	t.Run("context_timeout_during_retry", func(t *testing.T) {
		// Use a non-existent CID that will trigger retries
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := repo.GetRawData(ctx, "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo")
		if err == nil {
			t.Fatal("expected error for non-existent block")
		}
		if err == context.DeadlineExceeded || err == context.Canceled {
			// Context was cancelled/timed out as expected
			t.Logf("Got expected error: %v", err)
		}
	})
}

// TestEdgeCases_DelBlock 测试边界条件
func TestEdgeCases_DelBlock(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-edge-del")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("delete_same_block_twice", func(t *testing.T) {
		data := []byte("delete twice test")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		// Delete first time
		err = repo.DelBlock(ctx, cid.String())
		if err != nil {
			t.Fatalf("first DelBlock failed: %v", err)
		}

		// Delete second time (should be idempotent or return error)
		err = repo.DelBlock(ctx, cid.String())
		t.Logf("second DelBlock returned: %v", err)
	})

	t.Run("delete_with_cancelled_context", func(t *testing.T) {
		data := []byte("cancel test")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err = repo.DelBlock(cancelledCtx, cid.String())
		if err != nil {
			t.Logf("DelBlock with cancelled context returned error: %v", err)
		}
	})
}

// TestEdgeCases_ContextCancellation 测试 context 取消的边界条件
func TestEdgeCases_ContextCancellation(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-edge-context")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	t.Run("PutManyBlocks_respects_cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Create a large slice of data
		data := make([][]byte, 10000)
		for i := range data {
			data[i] = make([]byte, 1024) // 1KB each
		}

		// Cancel after a short delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		_, err := repo.PutManyBlocks(ctx, data)
		if err != nil && err != context.Canceled {
			t.Logf("PutManyBlocks returned error: %v", err)
		}
	})

	t.Run("HasAllBlocks_respects_cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Create a large list of non-existent CIDs
		cids := make([]string, 10000)
		for i := range cids {
			cids[i] = "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"
		}

		// Cancel immediately
		cancel()

		_, err := repo.HasAllBlocks(ctx, cids)
		if err != nil && err != context.Canceled {
			t.Logf("HasAllBlocks returned error: %v", err)
		}
	})
}

// TestEdgeCases_LongPaths 测试长路径边界条件
func TestEdgeCases_LongPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long path test in short mode")
	}

	// Try to create a repository with a very long path
	longName := strings.Repeat("a", 200)
	tmpDir := filepath.Join(os.TempDir(), longName)
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		// Some filesystems have path length limits
		if os.IsNotExist(err) || strings.Contains(err.Error(), "name too long") {
			t.Skip("filesystem doesn't support long paths")
			return
		}
		t.Fatalf("NewRepository failed: %v", err)
	}
	repo.Close()
}

// TestEdgeCases_SpecialCharacters 测试特殊字符边界条件
func TestEdgeCases_SpecialCharacters(t *testing.T) {
	// Note: NewRepository uses filepath.Clean which normalizes paths
	// So we can't easily test special characters in directory names
	// But we can test data with special characters

	tmpDir := filepath.Join(os.TempDir(), "test-edge-special")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("data_with_newlines", func(t *testing.T) {
		data := []byte("line1\nline2\r\nline3\r")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		retrieved, err := repo.GetRawData(ctx, cid.String())
		if err != nil {
			t.Fatalf("GetRawData failed: %v", err)
		}

		if string(retrieved) != string(data) {
			t.Error("data with newlines mismatch")
		}
	})

	t.Run("data_with_tabs", func(t *testing.T) {
		data := []byte("\t\ttabbed\t\t")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		retrieved, err := repo.GetRawData(ctx, cid.String())
		if err != nil {
			t.Fatalf("GetRawData failed: %v", err)
		}

		if string(retrieved) != string(data) {
			t.Error("data with tabs mismatch")
		}
	})

	t.Run("data_with_quotes", func(t *testing.T) {
		data := []byte(`"quoted" 'single'`)
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		retrieved, err := repo.GetRawData(ctx, cid.String())
		if err != nil {
			t.Fatalf("GetRawData failed: %v", err)
		}

		if string(retrieved) != string(data) {
			t.Error("data with quotes mismatch")
		}
	})
}
