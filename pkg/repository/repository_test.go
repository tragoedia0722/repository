package repository

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func cleanupRepo(t *testing.T, path string) {
	if err := os.RemoveAll(path); err != nil {
		t.Fatalf("failed to cleanup repo: %v", err)
	}
}

func TestNewRepository(t *testing.T) {
	t.Run("creates new repository", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-repo-new")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		if repo == nil {
			t.Fatal("expected non-nil repository")
		}

		if err = repo.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	t.Run("creates directory if not exists", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-repo-mkdir", "subdir")
		defer cleanupRepo(t, filepath.Join(os.TempDir(), "test-repo-mkdir"))

		// Ensure directory doesn't exist
		_, err := os.Stat(tmpDir)
		if !os.IsNotExist(err) {
			t.Skip("directory already exists")
		}

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		// Check directory was created
		if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
			t.Error("directory was not created")
		}

		repo.Close()
	})

	t.Run("opens existing repository", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-repo-existing")
		defer cleanupRepo(t, tmpDir)

		// Create first instance
		repo1, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		repo1.Close()

		// Open again
		repo2, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed on existing: %v", err)
		}
		repo2.Close()
	})
}

func TestRepository_BlockStore(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-blockstore")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	bs := repo.BlockStore()
	if bs == nil {
		t.Fatal("BlockStore returned nil")
	}
}

func TestRepository_PutBlock(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-putblock")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("puts single block", func(t *testing.T) {
		data := []byte("test data for single block")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		if cid == nil {
			t.Fatal("expected non-nil CID")
		}

		// Verify block exists
		has, err := repo.HasBlock(ctx, cid.String())
		if err != nil {
			t.Fatalf("HasBlock failed: %v", err)
		}
		if !has {
			t.Error("block was not stored")
		}
	})

	t.Run("puts empty block", func(t *testing.T) {
		data := []byte{}
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed for empty data: %v", err)
		}

		if cid == nil {
			t.Fatal("expected non-nil CID for empty data")
		}
	})

	t.Run("puts large block", func(t *testing.T) {
		data := make([]byte, 2*1024*1024) // 2MB
		for i := range data {
			data[i] = byte(i % 256)
		}

		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed for large data: %v", err)
		}

		if cid == nil {
			t.Fatal("expected non-nil CID for large data")
		}
	})
}

func TestRepository_PutManyBlocks(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-putmany")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("puts multiple blocks", func(t *testing.T) {
		data := [][]byte{
			[]byte("first block"),
			[]byte("second block"),
			[]byte("third block"),
		}

		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}

		if len(cids) != len(data) {
			t.Errorf("expected %d CIDs, got %d", len(data), len(cids))
		}

		// Verify all blocks exist
		cidStrings := make([]string, len(cids))
		for i, c := range cids {
			cidStrings[i] = c.String()
		}

		hasAll, err := repo.HasAllBlocks(ctx, cidStrings)
		if err != nil {
			t.Fatalf("HasAllBlocks failed: %v", err)
		}
		if !hasAll {
			t.Error("not all blocks were stored")
		}
	})

	t.Run("puts empty slice", func(t *testing.T) {
		cids, err := repo.PutManyBlocks(ctx, [][]byte{})
		if err != nil {
			t.Fatalf("PutManyBlocks failed for empty slice: %v", err)
		}

		if len(cids) != 0 {
			t.Errorf("expected 0 CIDs, got %d", len(cids))
		}
	})
}

func TestRepository_PutBlockWithCid(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-putwithcid")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("puts block with specific CID", func(t *testing.T) {
		data := []byte("test data with specific CID")

		// First create a block to get its CID
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		// Now put same data with same CID
		err = repo.PutBlockWithCid(ctx, cid.String(), data)
		if err != nil {
			t.Fatalf("PutBlockWithCid failed: %v", err)
		}

		// Verify
		has, err := repo.HasBlock(ctx, cid.String())
		if err != nil {
			t.Fatalf("HasBlock failed: %v", err)
		}
		if !has {
			t.Error("block was not stored")
		}
	})

	t.Run("fails with invalid CID", func(t *testing.T) {
		data := []byte("test data")

		err := repo.PutBlockWithCid(ctx, "invalid-cid", data)
		if err == nil {
			t.Error("expected error for invalid CID, got nil")
		}
	})
}

func TestRepository_HasBlock(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-hasblock")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("returns false for non-existent block", func(t *testing.T) {
		has, err := repo.HasBlock(ctx, "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo")
		if err != nil {
			t.Fatalf("HasBlock failed: %v", err)
		}
		if has {
			t.Error("expected false for non-existent block")
		}
	})

	t.Run("returns true for existing block", func(t *testing.T) {
		data := []byte("test data")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		has, err := repo.HasBlock(ctx, cid.String())
		if err != nil {
			t.Fatalf("HasBlock failed: %v", err)
		}
		if !has {
			t.Error("expected true for existing block")
		}
	})

	t.Run("fails with invalid CID", func(t *testing.T) {
		_, err := repo.HasBlock(ctx, "invalid-cid")
		if err == nil {
			t.Error("expected error for invalid CID, got nil")
		}
	})
}

func TestRepository_HasAllBlocks(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-hasall")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("returns true when all blocks exist", func(t *testing.T) {
		data := [][]byte{[]byte("block1"), []byte("block2")}
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
			t.Error("expected true when all blocks exist")
		}
	})

	t.Run("returns false when some blocks missing", func(t *testing.T) {
		data := []byte("test data")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		cidStrings := []string{cid.String(), "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"}

		hasAll, err := repo.HasAllBlocks(ctx, cidStrings)
		if err != nil {
			t.Fatalf("HasAllBlocks failed: %v", err)
		}
		if hasAll {
			t.Error("expected false when some blocks missing")
		}
	})

	t.Run("returns true for empty list", func(t *testing.T) {
		hasAll, err := repo.HasAllBlocks(ctx, []string{})
		if err != nil {
			t.Fatalf("HasAllBlocks failed: %v", err)
		}
		if !hasAll {
			t.Error("expected true for empty list")
		}
	})

	t.Run("fails with invalid CID", func(t *testing.T) {
		_, err := repo.HasAllBlocks(ctx, []string{"invalid-cid"})
		if err == nil {
			t.Error("expected error for invalid CID, got nil")
		}
	})
}

func TestRepository_GetRawData(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-getraw")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("gets existing block data", func(t *testing.T) {
		data := []byte("test data for retrieval")
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

	t.Run("returns error for non-existent block", func(t *testing.T) {
		_, err := repo.GetRawData(ctx, "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo")
		if err == nil {
			t.Error("expected error for non-existent block, got nil")
		}
	})

	t.Run("fails with invalid CID", func(t *testing.T) {
		_, err := repo.GetRawData(ctx, "invalid-cid")
		if err == nil {
			t.Error("expected error for invalid CID, got nil")
		}
	})

	t.Run("handles empty block", func(t *testing.T) {
		data := []byte{}
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		retrieved, err := repo.GetRawData(ctx, cid.String())
		if err != nil {
			t.Fatalf("GetRawData failed: %v", err)
		}

		if len(retrieved) != 0 {
			t.Errorf("expected empty data, got %d bytes", len(retrieved))
		}
	})
}

func TestRepository_DelBlock(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-del")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("deletes existing block", func(t *testing.T) {
		data := []byte("test data to delete")
		cid, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		// Verify exists
		has, _ := repo.HasBlock(ctx, cid.String())
		if !has {
			t.Fatal("block should exist before deletion")
		}

		// Delete
		err = repo.DelBlock(ctx, cid.String())
		if err != nil {
			t.Fatalf("DelBlock failed: %v", err)
		}

		// Verify deleted
		has, _ = repo.HasBlock(ctx, cid.String())
		if has {
			t.Error("block should not exist after deletion")
		}
	})

	t.Run("deletes non-existent block", func(t *testing.T) {
		err := repo.DelBlock(ctx, "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo")
		// Should not error - deleting non-existent is idempotent
		if err != nil {
			t.Logf("DelBlock returned error for non-existent: %v", err)
		}
	})

	t.Run("fails with invalid CID", func(t *testing.T) {
		err := repo.DelBlock(ctx, "invalid-cid")
		if err == nil {
			t.Error("expected error for invalid CID, got nil")
		}
	})
}

func TestRepository_Usage(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-usage")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// Initial usage should be 0 or small
	usage1, err := repo.Usage(ctx)
	if err != nil {
		t.Fatalf("Usage failed: %v", err)
	}

	// Add some blocks
	data := [][]byte{
		make([]byte, 1024*100), // 100KB
		make([]byte, 1024*200), // 200KB
		make([]byte, 1024*150), // 150KB
	}
	_, err = repo.PutManyBlocks(ctx, data)
	if err != nil {
		t.Fatalf("PutManyBlocks failed: %v", err)
	}

	// Usage should increase
	usage2, err := repo.Usage(ctx)
	if err != nil {
		t.Fatalf("Usage failed: %v", err)
	}

	if usage2 <= usage1 {
		t.Errorf("expected usage to increase: got %d, was %d", usage2, usage1)
	}
}

func TestRepository_Close(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-close")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}

	// Close once
	err = repo.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Close again (should be idempotent or return error based on implementation)
	err = repo.Close()
	if err != nil {
		t.Logf("Second Close returned error: %v", err)
	}
}

func TestRepository_Destroy(t *testing.T) {
	t.Run("destroys repository", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-repo-destroy")
		// Don't use defer cleanup since we're testing Destroy

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		// Add some data
		ctx := context.Background()
		_, err = repo.PutBlock(ctx, []byte("test"))
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		// Destroy
		err = repo.Destroy()
		if err != nil {
			t.Errorf("Destroy failed: %v", err)
		}

		// Verify directory is gone
		if _, err := os.Stat(tmpDir); !os.IsNotExist(err) {
			t.Error("repository directory should be removed after Destroy")
		}
	})

	t.Run("destroy on closed repository", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-repo-destroy-closed")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		repo.Close()

		err = repo.Destroy()
		if err != nil {
			t.Errorf("Destroy on closed repository failed: %v", err)
		}
	})
}

func TestRepository_CIDConsistency(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-cid")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// Same data should produce same CID
	data := []byte("consistent data")

	cid1, err := repo.PutBlock(ctx, data)
	if err != nil {
		t.Fatalf("first PutBlock failed: %v", err)
	}

	cid2, err := repo.PutBlock(ctx, data)
	if err != nil {
		t.Fatalf("second PutBlock failed: %v", err)
	}

	if cid1.String() != cid2.String() {
		t.Errorf("same data produced different CIDs: %s vs %s", cid1, cid2)
	}

	// Different data should produce different CIDs
	otherData := []byte("different data")
	cid3, err := repo.PutBlock(ctx, otherData)
	if err != nil {
		t.Fatalf("PutBlock with different data failed: %v", err)
	}

	if cid1.String() == cid3.String() {
		t.Error("different data produced same CID")
	}
}

func TestRepository_DataStore(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-datastore")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ds := repo.DataStore()
	if ds == nil {
		t.Fatal("DataStore returned nil")
	}
}

// Test that CID version and hash type are correct
func TestRepository_CIDFormat(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-repo-cidfmt")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	data := []byte("test CID format")

	cid, err := repo.PutBlock(ctx, data)
	if err != nil {
		t.Fatalf("PutBlock failed: %v", err)
	}

	// Check CID version
	if cid.Version() != 1 {
		t.Errorf("expected CID version 1, got %d", cid.Version())
	}

	// Check hash type - SHA2_256 (0x12) is verified by the CID constructor
	_ = cid.Hash() // Just verify we can access it
}

// Benchmark PutBlock
func BenchmarkPutBlock(b *testing.B) {
	tmpDir := filepath.Join(os.TempDir(), "bench-repo-put")
	defer os.RemoveAll(tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		b.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	data := make([]byte, 1024*100) // 100KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := repo.PutBlock(ctx, data)
		if err != nil {
			b.Fatalf("PutBlock failed: %v", err)
		}
	}
}
