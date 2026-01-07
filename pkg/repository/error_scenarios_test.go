package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestErrorScenarios_NewRepository 测试错误情况
func TestErrorScenarios_NewRepository(t *testing.T) {
	t.Run("invalid_path_empty_string", func(t *testing.T) {
		_, err := NewRepository("")
		if err == nil {
			t.Fatal("expected error for empty path")
		}
		if !strings.Contains(err.Error(), "cannot be empty") {
			t.Errorf("expected 'cannot be empty' error, got: %v", err)
		}
	})

	t.Run("invalid_path_with_null_byte", func(t *testing.T) {
		invalidPath := "test\x00path"
		_, err := NewRepository(invalidPath)
		if err == nil {
			t.Fatal("expected error for path with null byte")
		}
	})

	t.Run("cannot_create_directory", func(t *testing.T) {
		// Try to create a directory in /proc which should fail
		invalidPath := "/proc/invalid_repo_test_12345"

		_, err := NewRepository(invalidPath)
		if err == nil {
			t.Fatal("expected error when creating directory in /proc")
		}
		t.Logf("Got expected error: %v", err)
	})
}

// TestErrorScenarios_PutBlock 测试错误情况
func TestErrorScenarios_PutBlock(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-error-putblock")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("block_too_large", func(t *testing.T) {
		largeData := make([]byte, maxBlockSize+1)

		_, err := repo.PutBlock(ctx, largeData)
		if err == nil {
			t.Fatal("expected error for block exceeding maximum size")
		}

		if !strings.Contains(err.Error(), "exceeds maximum") {
			t.Errorf("expected 'exceeds maximum' error, got: %v", err)
		}
	})

	t.Run("exactly_at_limit", func(t *testing.T) {
		// This should succeed
		data := make([]byte, maxBlockSize)
		_, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Errorf("PutBlock failed for block at size limit: %v", err)
		}
	})

	t.Run("nil_data", func(t *testing.T) {
		// PutBlock doesn't accept nil, but we test with empty slice instead
		data := []byte(nil)
		_, err := repo.PutBlock(ctx, data)
		if err != nil {
			t.Errorf("PutBlock failed for nil data: %v", err)
		}
	})

	t.Run("context_cancelled_during_put", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		data := []byte("test data")
		_, err := repo.PutBlock(ctx, data)
		// Operation might succeed if it's fast enough
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("Got error (may not be context error): %v", err)
		}
	})
}

// TestErrorScenarios_PutBlockWithCid 测试错误情况
func TestErrorScenarios_PutBlockWithCid(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-error-putwithcid")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("invalid_cid_format", func(t *testing.T) {
		testCases := []string{
			"",
			"not-a-cid",
			"Qm",
			"Qm!!!invalid",
			"z",
			"12345",
		}

		for _, invalidCID := range testCases {
			t.Run(invalidCID, func(t *testing.T) {
				data := []byte("test data")
				err := repo.PutBlockWithCid(ctx, invalidCID, data)
				if err == nil {
					t.Errorf("expected error for CID %q", invalidCID)
				}
				if !strings.Contains(err.Error(), "invalid CID") {
					t.Logf("Error for %q: %v", invalidCID, err)
				}
			})
		}
	})

	t.Run("block_too_large", func(t *testing.T) {
		validData := []byte("test")
		cid, err := repo.PutBlock(ctx, validData)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		largeData := make([]byte, maxBlockSize+1)
		err = repo.PutBlockWithCid(ctx, cid.String(), largeData)
		if err == nil {
			t.Fatal("expected error for block exceeding maximum size")
		}
	})
}

// TestErrorScenarios_PutManyBlocks 测试错误情况
func TestErrorScenarios_PutManyBlocks(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-error-putmany")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("one_invalid_block_size", func(t *testing.T) {
		data := [][]byte{
			[]byte("valid 1"),
			make([]byte, maxBlockSize+1), // invalid
			[]byte("valid 2"),
		}

		_, err := repo.PutManyBlocks(ctx, data)
		if err == nil {
			t.Fatal("expected error when one block exceeds limit")
		}

		if !strings.Contains(err.Error(), "index 1") {
			t.Errorf("expected error for index 1, got: %v", err)
		}
		if !strings.Contains(err.Error(), "exceeds maximum") {
			t.Errorf("expected 'exceeds maximum' error, got: %v", err)
		}
	})

	t.Run("context_cancelled_mid_batch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Create a large batch
		data := make([][]byte, 10000)
		for i := range data {
			data[i] = []byte{byte(i)}
		}

		// Cancel after a short delay
		go func() {
			// Give it a moment to start
			// time.Sleep(1 * time.Millisecond)
			cancel()
		}()

		_, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Logf("PutManyBlocks with cancelled context returned error: %v", err)
		}
	})

	t.Run("nil_slice", func(t *testing.T) {
		// Note: Go doesn't allow passing nil to [][]byte directly
		// But we can test with an uninitialized variable
		var data [][]byte
		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Errorf("PutManyBlocks failed for nil slice: %v", err)
		}
		if cids != nil {
			t.Error("expected nil CIDs for nil input")
		}
	})

	t.Run("mixed_empty_and_valid_blocks", func(t *testing.T) {
		data := [][]byte{
			{},
			[]byte("valid"),
			{},
			[]byte("also valid"),
		}

		cids, err := repo.PutManyBlocks(ctx, data)
		if err != nil {
			t.Fatalf("PutManyBlocks failed: %v", err)
		}

		if len(cids) != 4 {
			t.Errorf("expected 4 CIDs, got %d", len(cids))
		}
	})
}

// TestErrorScenarios_HasBlock 测试错误情况
func TestErrorScenarios_HasBlock(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-error-hasblock")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("invalid_cid_formats", func(t *testing.T) {
		invalidCIDs := []string{
			"",
			"invalid",
			"Qm",
			"!!!",
		}

		for _, cid := range invalidCIDs {
			_, err := repo.HasBlock(ctx, cid)
			if err == nil {
				t.Errorf("expected error for CID %q", cid)
			}
			if !strings.Contains(err.Error(), "invalid CID") {
				t.Logf("Error for %q: %v", cid, err)
			}
		}
	})
}

// TestErrorScenarios_HasAllBlocks 测试错误情况
func TestErrorScenarios_HasAllBlocks(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-error-hasall")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("mixed_valid_and_invalid_cids", func(t *testing.T) {
		validData := []byte("test")
		cid, err := repo.PutBlock(ctx, validData)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		cids := []string{
			cid.String(),
			"invalid-cid",
			"QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo",
		}

		_, err = repo.HasAllBlocks(ctx, cids)
		if err == nil {
			t.Error("expected error for invalid CID in list")
		}
	})

	t.Run("all_invalid_cids", func(t *testing.T) {
		cids := []string{
			"invalid-1",
			"invalid-2",
			"invalid-3",
		}

		_, err := repo.HasAllBlocks(ctx, cids)
		if err == nil {
			t.Error("expected error for all invalid CIDs")
		}
	})

	t.Run("context_cancelled_during_check", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Create a large list of CIDs
		cids := make([]string, 1000)
		for i := range cids {
			cids[i] = fmt.Sprintf("QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsX%03d", i)
		}

		// Cancel immediately
		cancel()

		_, err := repo.HasAllBlocks(ctx, cids)
		if err != nil {
			t.Logf("HasAllBlocks with cancelled context returned: %v", err)
		}
	})
}

// TestErrorScenarios_GetRawData 测试错误情况
func TestErrorScenarios_GetRawData(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-error-getraw")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("invalid_cid_formats", func(t *testing.T) {
		invalidCIDs := []string{
			"",
			"not-a-cid",
			"Qm!!!",
		}

		for _, cid := range invalidCIDs {
			_, err := repo.GetRawData(ctx, cid)
			if err == nil {
				t.Errorf("expected error for CID %q", cid)
			}
		}
	})

	t.Run("non_existent_block", func(t *testing.T) {
		nonExistentCID := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"

		_, err := repo.GetRawData(ctx, nonExistentCID)
		if err == nil {
			t.Error("expected error for non-existent block")
		}

		if !strings.Contains(err.Error(), "not found") {
			t.Logf("Error for non-existent block: %v", err)
		}
	})

	t.Run("retry_exhausted", func(t *testing.T) {
		// Use a non-existent CID to trigger retry mechanism
		cid := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"

		_, err := repo.GetRawData(ctx, cid)
		if err == nil {
			t.Error("expected error after retries exhausted")
		}

		// Verify error message mentions retries
		if !strings.Contains(err.Error(), "retries") {
			t.Errorf("expected retry message in error, got: %v", err)
		}
	})

	t.Run("context_cancelled_during_retry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		// Use a non-existent CID to trigger retries
		cid := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"

		_, err := repo.GetRawData(ctx, cid)
		if err == nil {
			t.Error("expected error")
		}

		t.Logf("Error with timeout: %v", err)
	})
}

// TestErrorScenarios_DelBlock 测试错误情况
func TestErrorScenarios_DelBlock(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-error-delblock")
	defer cleanupRepo(t, tmpDir)

	repo, err := NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	t.Run("invalid_cid_formats", func(t *testing.T) {
		invalidCIDs := []string{
			"",
			"invalid",
			"Qm!!!",
		}

		for _, cid := range invalidCIDs {
			err := repo.DelBlock(ctx, cid)
			if err == nil {
				t.Errorf("expected error for CID %q", cid)
			}
		}
	})

	t.Run("delete_non_existent_block", func(t *testing.T) {
		nonExistentCID := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"

		// Deleting non-existent block may or may not error
		err := repo.DelBlock(ctx, nonExistentCID)
		t.Logf("DelBlock for non-existent block returned: %v", err)
	})

	t.Run("context_cancelled_during_delete", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		validData := []byte("test")
		cid, err := repo.PutBlock(context.Background(), validData)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		err = repo.DelBlock(ctx, cid.String())
		if err != nil {
			t.Logf("DelBlock with cancelled context returned: %v", err)
		}
	})
}

// TestErrorScenarios_Close 测试错误情况
func TestErrorScenarios_Close(t *testing.T) {
	t.Run("close_after_destroy", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-error-close-destroy")

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		// Destroy the repository
		err = repo.Destroy()
		if err != nil {
			t.Fatalf("Destroy failed: %v", err)
		}

		// Close should not panic
		err = repo.Close()
		t.Logf("Close after destroy returned: %v", err)
	})

	t.Run("multiple_close_calls", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-error-close-multi")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		// Close multiple times
		err = repo.Close()
		if err != nil {
			t.Errorf("First Close failed: %v", err)
		}

		err = repo.Close()
		t.Logf("Second Close returned: %v", err)

		err = repo.Close()
		t.Logf("Third Close returned: %v", err)
	})
}

// TestErrorScenarios_Destroy 测试错误情况
func TestErrorScenarios_Destroy(t *testing.T) {
	t.Run("destroy_after_close", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-error-destroy-close")

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		err = repo.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		err = repo.Destroy()
		t.Logf("Destroy after close returned: %v", err)
	})

	t.Run("destroy_non_existent_directory", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-error-destroy-nonexistent")

		// Create and immediately destroy a repository
		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		err = repo.Destroy()
		if err != nil {
			t.Fatalf("First Destroy failed: %v", err)
		}

		// Destroy again (on already destroyed repo)
		err = repo.Destroy()
		t.Logf("Second Destroy returned: %v", err)
	})
}

// TestErrorScenarios_StorageFailure 测试存储失败情况
func TestErrorScenarios_StorageFailure(t *testing.T) {
	t.Run("repository_on_read_only_filesystem", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping read-only test in short mode")
		}

		tmpDir := filepath.Join(os.TempDir(), "test-error-readonly")
		defer cleanupRepo(t, tmpDir)

		// Create repository
		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		repo.Close()

		// Make directory read-only
		err = os.Chmod(tmpDir, 0o444)
		if err != nil {
			t.Skip("cannot make directory read-only")
			return
		}
		defer os.Chmod(tmpDir, 0o755)

		// Try to create repository (may fail on read-only FS)
		_, err = NewRepository(tmpDir)
		t.Logf("NewRepository on read-only dir returned: %v", err)
	})
}

// TestErrorScenarios_Usage 测试 Usage 错误情况
func TestErrorScenarios_Usage(t *testing.T) {
	t.Run("usage_after_close", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-error-usage-close")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		repo.Close()

		ctx := context.Background()
		_, err = repo.Usage(ctx)
		t.Logf("Usage after close returned: %v", err)
	})

	t.Run("usage_with_cancelled_context", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "test-error-usage-ctx")
		defer cleanupRepo(t, tmpDir)

		repo, err := NewRepository(tmpDir)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		defer repo.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = repo.Usage(ctx)
		if err != nil {
			t.Logf("Usage with cancelled context returned: %v", err)
		}
	})
}

// TestErrorScenarios_PermissionErrors 测试权限错误
func TestErrorScenarios_PermissionErrors(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping permission tests when running as root")
	}

	t.Run("create_repository_in_protected_directory", func(t *testing.T) {
		protectedDir := filepath.Join(os.TempDir(), "test-error-protected")
		if err := os.MkdirAll(protectedDir, 0o000); err != nil {
			t.Skip("cannot create protected directory")
			return
		}
		defer func() {
			os.Chmod(protectedDir, 0o755)
			os.RemoveAll(protectedDir)
		}()

		repoPath := filepath.Join(protectedDir, "repo")
		_, err := NewRepository(repoPath)
		if err == nil {
			t.Error("expected error when creating repo in protected directory")
		} else {
			t.Logf("Got expected error: %v", err)
		}
	})
}
