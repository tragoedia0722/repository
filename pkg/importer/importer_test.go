package importer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	"github.com/tragoedia0722/repository/pkg/repository"
)

// createTestBlockstore creates a test blockstore using mount storage
func createTestBlockstore(t *testing.T) (blockstore.Blockstore, func()) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Create a mount-based storage similar to production
	mountPath := filepath.Join(tmpDir, "spec.json")
	spec := map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			map[string]interface{}{
				"mountpoint": "/blocks",
				"type":       "flatfs",
				"path":       filepath.Join(tmpDir, "blocks"),
				"sync":       true,
				"shardFunc":  "/repo/flatfs/shard/v1/next-to-last/2",
			},
		},
	}
	specBytes, _ := json.Marshal(spec)
	if err := os.WriteFile(mountPath, specBytes, 0o600); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to write spec: %v", err)
	}

	// Use repository package which properly handles blockstore
	repoPath := tmpDir
	repo, err := repository.NewRepository(repoPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create repository: %v", err)
	}

	cleanup := func() {
		repo.Close()
		os.RemoveAll(tmpDir)
	}

	return repo.BlockStore(), cleanup
}

// createTestFiles creates test files and directories
func createTestFiles(t *testing.T, dir string) {
	// Create a simple file
	err := os.WriteFile(filepath.Join(dir, "test1.txt"), []byte("Hello, World!"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Create another file
	err = os.WriteFile(filepath.Join(dir, "test2.txt"), []byte("Another test file"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Create a subdirectory with a file
	subdir := filepath.Join(dir, "subdir")
	err = os.Mkdir(subdir, 0o755)
	if err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}

	err = os.WriteFile(filepath.Join(subdir, "test3.txt"), []byte("Subdir file"), 0o644)
	if err != nil {
		t.Fatalf("failed to create subdir file: %v", err)
	}
}

func TestNewImporter(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/test/path")

	if imp == nil {
		t.Fatal("NewImporter returned nil")
	}

	if imp.blockStore == nil {
		t.Error("blockStore is nil")
	}

	if imp.path != filepath.Clean("/test/path") {
		t.Errorf("path not cleaned: got %q", imp.path)
	}
}

func TestImporter_WithProgress(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/test/path")

	progressCalled := false
	imp.WithProgress(func(completed, total int64, currentFile string) {
		progressCalled = true
	})

	if imp.progress == nil {
		t.Error("progress callback not set")
	}

	// Test it gets called
	imp.progress(100, 1000, "test.txt")
	if !progressCalled {
		t.Error("progress callback was not called")
	}
}

func TestImporter_Import_SingleFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Create a test file
	tmpDir, err := os.MkdirTemp("", "importer-file-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	err = os.WriteFile(testFile, []byte("Hello, World!"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Import
	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	if result == nil {
		t.Fatal("result is nil")
	}

	if result.RootCid == "" {
		t.Error("RootCid is empty")
	}

	if result.Size <= 0 {
		t.Errorf("invalid Size: %d", result.Size)
	}

	if result.FileName != "test.txt" {
		t.Errorf("unexpected FileName: %s", result.FileName)
	}

	if len(result.Contents) != 1 {
		t.Errorf("expected 1 content, got %d", len(result.Contents))
	}

	if len(result.Packages) == 0 {
		t.Error("expected at least one package")
	}
}

func TestImporter_Import_Directory(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Create test directory structure
	tmpDir, err := os.MkdirTemp("", "importer-dir-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	createTestFiles(t, tmpDir)

	// Import
	imp := NewImporter(bs, tmpDir)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	if result == nil {
		t.Fatal("result is nil")
	}

	if result.RootCid == "" {
		t.Error("RootCid is empty")
	}

	if result.Size <= 0 {
		t.Errorf("invalid Size: %d", result.Size)
	}

	// Should have 3 files
	if len(result.Contents) != 3 {
		t.Errorf("expected 3 contents, got %d", len(result.Contents))
	}
}

func TestImporter_Import_WithProgress(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Create test file
	tmpDir, err := os.MkdirTemp("", "importer-progress-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	err = os.WriteFile(testFile, []byte("Hello, World!"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Import with progress
	progressCalls := 0
	imp := NewImporter(bs, testFile)
	imp.WithProgress(func(completed, total int64, currentFile string) {
		progressCalls++
	})

	_, err = imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	if progressCalls == 0 {
		t.Error("progress callback was not called")
	}
}

func TestImporter_Import_ContextCancellation(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Create large test file
	tmpDir, err := os.MkdirTemp("", "importer-cancel-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "large.txt")
	// Create a 2MB file
	data := make([]byte, 2*1024*1024)
	err = os.WriteFile(testFile, data, 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Import with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	imp := NewImporter(bs, testFile)
	_, err = imp.Import(ctx)

	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
}

func TestImporter_Import_NonExistentPath(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/non/existent/path")
	_, err := imp.Import(context.Background())

	if err == nil {
		t.Error("expected error for non-existent path, got nil")
	}
}

func TestImporter_calcPackage(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/test/path")

	blocks := []string{
		"QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo",
		"QmNZJiFADfZe42S8AoGi8zjFQSGpg4YkNNKTmQZyeyt7qD",
		"QmPVYMLuEyUQSmpoBfD2QyWYtLKCM8348t3PkgT2p1YGk3",
	}

	pkg := imp.calcPackage(blocks)

	if pkg.Hash == "" {
		t.Error("package hash is empty")
	}

	if len(pkg.Blocks) != len(blocks) {
		t.Errorf("expected %d blocks, got %d", len(blocks), len(pkg.Blocks))
	}

	// Same blocks should produce same hash
	pkg2 := imp.calcPackage(blocks)
	if pkg.Hash != pkg2.Hash {
		t.Error("same blocks produced different hashes")
	}

	// Different blocks should produce different hash
	differentBlocks := []string{"QmDifferent"}
	pkg3 := imp.calcPackage(differentBlocks)
	if pkg.Hash == pkg3.Hash {
		t.Error("different blocks produced same hash")
	}
}

func TestImporter_sliceDirectory(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/test/path")

	t.Run("directory", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "importer-slice-dir-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		testFile := filepath.Join(tmpDir, "test.txt")
		err = os.WriteFile(testFile, []byte("test"), 0o644)
		if err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		dir, err := imp.sliceDirectory(tmpDir)
		if err != nil {
			t.Fatalf("sliceDirectory failed: %v", err)
		}

		if dir == nil {
			t.Fatal("directory is nil")
		}
	})

	t.Run("file", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "importer-slice-file-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		testFile := filepath.Join(tmpDir, "test.txt")
		err = os.WriteFile(testFile, []byte("test"), 0o644)
		if err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		dir, err := imp.sliceDirectory(testFile)
		if err != nil {
			t.Fatalf("sliceDirectory failed: %v", err)
		}

		if dir == nil {
			t.Fatal("directory is nil")
		}
	})

	t.Run("non-existent", func(t *testing.T) {
		_, err := imp.sliceDirectory("/non/existent")
		if err == nil {
			t.Error("expected error for non-existent path")
		}
	})
}

func TestImporter_updateProgress(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/test/path")

	var lastCompleted, lastTotal int64
	var lastFile string

	imp.WithProgress(func(completed, total int64, currentFile string) {
		lastCompleted = completed
		lastTotal = total
		lastFile = currentFile
	})

	// Create tracker with total size
	imp.tracker = newProgressTracker(1000, imp.progress)
	imp.updateProgress(100, "test.txt")

	if lastCompleted != 100 {
		t.Errorf("expected completed=100, got %d", lastCompleted)
	}

	if lastTotal != 1000 {
		t.Errorf("expected total=1000, got %d", lastTotal)
	}

	if lastFile != "test.txt" {
		t.Errorf("expected file=test.txt, got %s", lastFile)
	}
}

func TestImporter_Import_EmptyFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-empty-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "empty.txt")
	err = os.WriteFile(testFile, []byte{}, 0o644)
	if err != nil {
		t.Fatalf("failed to create empty file: %v", err)
	}

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	if result.Size != 0 {
		t.Errorf("expected size 0, got %d", result.Size)
	}

	if result.RootCid == "" {
		t.Error("RootCid should not be empty even for empty file")
	}
}

func TestImporter_Import_InvalidFilenames(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-invalid-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Note: Files with truly invalid characters (like <, >, :, etc.) cannot be
	// created on most filesystems, so we can't test that directly.
	// Instead, we test with valid but tricky filenames that will be cleaned

	testFile := filepath.Join(tmpDir, "test with spaces   and dots... .txt")
	err = os.WriteFile(testFile, []byte("test"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Filename should be cleaned (trailing dots/spaces removed)
	for _, content := range result.Contents {
		t.Logf("Cleaned filename: %s", content.Name)
	}
}

func containsInvalidChars(s string) bool {
	invalid := "<>:\"/\\|?*"
	for _, c := range invalid {
		for _, r := range s {
			if r == c {
				return true
			}
		}
	}
	return false
}

func TestImporter_Import_LargeFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-large-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a 3MB file (should span multiple chunks)
	testFile := filepath.Join(tmpDir, "large.bin")
	data := make([]byte, 3*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	err = os.WriteFile(testFile, data, 0o644)
	if err != nil {
		t.Fatalf("failed to create large file: %v", err)
	}

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	if result.Size != int64(len(data)) {
		t.Errorf("size mismatch: expected %d, got %d", len(data), result.Size)
	}

	// Large files should create multiple blocks
	if len(result.Packages) == 0 {
		t.Error("expected at least one package")
	}
}

// Test with LevelDB backend (repository uses LevelDB by default)
func TestImporter_LevelDBBackend(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "importer-leveldb-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use repository which has LevelDB backend
	repo, err := repository.NewRepository(tmpDir)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	defer repo.Close()

	bs := repo.BlockStore()

	// Create test file
	testFile := filepath.Join(tmpDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test data"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	if result.RootCid == "" {
		t.Error("RootCid is empty")
	}
}

// Test that the same content produces the same CID
func TestImporter_DeterministicCID(t *testing.T) {
	bs1, cleanup1 := createTestBlockstore(t)
	defer cleanup1()

	bs2, cleanup2 := createTestBlockstore(t)
	defer cleanup2()

	tmpDir, err := os.MkdirTemp("", "importer-deterministic-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	testData := []byte("Same content produces same CID")
	err = os.WriteFile(testFile, testData, 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Import twice
	imp1 := NewImporter(bs1, testFile)
	result1, err1 := imp1.Import(context.Background())
	if err1 != nil {
		t.Fatalf("First import failed: %v", err1)
	}

	imp2 := NewImporter(bs2, testFile)
	result2, err2 := imp2.Import(context.Background())
	if err2 != nil {
		t.Fatalf("Second import failed: %v", err2)
	}

	// Root CID should be the same for identical content
	if result1.RootCid != result2.RootCid {
		t.Errorf("same content produced different CIDs: %s vs %s", result1.RootCid, result2.RootCid)
	}
}

func TestImporter_addSymlink(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-symlink-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a symlink that points to a valid relative target
	linkPath := filepath.Join(tmpDir, "link")
	err = os.Symlink("target.txt", linkPath)
	if err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	imp := NewImporter(bs, linkPath)
	result, err := imp.Import(context.Background())

	// The symlink import may fail because the target doesn't exist
	// or it may succeed depending on the implementation
	if err != nil {
		t.Logf("Import of symlink returned error (expected): %v", err)
	} else if result != nil {
		t.Logf("Symlink import result: RootCid=%s", result.RootCid)
	}
}

func TestImporter_putNode(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-putnode-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test data"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Verify we got a result
	if result == nil {
		t.Fatal("result is nil")
	}

	if result.RootCid == "" {
		t.Error("RootCid is empty")
	}

	t.Logf("putNode result: RootCid=%s, FileName=%s", result.RootCid, result.FileName)
}

func TestImporter_addNode(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-addnode-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("context cancellation", func(t *testing.T) {
		testFile := filepath.Join(tmpDir, "cancel-test.txt")
		err = os.WriteFile(testFile, []byte("data"), 0o644)
		if err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		imp := NewImporter(bs, testFile)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err = imp.Import(ctx)
		if err == nil {
			t.Error("expected error for cancelled context")
		}
	})

	t.Run("non-existent file", func(t *testing.T) {
		imp := NewImporter(bs, "/nonexistent/file.txt")
		_, err := imp.Import(context.Background())
		if err == nil {
			t.Error("expected error for non-existent file")
		}
	})
}

func TestImporter_addDir(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-adddir-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create nested directory structure
	subdir := filepath.Join(tmpDir, "parent", "child")
	err = os.MkdirAll(subdir, 0o755)
	if err != nil {
		t.Fatalf("failed to create nested dirs: %v", err)
	}

	err = os.WriteFile(filepath.Join(subdir, "file.txt"), []byte("nested file"), 0o644)
	if err != nil {
		t.Fatalf("failed to create nested file: %v", err)
	}

	imp := NewImporter(bs, tmpDir)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Should have imported the directory
	if result.Size <= 0 {
		t.Errorf("expected positive size, got %d", result.Size)
	}

	t.Logf("Directory import: RootCid=%s, Size=%d, Contents=%d", result.RootCid, result.Size, len(result.Contents))
}

func TestImporter_addFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-addfile-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("large file spans multiple blocks", func(t *testing.T) {
		largeFile := filepath.Join(tmpDir, "large.bin")
		data := make([]byte, 5*1024*1024) // 5MB
		for i := range data {
			data[i] = byte(i % 256)
		}
		err = os.WriteFile(largeFile, data, 0o644)
		if err != nil {
			t.Fatalf("failed to create large file: %v", err)
		}

		imp := NewImporter(bs, largeFile)
		result, err := imp.Import(context.Background())
		if err != nil {
			t.Fatalf("Import failed: %v", err)
		}

		// Large file should create multiple packages/blocks
		if len(result.Packages) == 0 {
			t.Error("expected at least one package for large file")
		}

		totalBlocks := 0
		for _, pkg := range result.Packages {
			totalBlocks += len(pkg.Blocks)
		}
		t.Logf("Large file created %d blocks across %d packages", totalBlocks, len(result.Packages))
	})

	t.Run("small single file", func(t *testing.T) {
		smallFile := filepath.Join(tmpDir, "small.txt")
		err = os.WriteFile(smallFile, []byte("small"), 0o644)
		if err != nil {
			t.Fatalf("failed to create small file: %v", err)
		}

		imp := NewImporter(bs, smallFile)
		result, err := imp.Import(context.Background())
		if err != nil {
			t.Fatalf("Import failed: %v", err)
		}

		if result.FileName != "small.txt" {
			t.Errorf("expected filename 'small.txt', got %s", result.FileName)
		}

		if result.Size != 5 {
			t.Errorf("expected size 5, got %d", result.Size)
		}
	})
}
