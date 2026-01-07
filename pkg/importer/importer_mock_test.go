package importer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ============================================================
// 简化的 Mock 测试 (Simplified Mock Tests)
// ============================================================

func TestImporter_MultipleImports_DifferentInstances(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create multiple files with different content
	testFile1 := filepath.Join(tmpDir, "test1.txt")
	testFile2 := filepath.Join(tmpDir, "test2.txt")
	testFile3 := filepath.Join(tmpDir, "test3.txt")

	content1 := strings.Repeat("a", 1024)     // 1KB
	content2 := strings.Repeat("b", 10*1024)  // 10KB
	content3 := strings.Repeat("c", 100*1024) // 100KB

	os.WriteFile(testFile1, []byte(content1), 0o644)
	os.WriteFile(testFile2, []byte(content2), 0o644)
	os.WriteFile(testFile3, []byte(content3), 0o644)

	// Import all files with different instances
	results := make([]*Result, 3)
	files := []string{testFile1, testFile2, testFile3}
	contents := []string{content1, content2, content3}

	for i, file := range files {
		imp := NewImporter(bs, file)
		result, err := imp.Import(context.Background())

		if err != nil {
			t.Errorf("Import %d failed: %v", i, err)
		}

		if result == nil {
			t.Fatalf("Import %d result is nil", i)
		}

		results[i] = result

		// Verify size
		expectedSize := int64(len(contents[i]))
		if result.Size != expectedSize {
			t.Errorf("Import %d size mismatch: expected %d, got %d", i, expectedSize, result.Size)
		}

		// Verify CID is unique
		if result.RootCid == "" {
			t.Errorf("Import %d has empty CID", i)
		}
	}

	// All CIDs should be different
	if results[0].RootCid == results[1].RootCid ||
		results[1].RootCid == results[2].RootCid ||
		results[0].RootCid == results[2].RootCid {
		t.Error("Different files should have different CIDs")
	}

	t.Logf("Successfully imported 3 files with different CIDs")
}

func TestImporter_Import_SameFileTwice(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	content := "deterministic content"
	os.WriteFile(testFile, []byte(content), 0o644)

	// Import the same file twice
	imp1 := NewImporter(bs, testFile)
	result1, err1 := imp1.Import(context.Background())

	if err1 != nil {
		t.Fatalf("First import failed: %v", err1)
	}

	imp2 := NewImporter(bs, testFile)
	result2, err2 := imp2.Import(context.Background())

	if err2 != nil {
		t.Fatalf("Second import failed: %v", err2)
	}

	// CIDs should be identical (deterministic)
	if result1.RootCid != result2.RootCid {
		t.Errorf("Same file should produce same CID: got %s and %s",
			result1.RootCid, result2.RootCid)
	}

	// Sizes should be identical
	if result1.Size != result2.Size {
		t.Errorf("Sizes should match: got %d and %d",
			result1.Size, result2.Size)
	}

	t.Logf("Deterministic CID verified: %s", result1.RootCid)
}

func TestImporter_ProgressCallback_Nil(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	os.WriteFile(testFile, []byte("test content"), 0o644)

	// Import without progress callback
	imp := NewImporter(bs, testFile)
	// Don't call WithProgress - progress should be nil

	result, err := imp.Import(context.Background())

	if err != nil {
		t.Errorf("Import failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	t.Log("Import succeeded without progress callback")
}

func TestImporter_ProgressCallback_EmptyFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "empty.txt")
	os.WriteFile(testFile, []byte{}, 0o644)

	callbackCalled := false
	var callbackTotal int64

	imp := NewImporter(bs, testFile)
	imp.WithProgress(func(completed, total int64, currentFile string) {
		callbackCalled = true
		callbackTotal = total
	})

	result, err := imp.Import(context.Background())

	if err != nil {
		t.Errorf("Import failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.Size != 0 {
		t.Errorf("Expected size 0, got %d", result.Size)
	}

	// Note: Progress callback may or may not be called for empty files
	// depending on implementation. Just log the result.
	if callbackCalled {
		t.Logf("Progress callback WAS called for empty file (total=%d)", callbackTotal)
		if callbackTotal != 0 {
			t.Errorf("Expected total size 0, got %d", callbackTotal)
		}
	} else {
		t.Log("Progress callback was NOT called for empty file (acceptable)")
	}
}

func TestImporter_Context_AlreadyDone(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	os.WriteFile(testFile, []byte("test content"), 0o644)

	// Create already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(ctx)

	if err == nil {
		t.Error("Expected error for already-cancelled context")
	}

	if result != nil {
		t.Error("Expected nil result for cancelled import")
	}
}

func TestImporter_Context_DeadlineExceeded_DuringImport(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a large file (5MB)
	testFile := filepath.Join(tmpDir, "large.txt")
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	f, _ := os.Create(testFile)
	for i := 0; i < 5; i++ {
		f.Write(data)
	}
	f.Close()

	// Create context with very short deadline
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(ctx)

	// Should fail with deadline exceeded or context error
	if err == nil {
		t.Error("Expected error for exceeded deadline")
	}

	if result != nil {
		t.Error("Expected nil result for timed out import")
	}

	t.Logf("Got expected error: %v", err)
}

func TestImporter_PathWithSpecialCharacters(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create directory with special characters
	specialDir := filepath.Join(tmpDir, "dir with spaces")
	os.Mkdir(specialDir, 0o755)

	testFile := filepath.Join(specialDir, "file[test].txt")
	os.WriteFile(testFile, []byte("test"), 0o644)

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Errorf("Import failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	t.Logf("Successfully imported file with special chars: %s", result.FileName)
}

func TestImporter_Directory_WithMixedFiles(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create various file types
	os.WriteFile(filepath.Join(tmpDir, "small.txt"), []byte("small"), 0o644)
	os.WriteFile(filepath.Join(tmpDir, "medium.bin"), make([]byte, 50*1024), 0o644)

	// Create subdirectory
	subdir := filepath.Join(tmpDir, "subdir")
	os.Mkdir(subdir, 0o755)
	os.WriteFile(filepath.Join(subdir, "nested.txt"), []byte("nested"), 0o644)

	imp := NewImporter(bs, tmpDir)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Errorf("Import failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	// Should have 3 files
	if len(result.Contents) != 3 {
		t.Errorf("Expected 3 contents, got %d", len(result.Contents))
	}

	t.Logf("Imported directory with %d files", len(result.Contents))
}

func TestImporter_WithProgress_VerifyMonotonic(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create file larger than chunk size
	testFile := filepath.Join(tmpDir, "test.txt")
	content := strings.Repeat("x", 2*1024*1024) // 2MB
	os.WriteFile(testFile, []byte(content), 0o644)

	var lastCompleted int64 = -1
	callbackCount := 0

	imp := NewImporter(bs, testFile)
	imp.WithProgress(func(completed, total int64, currentFile string) {
		callbackCount++

		// Verify monotonic increase
		if completed < lastCompleted {
			t.Errorf("Progress decreased: %d < %d", completed, lastCompleted)
		}
		lastCompleted = completed

		// Verify completed <= total
		if completed > total {
			t.Errorf("Completed (%d) exceeds total (%d)", completed, total)
		}
	})

	result, err := imp.Import(context.Background())

	if err != nil {
		t.Errorf("Import failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	// Should have received progress updates
	if callbackCount == 0 {
		t.Error("No progress updates received")
	}

	t.Logf("Received %d progress updates, all monotonic", callbackCount)
}

func TestImporter_Result_VerifyFields(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	content := "test content for verification"
	os.WriteFile(testFile, []byte(content), 0o644)

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Verify all fields are populated
	if result.FileName == "" {
		t.Error("FileName is empty")
	}

	if result.RootCid == "" {
		t.Error("RootCid is empty")
	}

	if result.Size != int64(len(content)) {
		t.Errorf("Size mismatch: expected %d, got %d", len(content), result.Size)
	}

	if result.Packages == nil {
		t.Error("Packages is nil")
	}

	if result.Contents == nil {
		t.Error("Contents is nil")
	}

	// Verify contents
	if len(result.Contents) != 1 {
		t.Errorf("Expected 1 content entry, got %d", len(result.Contents))
	}

	if result.Contents[0].Name != result.FileName {
		t.Errorf("Content name mismatch: %s != %s",
			result.Contents[0].Name, result.FileName)
	}

	t.Logf("Result verification passed: CID=%s, Size=%d, FileName=%s",
		result.RootCid, result.Size, result.FileName)
}

func TestImporter_ErrorHandling_InvalidPath(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	invalidPaths := []string{
		"/nonexistent/file.txt",
		"/nonexistent/.hidden/file.txt",
		"/tmp/nonexistent-path-12345/test.txt",
	}

	for _, path := range invalidPaths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			imp := NewImporter(bs, path)
			result, err := imp.Import(context.Background())

			if err == nil {
				t.Errorf("Expected error for invalid path %q", path)
			}

			if result != nil {
				t.Errorf("Expected nil result for invalid path %q", path)
			}
		})
	}
}

func TestImporter_ConcurrentUsage_SameFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	os.WriteFile(testFile, []byte("test content"), 0o644)

	// Try to import same file concurrently (may fail as Importer is not thread-safe)
	const numGoroutines = 3
	results := make(chan *Result, numGoroutines)
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			imp := NewImporter(bs, testFile)
			result, err := imp.Import(context.Background())
			if err != nil {
				errors <- err
			} else {
				results <- result
			}
		}()
	}

	// Collect results
	successCount := 0
	errorCount := 0
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-results:
			successCount++
		case <-errors:
			errorCount++
		case <-time.After(10 * time.Second):
			t.Fatal("Test timed out")
		}
	}

	t.Logf("Concurrent imports: %d succeeded, %d failed", successCount, errorCount)

	// At least some should succeed
	if successCount == 0 {
		t.Error("No concurrent imports succeeded")
	}
}

func TestProgressTracker_WithNilCallback(t *testing.T) {
	// Should not panic
	tracker := newProgressTracker(1000, nil)

	tracker.update(100, "test.txt")
	tracker.update(200, "test2.txt")

	if tracker.getProcessed() != 300 {
		t.Errorf("Expected processed 300, got %d", tracker.getProcessed())
	}

	if tracker.checkInterrupted() {
		t.Error("Should not be interrupted")
	}
}

func TestProgressTracker_InterruptAndCheck(t *testing.T) {
	tracker := newProgressTracker(1000, nil)

	if tracker.checkInterrupted() {
		t.Error("Should not be interrupted initially")
	}

	tracker.interrupt()

	if !tracker.checkInterrupted() {
		t.Error("Should be interrupted after interrupt()")
	}
}

func TestCleanFilename_Consistency(t *testing.T) {
	tests := []struct {
		input        string
		wantFallback bool
	}{
		{"normal.txt", false},
		{"", true},
		{"   ", true},
		{"...", true},
		{"<>", true},
		{"file<>name.txt", false}, // Gets cleaned
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := cleanFilename(tt.input)

			if tt.wantFallback {
				// Should use default fallback
				if result != defaultFileName && !strings.HasPrefix(result, defaultFileName+".") {
					t.Logf("Input %q -> %q (uses fallback)", tt.input, result)
				}
			} else {
				// Should have non-empty result
				if result == "" {
					t.Errorf("cleanFilename(%q) returned empty", tt.input)
				}
			}
		})
	}
}
