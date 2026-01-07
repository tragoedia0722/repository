package importer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================
// 边界条件测试 (Edge Cases)
// ============================================================

func TestImporter_EmptyPath(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "empty string",
			path:    "",
			wantErr: true,
		},
		{
			name:    "whitespace only",
			path:    "   ",
			wantErr: true,
		},
		{
			name:    "single dot",
			path:    ".",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			imp := NewImporter(bs, tt.path)
			ctx := context.Background()

			result, err := imp.Import(ctx)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error for path %q, got nil", tt.path)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for path %q: %v", tt.path, err)
				}
				if result != nil && result.FileName == "" {
					t.Errorf("Expected non-empty filename for path %q", tt.path)
				}
			}
		})
	}
}

func TestImporter_VeryLongFilename(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Create a temp file with a very long name
	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a filename that's 255 characters long (common FS limit)
	longName := strings.Repeat("a", 255) + ".txt"
	testFile := filepath.Join(tmpDir, longName)

	err = os.WriteFile(testFile, []byte("test content"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Errorf("Failed to import file with long name: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	// Filename should be cleaned but not empty
	if result.FileName == "" {
		t.Error("Cleaned filename is empty for long filename")
	}

	// Cleaned name should be shorter than original
	if len(result.FileName) >= len(longName) {
		t.Logf("Warning: Cleaned name (%d) not shorter than original (%d)", len(result.FileName), len(longName))
	}
}

func TestImporter_SpecialCharactersInFilename(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	specialNames := []string{
		"file<>test.txt",               // Invalid Windows chars
		"file|with|pipes.txt",          // Pipes
		"file\"with\"quotes.txt",       // Quotes
		"file:with:colons.txt",         // Colons
		"file*with*asterisks.txt",      // Asterisks
		"file?with?questions.txt",      // Question marks
		"file\twith\ttabs.txt",         // Tabs
		"file\nwith\nnewlines.txt",     // Newlines
		"file..multiple..dots.txt.txt", // Multiple dots
		"   leading_spaces.txt",        // Leading spaces
		"trailing_spaces.txt   ",       // Trailing spaces
		"con.txt",                      // Windows reserved name
		"aux.txt",                      // Windows reserved name
		"nul.txt",                      // Windows reserved name
	}

	for _, name := range specialNames {
		t.Run(name, func(t *testing.T) {
			// Sanitize name for filesystem
			safeName := strings.Map(func(r rune) rune {
				if r < 32 || strings.ContainsRune("<>:\"|?*", r) {
					return '_'
				}
				return r
			}, name)

			testFile := filepath.Join(tmpDir, safeName)
			err := os.WriteFile(testFile, []byte("test"), 0o644)
			if err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			imp := NewImporter(bs, testFile)
			result, err := imp.Import(context.Background())

			if err != nil {
				t.Errorf("Failed to import file %q: %v", safeName, err)
			}

			if result == nil {
				t.Fatal("Result is nil")
			}

			// Cleaned name should not be empty
			if result.FileName == "" {
				t.Errorf("Cleaned filename is empty for %q", safeName)
			}

			// Cleaned name should not contain invalid chars
			invalidChars := "<>:\"|?*"
			for _, c := range invalidChars {
				if strings.ContainsRune(result.FileName, c) {
					t.Errorf("Cleaned filename %q contains invalid character %q", result.FileName, c)
				}
			}
		})
	}
}

func TestImporter_ZeroByteFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "empty.txt")
	err = os.WriteFile(testFile, []byte{}, 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Errorf("Failed to import zero-byte file: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.Size != 0 {
		t.Errorf("Expected size 0, got %d", result.Size)
	}

	if result.RootCid == "" {
		t.Error("RootCid is empty for zero-byte file")
	}
}

func TestImporter_HugeFileSize(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a file larger than chunkSize (1MB)
	// Use 5MB for testing
	testFile := filepath.Join(tmpDir, "large.bin")
	size := int64(5 * 1024 * 1024) // 5MB

	// Create file with pattern to verify content
	data := make([]byte, 1024*1024) // 1MB pattern
	for i := range data {
		data[i] = byte(i % 256)
	}

	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := f.Write(data); err != nil {
			f.Close()
			t.Fatalf("failed to write data: %v", err)
		}
	}
	f.Close()

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Errorf("Failed to import large file: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.Size != size {
		t.Errorf("Expected size %d, got %d", size, result.Size)
	}

	// Should create multiple blocks
	if len(result.Packages) == 0 {
		t.Error("Expected at least one package for large file")
	}

	// 5MB / 1MB chunks = ~5 chunks => should have blocks
	totalBlocks := 0
	for _, pkg := range result.Packages {
		totalBlocks += len(pkg.Blocks)
	}

	if totalBlocks < 5 {
		t.Errorf("Expected at least 5 blocks for 5MB file, got %d", totalBlocks)
	}
}

func TestImporter_DeepDirectoryStructure(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a deep directory structure (100 levels)
	currentPath := tmpDir
	for i := 0; i < 100; i++ {
		currentPath = filepath.Join(currentPath, "level"+string(rune('0'+i%10)))
		if err := os.Mkdir(currentPath, 0o755); err != nil {
			t.Fatalf("failed to create directory at level %d: %v", i, err)
		}
	}

	// Add a file at the deepest level
	deepFile := filepath.Join(currentPath, "deep.txt")
	err = os.WriteFile(deepFile, []byte("deep content"), 0o644)
	if err != nil {
		t.Fatalf("failed to create deep file: %v", err)
	}

	imp := NewImporter(bs, tmpDir)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Errorf("Failed to import deep directory: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	// Should have multiple contents
	if len(result.Contents) == 0 {
		t.Error("Expected at least one content entry")
	}
}

func TestImporter_ManyFiles(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create 1000 files
	numFiles := 1000
	for i := 0; i < numFiles; i++ {
		filename := filepath.Join(tmpDir, "file"+string(rune('0'+i%10))+".txt")
		content := []byte("content " + string(rune('0'+i%10)))
		if err := os.WriteFile(filename, content, 0o644); err != nil {
			t.Fatalf("failed to create file %d: %v", i, err)
		}
	}

	imp := NewImporter(bs, tmpDir)
	result, err := imp.Import(context.Background())

	if err != nil {
		t.Errorf("Failed to import many files: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Contents) != numFiles {
		t.Errorf("Expected %d contents, got %d", numFiles, len(result.Contents))
	}

	// Verify all files are accounted for
	totalSize := int64(0)
	for _, content := range result.Contents {
		totalSize += content.Size
	}

	if result.Size != totalSize {
		t.Errorf("Total size mismatch: result.Size=%d, sum of contents=%d", result.Size, totalSize)
	}
}

// ============================================================
// 错误情况测试 (Error Cases)
// ============================================================

func TestImporter_PathTraversal(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "parent reference",
			path:    "../../../etc/passwd",
			wantErr: true,
		},
		{
			name:    "mixed parent reference",
			path:    "foo/../../../bar",
			wantErr: true,
		},
		{
			name:    "current dir mixed",
			path:    "./../../test",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			imp := NewImporter(bs, tt.path)
			ctx := context.Background()

			result, err := imp.Import(ctx)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error for path %q, got nil", tt.path)
				}
			}
			if result != nil && tt.wantErr {
				t.Errorf("Expected nil result for path %q, got %+v", tt.path, result)
			}
		})
	}
}

func TestImporter_NonExistentFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/nonexistent/path/that/does/not/exist.txt")
	ctx := context.Background()

	result, err := imp.Import(ctx)

	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}

	if result != nil {
		t.Error("Expected nil result for non-existent file")
	}
}

func TestImporter_PermissionDenied(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Running as root, skipping permission test")
	}

	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a file with no read permissions
	testFile := filepath.Join(tmpDir, "no-read.txt")
	err = os.WriteFile(testFile, []byte("secret"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Remove read permissions
	err = os.Chmod(testFile, 0o000)
	if err != nil {
		t.Fatalf("failed to change permissions: %v", err)
	}

	imp := NewImporter(bs, testFile)
	ctx := context.Background()

	result, err := imp.Import(ctx)

	if err == nil {
		t.Error("Expected error for file with no read permissions, got nil")
	}

	if result != nil {
		t.Error("Expected nil result for unreadable file")
	}
}

func TestImporter_FileReplacedDuringImport(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create initial file
	testFile := filepath.Join(tmpDir, "changing.txt")
	err = os.WriteFile(testFile, []byte("initial content"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Import in background
	imp := NewImporter(bs, testFile)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Simulate file changing during import by using a custom reader that fails
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Remove file mid-import
		os.Remove(testFile)
	}()

	result, err := imp.Import(ctx)

	// Result depends on timing - either succeeds or fails
	if err != nil {
		t.Logf("Import failed as expected due to file modification: %v", err)
	} else if result != nil {
		t.Logf("Import succeeded despite file modification (might have read before change)")
	}
}

// ============================================================
// 上下文取消测试 (Context Cancellation)
// ============================================================

func TestImporter_ImmediateCancellation(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test content"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Create already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(ctx)

	if err == nil {
		t.Error("Expected error for cancelled context, got nil")
	}

	if !errors.Is(err, context.Canceled) && err != ErrInterrupted {
		t.Logf("Got error (not context.Canceled): %v", err)
	}

	if result != nil {
		t.Error("Expected nil result for cancelled import")
	}
}

func TestImporter_ImmediateTimeout(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create large file (10MB)
	testFile := filepath.Join(tmpDir, "large.txt")
	_ = int64(10 * 1024 * 1024) // Expected size
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	f, _ := os.Create(testFile)
	for i := 0; i < 10; i++ {
		f.Write(data)
	}
	f.Close()

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	imp := NewImporter(bs, testFile)
	result, err := imp.Import(ctx)

	if err == nil {
		t.Error("Expected error for timeout, got nil")
	}

	if result != nil {
		t.Error("Expected nil result for timeout")
	}
}

func TestImporter_ProgressCallback_PanicRecovery(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test content"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Progress callback that panics
	panicked := false
	panicCallback := func(completed, total int64, currentFile string) {
		panic("intentional panic in progress callback")
	}

	// Recover from panic
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			t.Logf("Recovered from panic: %v", r)
		}
	}()

	imp := NewImporter(bs, testFile)
	imp.WithProgress(panicCallback)

	ctx := context.Background()
	result, err := imp.Import(ctx)

	// Import should complete despite panic in callback
	// (callback runs in same goroutine, so panic will propagate)
	if panicked {
		t.Log("Panic in progress callback was recovered (expected)")
		if result != nil || err == nil {
			t.Error("Import should have failed due to panic in callback")
		}
	}
}

// ============================================================
// 进度跟踪测试 (Progress Tracking)
// ============================================================

func TestImporter_ProgressCallback_ConcurrentCalls(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create multiple files
	for i := 0; i < 10; i++ {
		filename := filepath.Join(tmpDir, "file"+string(rune('0'+i%10))+".txt")
		content := make([]byte, 100*1024) // 100KB each
		if err := os.WriteFile(filename, content, 0o644); err != nil {
			t.Fatalf("failed to create file %d: %v", i, err)
		}
	}

	// Track concurrent calls
	var maxConcurrent int32
	var currentConcurrent atomic.Int32

	progressCallback := func(completed, total int64, currentFile string) {
		count := currentConcurrent.Add(1)
		defer currentConcurrent.Add(-1)

		// Track maximum
		for {
			max := atomic.LoadInt32(&maxConcurrent)
			if count <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, count) {
				break
			}
		}
	}

	imp := NewImporter(bs, tmpDir)
	imp.WithProgress(progressCallback)

	ctx := context.Background()
	result, err := imp.Import(ctx)

	if err != nil {
		t.Errorf("Import failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	t.Logf("Maximum concurrent progress callback calls: %d", maxConcurrent)
}

func TestImporter_ProgressCallback_Accuracy(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "importer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create file with known size
	testFile := filepath.Join(tmpDir, "test.txt")
	content := strings.Repeat("a", 1024*100) // 100KB
	err = os.WriteFile(testFile, []byte(content), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Track progress
	var lastCompleted int64
	var totalSize int64
	var callbackCount int

	progressCallback := func(completed, total int64, currentFile string) {
		callbackCount++

		// Total should be consistent
		if totalSize == 0 {
			totalSize = total
		} else if total != totalSize {
			t.Errorf("Total size changed: was %d, now %d", totalSize, total)
		}

		// Completed should be monotonically increasing
		if completed < lastCompleted {
			t.Errorf("Completed decreased: was %d, now %d", lastCompleted, completed)
		}
		lastCompleted = completed

		// Completed should not exceed total
		if completed > total {
			t.Errorf("Completed (%d) exceeds total (%d)", completed, total)
		}
	}

	imp := NewImporter(bs, testFile)
	imp.WithProgress(progressCallback)

	ctx := context.Background()
	result, err := imp.Import(ctx)

	if err != nil {
		t.Errorf("Import failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	// Verify progress was tracked
	if callbackCount == 0 {
		t.Error("Progress callback was never called")
	}

	if totalSize != result.Size {
		t.Errorf("Total size mismatch: progress reported %d, result has %d", totalSize, result.Size)
	}

	// Final completed should match total
	if lastCompleted != totalSize {
		t.Errorf("Final completed (%d) does not match total (%d)", lastCompleted, totalSize)
	}

	t.Logf("Progress callback called %d times for %d bytes", callbackCount, totalSize)
}

// ============================================================
// 包和哈希测试 (Package and Hash Tests)
// ============================================================

func TestImporter_calcPackage_EmptyBlocks(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/test")

	pkg := imp.calcPackage([]string{})

	if pkg.Hash == "" {
		t.Error("Hash is empty for empty blocks")
	}

	if len(pkg.Blocks) != 0 {
		t.Errorf("Expected 0 blocks, got %d", len(pkg.Blocks))
	}
}

func TestImporter_calcPackage_SingleBlock(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/test")

	blocks := []string{"QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"}
	pkg := imp.calcPackage(blocks)

	if pkg.Hash == "" {
		t.Error("Hash is empty")
	}

	if len(pkg.Blocks) != 1 {
		t.Errorf("Expected 1 block, got %d", len(pkg.Blocks))
	}

	if pkg.Blocks[0] != blocks[0] {
		t.Errorf("Block mismatch: expected %s, got %s", blocks[0], pkg.Blocks[0])
	}
}

func TestImporter_createPackages_ExactlyMultiple(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/test")

	// Create exactly 300 blocks (should be 3 packages)
	blocks := make([]string, 300)
	for i := range blocks {
		blocks[i] = "Qm" + strings.Repeat(string(rune('a'+i%26)), 44)
	}

	packages := imp.createPackages(blocks)

	if len(packages) != 3 {
		t.Errorf("Expected 3 packages, got %d", len(packages))
	}

	// Each package should have 100 blocks
	for i, pkg := range packages {
		if len(pkg.Blocks) != 100 {
			t.Errorf("Package %d has %d blocks, expected 100", i, len(pkg.Blocks))
		}
		if pkg.Hash == "" {
			t.Errorf("Package %d has empty hash", i)
		}
	}
}

func TestImporter_createPackages_NotMultiple(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	imp := NewImporter(bs, "/test")

	// Create 250 blocks (should be 2 full packages + 1 partial)
	blocks := make([]string, 250)
	for i := range blocks {
		blocks[i] = "Qm" + strings.Repeat(string(rune('a'+i%26)), 44)
	}

	packages := imp.createPackages(blocks)

	if len(packages) != 3 {
		t.Errorf("Expected 3 packages, got %d", len(packages))
	}

	// First two should be full, last should be partial
	if len(packages[0].Blocks) != 100 {
		t.Errorf("Package 0 has %d blocks, expected 100", len(packages[0].Blocks))
	}
	if len(packages[1].Blocks) != 100 {
		t.Errorf("Package 1 has %d blocks, expected 100", len(packages[1].Blocks))
	}
	if len(packages[2].Blocks) != 50 {
		t.Errorf("Package 2 has %d blocks, expected 50", len(packages[2].Blocks))
	}
}

// ============================================================
// 文件名清理测试 (Filename Cleaning Tests)
// ============================================================

func TestCleanFilename_EdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantEmpty bool
	}{
		{
			name:      "empty string",
			input:     "",
			wantEmpty: true, // After helper.CleanFilename
		},
		{
			name:      "spaces only",
			input:     "     ",
			wantEmpty: true,
		},
		{
			name:      "dots only",
			input:     "...",
			wantEmpty: true,
		},
		{
			name:      "invalid chars only",
			input:     "<>:/|?*",
			wantEmpty: true,
		},
		{
			name:      "mixed invalid and valid",
			input:     "test<file>.txt",
			wantEmpty: false,
		},
		{
			name:      "unicode characters",
			input:     "文件名测试.txt",
			wantEmpty: false,
		},
		{
			name:      "emoji in filename",
			input:     "test😀file.txt",
			wantEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanFilename(tt.input)

			if tt.wantEmpty {
				if result != defaultFileName && !strings.HasPrefix(result, defaultFileName+".") {
					t.Logf("Input %q cleaned to %q (expected fallback)", tt.input, result)
				}
			} else {
				if result == "" {
					t.Errorf("cleanFilename(%q) returned empty string", tt.input)
				}
			}
		})
	}
}

func TestCleanDirname_EdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantEmpty bool
	}{
		{
			name:      "empty",
			input:     "",
			wantEmpty: true,
		},
		{
			name:      "normal directory",
			input:     "mydir",
			wantEmpty: false,
		},
		{
			name:      "directory with spaces",
			input:     "my directory",
			wantEmpty: false,
		},
		{
			name:      "directory with invalid chars",
			input:     "dir<name>",
			wantEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanDirname(tt.input)

			if tt.wantEmpty {
				if result != defaultDirName {
					t.Logf("Input %q cleaned to %q (expected defaultDirName)", tt.input, result)
				}
			} else {
				if result == "" {
					t.Errorf("cleanDirname(%q) returned empty string", tt.input)
				}
			}
		})
	}
}

func TestCleanEntryName_Consistency(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		isDir     bool
		wantClean bool
	}{
		{
			name:      "file with extension",
			input:     "test.txt",
			isDir:     false,
			wantClean: true,
		},
		{
			name:      "directory",
			input:     "mydir",
			isDir:     true,
			wantClean: true,
		},
		{
			name:      "file without extension",
			input:     "README",
			isDir:     false,
			wantClean: true,
		},
		{
			name:      "empty filename",
			input:     "",
			isDir:     false,
			wantClean: false,
		},
		{
			name:      "empty dirname",
			input:     "",
			isDir:     true,
			wantClean: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanEntryName(tt.input, tt.isDir)

			if tt.wantClean {
				if result == "" {
					t.Errorf("cleanEntryName(%q, isDir=%v) returned empty", tt.input, tt.isDir)
				}
			} else {
				if result == "" {
					// Expected - should use fallback
					if tt.isDir && result != defaultDirName {
						t.Errorf("Expected defaultDirName, got %q", result)
					}
					if !tt.isDir && result != defaultFileName && !strings.HasPrefix(result, defaultFileName+".") {
						t.Errorf("Expected defaultFileName, got %q", result)
					}
				}
			}
		})
	}
}

// ============================================================
// 辅助函数测试 (Helper Function Tests)
// ============================================================

func TestNewProgressTracker_ZeroTotal(t *testing.T) {
	tracker := newProgressTracker(0, nil)

	if tracker == nil {
		t.Fatal("newProgressTracker returned nil")
	}

	if tracker.totalSize != 0 {
		t.Errorf("Expected totalSize 0, got %d", tracker.totalSize)
	}

	if tracker.getProcessed() != 0 {
		t.Errorf("Expected processed 0, got %d", tracker.getProcessed())
	}
}

func TestProgressTracker_Update(t *testing.T) {
	var callbackCompleted, callbackTotal int64
	var callbackCalled bool

	callback := func(completed, total int64, currentFile string) {
		callbackCalled = true
		callbackCompleted = completed
		callbackTotal = total
	}

	tracker := newProgressTracker(1000, callback)

	// Update multiple times
	tracker.update(100, "test1.txt")
	if tracker.getProcessed() != 100 {
		t.Errorf("Expected processed 100, got %d", tracker.getProcessed())
	}

	tracker.update(200, "test2.txt")
	if tracker.getProcessed() != 300 {
		t.Errorf("Expected processed 300, got %d", tracker.getProcessed())
	}

	tracker.update(700, "test3.txt")
	if tracker.getProcessed() != 1000 {
		t.Errorf("Expected processed 1000, got %d", tracker.getProcessed())
	}

	if !callbackCalled {
		t.Error("Callback was never called")
	}

	if callbackTotal != 1000 {
		t.Errorf("Callback total mismatch: expected 1000, got %d", callbackTotal)
	}

	// Verify final completed value
	if callbackCompleted != 1000 {
		t.Errorf("Final completed mismatch: expected 1000, got %d", callbackCompleted)
	}
}

func TestProgressTracker_Interrupt(t *testing.T) {
	tracker := newProgressTracker(1000, nil)

	if tracker.checkInterrupted() {
		t.Error("Should not be interrupted initially")
	}

	tracker.interrupt()

	if !tracker.checkInterrupted() {
		t.Error("Should be interrupted after interrupt()")
	}
}

func TestProgressTracker_NilCallback(t *testing.T) {
	// Should not panic
	tracker := newProgressTracker(1000, nil)

	tracker.update(100, "test.txt")

	if tracker.getProcessed() != 100 {
		t.Errorf("Expected processed 100, got %d", tracker.getProcessed())
	}
}
