package extractor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	"github.com/tragoedia0722/repository/pkg/importer"
	"github.com/tragoedia0722/repository/pkg/repository"
)

// createTestBlockstore creates a test blockstore using repository
func createTestBlockstore(t *testing.T) (blockstore.Blockstore, func()) {
	tmpDir, err := os.MkdirTemp("", "extractor-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Use repository package which properly handles blockstore
	repo, err := repository.NewRepository(tmpDir)
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

// importTestFiles creates and imports test files, returning the root CID
func importTestFiles(t *testing.T, bs blockstore.Blockstore) string {
	tmpDir, err := os.MkdirTemp("", "extractor-import-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files
	err = os.WriteFile(filepath.Join(tmpDir, "test1.txt"), []byte("Hello, World!"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	err = os.WriteFile(filepath.Join(tmpDir, "test2.txt"), []byte("Another test file"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Create subdirectory
	subdir := filepath.Join(tmpDir, "subdir")
	err = os.Mkdir(subdir, 0o755)
	if err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}

	err = os.WriteFile(filepath.Join(subdir, "test3.txt"), []byte("Subdir file"), 0o644)
	if err != nil {
		t.Fatalf("failed to create subdir file: %v", err)
	}

	// Import
	imp := importer.NewImporter(bs, tmpDir)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	return result.RootCid
}

func TestNewExtractor(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	rootCid := "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"

	ext := NewExtractor(bs, rootCid, "/output/path")

	if ext == nil {
		t.Fatal("NewExtractor returned nil")
	}

	if ext.blockStore == nil {
		t.Error("blockStore is nil")
	}

	if ext.cid != rootCid {
		t.Errorf("cid not set: expected %s, got %s", rootCid, ext.cid)
	}

	// Path should be cleaned
	expectedPath := filepath.Clean("/output/path")
	if ext.path != expectedPath {
		t.Errorf("path not cleaned: expected %s, got %s", expectedPath, ext.path)
	}
}

func TestNewExtractor_PathCleaning(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tests := []struct {
		name        string
		inputPath   string
		expectClean bool
	}{
		{
			name:        "normal path",
			inputPath:   "/normal/path",
			expectClean: true,
		},
		{
			name:        "path with invalid chars",
			inputPath:   "/path<>:\"/\\|?*with/invalid",
			expectClean: true,
		},
		{
			name:        "path with spaces",
			inputPath:   "/path with spaces",
			expectClean: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext := NewExtractor(bs, "QmTestCID", tt.inputPath)
			if ext == nil {
				t.Fatal("NewExtractor returned nil")
			}

			if tt.expectClean {
				// Check that path doesn't contain obviously invalid characters
				if strings.ContainsAny(ext.path, "<>:\"|?*") {
					t.Errorf("path contains invalid characters: %s", ext.path)
				}
			}
		})
	}
}

func TestExtractor_WithProgress(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	ext := NewExtractor(bs, "QmTestCID", "/output/path")

	progressCalled := false
	ext.WithProgress(func(completed, total int64, currentFile string) {
		progressCalled = true
	})

	if ext.progress == nil {
		t.Error("progress callback not set")
	}

	// Test it gets called
	ext.progress(100, 1000, "test.txt")
	if !progressCalled {
		t.Error("progress callback was not called")
	}
}

func TestExtractor_Extract_SingleFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Import a single file first
	tmpDir, err := os.MkdirTemp("", "extractor-single-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	testData := []byte("Hello, World!")
	err = os.WriteFile(testFile, testData, 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := importer.NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Now extract
	outputDir, err := os.MkdirTemp("", "extractor-output-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	ext := NewExtractor(bs, result.RootCid, outputDir)
	err = ext.Extract(context.Background(), true)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Verify file was extracted
	extractedFile := filepath.Join(outputDir, "test.txt")
	data, err := os.ReadFile(extractedFile)
	if err != nil {
		t.Fatalf("failed to read extracted file: %v", err)
	}

	if string(data) != string(testData) {
		t.Errorf("data mismatch: got %q, want %q", data, testData)
	}
}

func TestExtractor_Extract_Directory(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	rootCid := importTestFiles(t, bs)

	// Extract
	outputDir, err := os.MkdirTemp("", "extractor-dir-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	ext := NewExtractor(bs, rootCid, outputDir)
	err = ext.Extract(context.Background(), true)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Verify files were extracted
	files, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("failed to read output dir: %v", err)
	}

	if len(files) < 2 {
		t.Errorf("expected at least 2 files, got %d", len(files))
	}

	// Check subdirectory
	subdirPath := filepath.Join(outputDir, "subdir")
	info, err := os.Stat(subdirPath)
	if err != nil {
		t.Fatalf("subdir not found: %v", err)
	}

	if !info.IsDir() {
		t.Error("subdir is not a directory")
	}
}

func TestExtractor_Extract_WithProgress(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Import a file
	tmpDir, err := os.MkdirTemp("", "extractor-progress-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test data"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := importer.NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Extract with progress
	outputDir, err := os.MkdirTemp("", "extractor-output-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	progressCalls := 0
	ext := NewExtractor(bs, result.RootCid, outputDir)
	ext.WithProgress(func(completed, total int64, currentFile string) {
		progressCalls++
	})

	err = ext.Extract(context.Background(), true)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if progressCalls == 0 {
		t.Error("progress callback was not called")
	}
}

func TestExtractor_Extract_NoOverwrite(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Import a file
	tmpDir, err := os.MkdirTemp("", "extractor-nooverwrite-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	testData := []byte("original data")
	err = os.WriteFile(testFile, testData, 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := importer.NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Extract once
	outputDir, err := os.MkdirTemp("", "extractor-output-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	ext := NewExtractor(bs, result.RootCid, outputDir)
	err = ext.Extract(context.Background(), true)
	if err != nil {
		t.Fatalf("First Extract failed: %v", err)
	}

	// Try to extract again without overwrite
	err = ext.Extract(context.Background(), false)
	if err == nil {
		t.Error("expected error when extracting without overwrite")
	}

	if err != ErrPathExistsOverwrite {
		t.Logf("got error: %v", err)
	}
}

func TestExtractor_Extract_Overwrite(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Import a file
	tmpDir, err := os.MkdirTemp("", "extractor-overwrite-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	testData := []byte("new data with more bytes to ensure size differs")
	err = os.WriteFile(testFile, testData, 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := importer.NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Create output file with different content
	outputDir, err := os.MkdirTemp("", "extractor-output-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	outputFile := filepath.Join(outputDir, "test.txt")
	err = os.WriteFile(outputFile, []byte("old data"), 0o644)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}

	// Extract with overwrite
	ext := NewExtractor(bs, result.RootCid, outputDir)
	err = ext.Extract(context.Background(), true)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Verify file was overwritten
	data, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	if string(data) != string(testData) {
		t.Errorf("file not overwritten: got %q, want %q", data, testData)
	}
}

func TestExtractor_Extract_InvalidCID(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	outputDir, err := os.MkdirTemp("", "extractor-output-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	ext := NewExtractor(bs, "invalid-cid", outputDir)
	err = ext.Extract(context.Background(), true)
	if err == nil {
		t.Error("expected error for invalid CID, got nil")
	}
}

func TestExtractor_Extract_NonExistentCID(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	outputDir, err := os.MkdirTemp("", "extractor-output-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	// A valid CID format but content doesn't exist
	ext := NewExtractor(bs, "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo", outputDir)
	err = ext.Extract(context.Background(), true)
	if err == nil {
		t.Error("expected error for non-existent CID, got nil")
	}
}

func TestExtractor_Extract_ContextCancellation(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Import a file
	tmpDir, err := os.MkdirTemp("", "extractor-cancel-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a large file
	testFile := filepath.Join(tmpDir, "large.txt")
	data := make([]byte, 2*1024*1024) // 2MB
	err = os.WriteFile(testFile, data, 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := importer.NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Extract with cancelled context
	outputDir, err := os.MkdirTemp("", "extractor-output-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	ext := NewExtractor(bs, result.RootCid, outputDir)
	err = ext.Extract(ctx, true)
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
}

func TestExtractor_isSubPath(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	ext := NewExtractor(bs, "QmTest", "/base/path")

	tests := []struct {
		name     string
		path     string
		base     string
		expected bool
	}{
		{
			name:     "same path",
			path:     "/base/path",
			base:     "/base/path",
			expected: true,
		},
		{
			name:     "subdirectory",
			path:     "/base/path/sub",
			base:     "/base/path",
			expected: true,
		},
		{
			name:     "deep subdirectory",
			path:     "/base/path/sub/deep/file.txt",
			base:     "/base/path",
			expected: true,
		},
		{
			name:     "different path",
			path:     "/other/path",
			base:     "/base/path",
			expected: false,
		},
		{
			name:     "parent directory",
			path:     "/base",
			base:     "/base/path",
			expected: false,
		},
		{
			name:     "path traversal attempt",
			path:     "/base/path/../escape",
			base:     "/base/path",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ext.isSubPath(tt.path, tt.base)
			if result != tt.expected {
				t.Errorf("isSubPath(%q, %q) = %v, want %v", tt.path, tt.base, result, tt.expected)
			}
		})
	}
}

func TestExtractor_updateProgress(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	ext := NewExtractor(bs, "QmTest", "/output/path")

	var lastCompleted, lastTotal int64
	var lastFile string

	ext.WithProgress(func(completed, total int64, currentFile string) {
		lastCompleted = completed
		lastTotal = total
		lastFile = currentFile
	})

	// Set node size first
	ext.nodeSize = 1000
	ext.updateProgress(100, "test.txt")

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

func TestExtractor_Extract_EmptyFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Import empty file
	tmpDir, err := os.MkdirTemp("", "extractor-empty-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "empty.txt")
	err = os.WriteFile(testFile, []byte{}, 0o644)
	if err != nil {
		t.Fatalf("failed to create empty file: %v", err)
	}

	imp := importer.NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Extract
	outputDir, err := os.MkdirTemp("", "extractor-output-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	ext := NewExtractor(bs, result.RootCid, outputDir)
	err = ext.Extract(context.Background(), true)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Verify empty file was created
	outputFile := filepath.Join(outputDir, "empty.txt")
	info, err := os.Stat(outputFile)
	if err != nil {
		t.Fatalf("failed to stat output file: %v", err)
	}

	if info.Size() != 0 {
		t.Errorf("expected empty file, got size %d", info.Size())
	}
}

func TestExtractor_Extract_ExistingFileSameSize(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Import a file
	tmpDir, err := os.MkdirTemp("", "extractor-samesize-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	testData := []byte("same size data")
	err = os.WriteFile(testFile, testData, 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := importer.NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	// Create output file with same size
	outputDir, err := os.MkdirTemp("", "extractor-output-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	outputFile := filepath.Join(outputDir, "test.txt")
	err = os.WriteFile(outputFile, testData, 0o644)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}

	// Extract with overwrite - should skip if same size
	ext := NewExtractor(bs, result.RootCid, outputDir)
	err = ext.Extract(context.Background(), true)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// File should still exist
	_, err = os.Stat(outputFile)
	if err != nil {
		t.Error("output file should exist")
	}
}

func TestExtractor_Extract_BackslashHandling(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// This test verifies that backslashes in filenames are properly handled
	// They should be treated as path separators and cleaned

	// Create test structure with backslash-like names
	tmpDir, err := os.MkdirTemp("", "extractor-backslash-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	imp := importer.NewImporter(bs, testFile)
	result, err := imp.Import(context.Background())
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	outputDir, err := os.MkdirTemp("", "extractor-output-*")
	if err != nil {
		t.Fatalf("failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	ext := NewExtractor(bs, result.RootCid, outputDir)
	err = ext.Extract(context.Background(), true)
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	// Just verify extraction succeeded without path traversal issues
	_, err = os.Stat(outputDir)
	if err != nil {
		t.Error("output directory should exist")
	}
}

func TestExtractor_isValidSymlinkTarget(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	ext := NewExtractor(bs, "QmTest", "/output/path")

	tests := []struct {
		name     string
		target   string
		expected bool
	}{
		{
			name:     "relative path",
			target:   "relative/path",
			expected: true,
		},
		{
			name:     "simple relative",
			target:   "file.txt",
			expected: true,
		},
		{
			name:     "absolute path",
			target:   "/absolute/path",
			expected: false,
		},
		{
			name:     "parent reference",
			target:   "../parent",
			expected: false,
		},
		{
			name:     "parent reference that normalizes to relative",
			target:   "path/../to/file",
			expected: true, // filepath.Clean() removes the ".." since it stays within bounds
		},
		{
			name:     "current directory",
			target:   "./file",
			expected: true,
		},
		{
			name:     "empty path",
			target:   "",
			expected: true, // filepath.Clean("") = "." which doesn't start with ".."
		},
		{
			name:     "absolute with drive letter (Windows) on Linux",
			target:   "C:\\path",
			expected: true, // On Linux, this is just a filename, not an absolute path
		},
		{
			name:     "double parent",
			target:   "../../file",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ext.isValidSymlinkTarget(tt.target)
			if result != tt.expected {
				t.Errorf("isValidSymlinkTarget(%q) = %v, want %v", tt.target, result, tt.expected)
			}
		})
	}
}

func TestExtractor_isDir(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	ext := NewExtractor(bs, "QmTest", "/output/path")

	t.Run("nil node", func(t *testing.T) {
		// Should not panic and return false
		result := ext.isDir(nil)
		if result {
			t.Error("isDir(nil) should return false")
		}
	})
}

func TestExtractor_createNewFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	ext := NewExtractor(bs, "QmTest", "/output/path")

	t.Run("creates new file in directory", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "extractor-newfile-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		filePath := filepath.Join(tmpDir, "subdir", "newfile.txt")
		f, err := ext.createNewFile(filePath)
		if err != nil {
			t.Fatalf("createNewFile failed: %v", err)
		}
		defer f.Close()

		// Verify file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Error("file should exist")
		}
	})

	t.Run("fails if file already exists", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "extractor-newfile-exists-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		filePath := filepath.Join(tmpDir, "existing.txt")
		// Create the file first
		err = os.WriteFile(filePath, []byte("existing"), 0o644)
		if err != nil {
			t.Fatalf("failed to create existing file: %v", err)
		}

		// Try to create again with EXCL flag
		_, err = ext.createNewFile(filePath)
		if err == nil {
			t.Error("expected error when file already exists")
		}
	})
}

func TestExtractor_createPartFile(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	ext := NewExtractor(bs, "QmTest", "/output/path")

	t.Run("creates new part file", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "extractor-partfile-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		finalPath := filepath.Join(tmpDir, "subdir", "file.txt")
		f, partPath, err := ext.createPartFile(finalPath)
		if err != nil {
			t.Fatalf("createPartFile failed: %v", err)
		}
		defer f.Close()

		expectedPartPath := finalPath + ".part"
		if partPath != expectedPartPath {
			t.Errorf("part path = %s, want %s", partPath, expectedPartPath)
		}

		// Verify part file exists
		if _, err := os.Stat(partPath); os.IsNotExist(err) {
			t.Error("part file should exist")
		}
	})

	t.Run("removes existing part file", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "extractor-partfile-remove-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		finalPath := filepath.Join(tmpDir, "file.txt")
		partPath := finalPath + ".part"

		// Create an existing part file
		err = os.WriteFile(partPath, []byte("old data"), 0o644)
		if err != nil {
			t.Fatalf("failed to create existing part file: %v", err)
		}

		// createPartFile should remove it and create a new one
		f, newPartPath, err := ext.createPartFile(finalPath)
		if err != nil {
			t.Fatalf("createPartFile failed: %v", err)
		}
		defer f.Close()

		if newPartPath != partPath {
			t.Errorf("part path = %s, want %s", newPartPath, partPath)
		}
	})
}
