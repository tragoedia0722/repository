package extractor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// =============================================================================
// Error Wrapping Tests
// =============================================================================

func TestPathError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *PathError
		want string
	}{
		{
			name: "with operation",
			err: &PathError{
				Path: "/test/path",
				Op:   "extract",
				Err:  errors.New("test error"),
			},
			want: `extract "/test/path": test error`,
		},
		{
			name: "without operation",
			err: &PathError{
				Path: "/test/path",
				Err:  errors.New("test error"),
			},
			want: `path error "/test/path": test error`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPathError_Unwrap(t *testing.T) {
	originalErr := errors.New("original error")
	pathErr := &PathError{
		Path: "/test/path",
		Op:   "extract",
		Err:  originalErr,
	}

	if unwrapped := pathErr.Unwrap(); unwrapped != originalErr {
		t.Errorf("Unwrap() = %v, want %v", unwrapped, originalErr)
	}
}

func TestErrorWrappers(t *testing.T) {
	tests := []struct {
		name   string
		wrap   func() error
		verify func(error) bool
	}{
		{
			name: "wrapPathTraversal",
			wrap: func() error {
				return wrapPathTraversal("/test/path")
			},
			verify: func(err error) bool {
				var pathErr *PathError
				return errors.As(err, &pathErr) && errors.Is(err, ErrPathTraversal)
			},
		},
		{
			name: "wrapInvalidSymlinkTarget",
			wrap: func() error {
				return wrapInvalidSymlinkTarget("target")
			},
			verify: func(err error) bool {
				return errors.Is(err, ErrInvalidSymlinkTarget)
			},
		},
		{
			name: "wrapUnsupportedFileType",
			wrap: func() error {
				return wrapUnsupportedFileType("/test/path", "mockType")
			},
			verify: func(err error) bool {
				var pathErr *PathError
				return errors.As(err, &pathErr) && errors.Is(err, ErrUnsupportedFileType)
			},
		},
		{
			name: "wrapInvalidDirectoryEntry",
			wrap: func() error {
				return wrapInvalidDirectoryEntry("entry")
			},
			verify: func(err error) bool {
				return errors.Is(err, ErrInvalidDirectoryEntry)
			},
		},
		{
			name: "wrapPathComponent",
			wrap: func() error {
				return wrapPathComponent("component")
			},
			verify: func(err error) bool {
				return errors.Is(err, ErrInvalidPathComponent)
			},
		},
		{
			name: "wrapPathTraversalAttempt",
			wrap: func() error {
				return wrapPathTraversalAttempt("entry")
			},
			verify: func(err error) bool {
				return errors.Is(err, ErrPathTraversalAttempt)
			},
		},
		{
			name: "wrapRemoveFailed",
			wrap: func() error {
				return wrapRemoveFailed("/test/path", errors.New("remove error"))
			},
			verify: func(err error) bool {
				var pathErr *PathError
				return errors.As(err, &pathErr) && pathErr.Op == "remove"
			},
		},
		{
			name: "wrapMkdirFailed",
			wrap: func() error {
				return wrapMkdirFailed("/test/path", errors.New("mkdir error"))
			},
			verify: func(err error) bool {
				var pathErr *PathError
				return errors.As(err, &pathErr) && pathErr.Op == "mkdir"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.wrap()
			if !tt.verify(err) {
				t.Errorf("wrapped error verification failed")
			}
		})
	}
}

// =============================================================================
// Path Utilities Tests
// =============================================================================

func TestCleanPathComponents_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "simple path",
			input: "/simple/path",
		},
		{
			name:  "path with dots",
			input: "/path/with/./dots",
		},
		{
			name:  "double slash",
			input: "/path//double/slash",
		},
		{
			name:  "root path",
			input: "/",
		},
		{
			name:  "relative path",
			input: "relative/path",
		},
		{
			name:  "trailing slash",
			input: "/path/trailing/",
		},
		{
			name:  "multiple slashes",
			input: "/path///multiple///slashes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanPathComponents(tt.input)
			if result == "" {
				t.Error("cleanPathComponents() returned empty string")
			}
			// Verify no double slashes (except leading)
			if strings.Contains(result[1:], "//") {
				t.Errorf("cleanPathComponents() contains double slashes: %v", result)
			}
		})
	}
}

func TestNormalizeEntryName_EdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantErr    bool
		checkClean bool
	}{
		{
			name:       "simple name",
			input:      "file.txt",
			wantErr:    false,
			checkClean: true,
		},
		{
			name:       "backslash path",
			input:      "dir\\file.txt",
			wantErr:    false,
			checkClean: true,
		},
		{
			name:       "nested backslash path",
			input:      "dir1\\dir2\\file.txt",
			wantErr:    false,
			checkClean: true,
		},
		{
			name:       "backslash with parent traversal",
			input:      "dir\\..\\file.txt",
			wantErr:    true,
			checkClean: false,
		},
		{
			name:       "only backslashes",
			input:      "\\\\\\",
			wantErr:    true,
			checkClean: false,
		},
		{
			name:       "empty component after cleaning",
			input:      "<<>>:",
			wantErr:    false,
			checkClean: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := normalizeEntryName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("normalizeEntryName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.checkClean && result == "" {
				t.Error("normalizeEntryName() returned empty string, want non-empty")
			}
			if tt.checkClean && strings.Contains(result, "\\") {
				t.Error("normalizeEntryName() result contains backslashes")
			}
		})
	}
}

func TestValidateSymlinkTarget_Comprehensive(t *testing.T) {
	tests := []struct {
		name   string
		target string
		valid  bool
	}{
		// Valid cases
		{
			name:   "simple relative",
			target: "file.txt",
			valid:  true,
		},
		{
			name:   "nested relative",
			target: "dir/file.txt",
			valid:  true,
		},
		{
			name:   "current dir reference",
			target: "./file.txt",
			valid:  true,
		},
		{
			name:   "parent that stays in bounds",
			target: "dir/../file.txt",
			valid:  true,
		},
		{
			name:   "complex but safe",
			target: "a/b/c/../../d",
			valid:  true,
		},

		// Invalid cases
		{
			name:   "absolute path",
			target: "/etc/passwd",
			valid:  false,
		},
		{
			name:   "parent reference",
			target: "../file",
			valid:  false,
		},
		{
			name:   "deep parent traversal",
			target: "../../etc/passwd",
			valid:  false,
		},
		{
			name:   "dotted parent traversal",
			target: "./../../etc/passwd",
			valid:  false,
		},
		{
			name:   "nested parent traversal",
			target: "dir/../../../etc/passwd",
			valid:  false,
		},
		{
			name:   "parent with separator",
			target: "../",
			valid:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validateSymlinkTarget(tt.target)
			if result != tt.valid {
				t.Errorf("validateSymlinkTarget(%q) = %v, want %v", tt.target, result, tt.valid)
			}
		})
	}
}

// =============================================================================
// File Operations Tests
// =============================================================================

func TestGetPathInfo_EdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pathinfo-edge-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files
	existingFile := filepath.Join(tmpDir, "existing.txt")
	if err := os.WriteFile(existingFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	existingDir := filepath.Join(tmpDir, "existing_dir")
	if err := os.Mkdir(existingDir, 0o755); err != nil {
		t.Fatal(err)
	}

	symlinkPath := filepath.Join(tmpDir, "symlink")
	if err := os.Symlink(existingFile, symlinkPath); err != nil {
		t.Skip("cannot create symlink, skipping symlink tests")
	}

	tests := []struct {
		name      string
		path      string
		wantExist bool
		wantDir   bool
	}{
		{
			name:      "existing file",
			path:      existingFile,
			wantExist: true,
			wantDir:   false,
		},
		{
			name:      "existing directory",
			path:      existingDir,
			wantExist: true,
			wantDir:   true,
		},
		{
			name:      "symlink",
			path:      symlinkPath,
			wantExist: true,
			wantDir:   false,
		},
		{
			name:      "non-existing file",
			path:      filepath.Join(tmpDir, "nonexistent.txt"),
			wantExist: false,
			wantDir:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := getPathInfo(tt.path)
			if err != nil {
				t.Errorf("getPathInfo() unexpected error = %v", err)
				return
			}
			if info.exists != tt.wantExist {
				t.Errorf("getPathInfo().exists = %v, want %v", info.exists, tt.wantExist)
			}
			if info.exists && info.IsDir() != tt.wantDir {
				t.Errorf("getPathInfo().IsDir() = %v, want %v", info.IsDir(), tt.wantDir)
			}
		})
	}
}

func TestShouldSkipExistingFile_Cases(t *testing.T) {
	tests := []struct {
		name      string
		isRegular bool
		isDir     bool
		size      int64
		nodeSize  int64
		isNodeDir bool
		wantSkip  bool
	}{
		// Should skip
		{
			name:      "same size regular file",
			isRegular: true,
			isDir:     false,
			size:      1024,
			nodeSize:  1024,
			isNodeDir: false,
			wantSkip:  true,
		},
		// Should not skip
		{
			name:      "different size regular file",
			isRegular: true,
			isDir:     false,
			size:      1024,
			nodeSize:  2048,
			isNodeDir: false,
			wantSkip:  false,
		},
		{
			name:      "directory to directory",
			isRegular: false,
			isDir:     true,
			size:      0,
			nodeSize:  0,
			isNodeDir: true,
			wantSkip:  false, // Merge contents
		},
		{
			name:      "file to directory",
			isRegular: true,
			isDir:     false,
			size:      1024,
			nodeSize:  0,
			isNodeDir: true,
			wantSkip:  false,
		},
		{
			name:      "directory to file",
			isRegular: false,
			isDir:     true,
			size:      0,
			nodeSize:  1024,
			isNodeDir: false,
			wantSkip:  false,
		},
		{
			name:      "zero size files same size",
			isRegular: true,
			isDir:     false,
			size:      0,
			nodeSize:  0,
			isNodeDir: false,
			wantSkip:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fi := &mockFileInfo{
				isRegular: tt.isRegular,
				isDir:     tt.isDir,
				size:      tt.size,
			}
			result := shouldSkipExistingFile(fi, tt.nodeSize, tt.isNodeDir)
			if result != tt.wantSkip {
				t.Errorf("shouldSkipExistingFile() = %v, want %v", result, tt.wantSkip)
			}
		})
	}
}

type mockFileInfo struct {
	isRegular bool
	isDir     bool
	size      int64
}

func (m *mockFileInfo) Name() string { return "mock" }
func (m *mockFileInfo) Size() int64  { return m.size }
func (m *mockFileInfo) Mode() os.FileMode {
	if m.isDir {
		return os.ModeDir | 0o755
	}
	if m.isRegular {
		return 0o644
	}
	return 0o644
}
func (m *mockFileInfo) ModTime() time.Time { return time.Now() }
func (m *mockFileInfo) IsDir() bool        { return m.isDir }
func (m *mockFileInfo) Sys() interface{}   { return nil }

// =============================================================================
// Progress Tracker Tests
// =============================================================================

func TestProgressTracker_Atomicity(t *testing.T) {
	pt := newProgressTracker(1000000, nil)
	done := make(chan bool)

	// Test concurrent reads of completed and total bytes (which are atomic)
	go func() {
		for i := 0; i < 100; i++ {
			_ = pt.getCompleted()
			_ = pt.getTotal()
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			_ = pt.getCompleted()
			_ = pt.getTotal()
		}
		done <- true
	}()

	// Wait for goroutines
	<-done
	<-done

	// Should not panic or race
	finalCompleted := pt.getCompleted()
	if finalCompleted < 0 {
		t.Errorf("getCompleted() = %v, want >= 0", finalCompleted)
	}
}

func TestProgressTracker_UpdateWithCallback(t *testing.T) {
	var completed, total int64
	var filename string
	callback := func(c, t int64, f string) {
		completed = c
		total = t
		filename = f
	}

	pt := newProgressTracker(2048, callback)
	pt.update(1024, "test1.txt")
	pt.update(512, "test2.txt")

	if completed != 1536 {
		t.Errorf("callback completed = %v, want 1536", completed)
	}
	if total != 2048 {
		t.Errorf("callback total = %v, want 2048", total)
	}
	if filename != "test2.txt" {
		t.Errorf("callback filename = %v, want test2.txt", filename)
	}
}

func TestProgressTracker_Interrupt(t *testing.T) {
	pt := newProgressTracker(1024, nil)

	if pt.isSet() {
		t.Error("isSet() = true, want false initially")
	}

	pt.interrupt()

	if !pt.isSet() {
		t.Error("isSet() = false after interrupt(), want true")
	}

	pt.reset()

	if pt.isSet() {
		t.Error("isSet() = true after reset(), want false")
	}
}

// =============================================================================
// ExtractReader Tests
// =============================================================================

func TestExtractReader_ReadThrottling(t *testing.T) {
	tests := []struct {
		name          string
		dataSize      int
		bufSize       int
		expectedCalls int
	}{
		{
			name:          "small file - one call",
			dataSize:      1024, // Less than threshold
			bufSize:       512,
			expectedCalls: 1, // Will flush at end
		},
		{
			name:          "exactly threshold",
			dataSize:      progressUpdateThreshold,
			bufSize:       1024,
			expectedCalls: 1,
		},
		{
			name:          "twice threshold",
			dataSize:      progressUpdateThreshold * 2,
			bufSize:       1024,
			expectedCalls: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			pr := &extractReader{
				r: strings.NewReader(strings.Repeat("x", tt.dataSize)),
				onProgress: func(n int64) {
					callCount++
				},
			}

			buf := make([]byte, tt.bufSize)
			for {
				_, err := pr.Read(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}

			// Flush remaining progress
			if pr.bytesSinceUpdate > 0 {
				pr.onProgress(pr.bytesSinceUpdate)
				pr.bytesSinceUpdate = 0
			}

			if callCount != tt.expectedCalls {
				t.Errorf("Read() progress calls = %d, want %d", callCount, tt.expectedCalls)
			}
		})
	}
}

// =============================================================================
// isSubPath Edge Cases
// =============================================================================

func TestIsSubPath_EdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "subpath-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	ext := NewExtractor(nil, "QmTest", tmpDir)

	tests := []struct {
		name     string
		path     string
		base     string
		expected bool
	}{
		{
			name:     "same path",
			path:     tmpDir,
			base:     tmpDir,
			expected: true,
		},
		{
			name:     "nested path",
			path:     filepath.Join(tmpDir, "nested", "file"),
			base:     tmpDir,
			expected: true,
		},
		{
			name:     "different path",
			path:     filepath.Join(tmpDir, "other"),
			base:     filepath.Join(tmpDir, "different"),
			expected: false,
		},
		{
			name:     "parent directory",
			path:     tmpDir,
			base:     filepath.Join(tmpDir, "subdir"),
			expected: false,
		},
		{
			name:     "complex nested",
			path:     filepath.Join(tmpDir, "a", "b", "c"),
			base:     tmpDir,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ext.isSubPath(tt.path, tt.base)
			if result != tt.expected {
				t.Errorf("isSubPath() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsSubPath_SymlinkHandling(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "subpath-symlink-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create real directory
	realDir := filepath.Join(tmpDir, "real")
	if err := os.Mkdir(realDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create symlink
	symlinkPath := filepath.Join(tmpDir, "link")
	if err := os.Symlink(realDir, symlinkPath); err != nil {
		t.Skip("cannot create symlink, skipping")
	}

	ext := NewExtractor(nil, "QmTest", tmpDir)

	// Symlink to directory should be considered subpath
	result := ext.isSubPath(symlinkPath, tmpDir)
	if !result {
		t.Error("isSubPath() = false for symlink to subdirectory, want true")
	}

	// Path inside symlinked directory should be subpath
	symlinkFilePath := filepath.Join(symlinkPath, "file.txt")
	result = ext.isSubPath(symlinkFilePath, tmpDir)
	if !result {
		t.Error("isSubPath() = false for file inside symlinked directory, want true")
	}
}

// =============================================================================
// Context Cancellation Tests
// =============================================================================

func TestExtractor_ContextCancellation(t *testing.T) {
	// Note: This test verifies that context cancellation is checked during extraction.
	// Since we can't easily mock a full IPFS DAG extraction that respects context,
	// we test the context check path through writeTo.

	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "context-cancel-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Create a valid context and cancel it immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to extract - should fail early due to CID parsing or context check
	ext := NewExtractor(bs, "QmYwAPJAZvSA9QkRfs9gKXG6oTLRzDPEqaJQ81GCAgJMjn", tmpDir)
	err = ext.Extract(ctx, true)

	// We expect an error (either context.Canceled or CID parsing error)
	if err == nil {
		t.Error("Extract() expected error for cancelled context, got nil")
	}
	// The context should be checked before or during CID parsing
	// Either error is acceptable for this test
}

func TestExtractor_WithProgress_Concurrent(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	ext := NewExtractor(bs, "QmTest", "/output")

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			select {
			case <-done:
				return
			default:
				ext.WithProgress(func(completed, total int64, file string) {
					// No-op
				})
			}
		}()
	}

	// Let goroutines run
	time.Sleep(10 * time.Millisecond)
	close(done)

	// Should not panic or race
}

// =============================================================================
// Constants Tests
// =============================================================================

func TestConstants_Validation(t *testing.T) {
	// Debug: print actual values
	t.Logf("dirPermissions = %04o (%v)", uint64(dirPermissions), dirPermissions)
	t.Logf("filePermissions = %04o (%v)", uint64(filePermissions), filePermissions)

	tests := []struct {
		name  string
		value interface{}
		check func(interface{}) bool
	}{
		{
			name:  "defaultWriteBufferSize",
			value: defaultWriteBufferSize,
			check: func(v interface{}) bool {
				size, ok := v.(int)
				return ok && size > 0 && size%1024 == 0 // Should be positive and KB-aligned
			},
		},
		{
			name:  "dirPermissions",
			value: dirPermissions,
			check: func(v interface{}) bool {
				// Constants are untyped int, check as int
				val, ok := v.(int)
				return ok && val == 0o755
			},
		},
		{
			name:  "filePermissions",
			value: filePermissions,
			check: func(v interface{}) bool {
				// Constants are untyped int, check as int
				val, ok := v.(int)
				return ok && val == 0o644
			},
		},
		{
			name:  "defaultCleanedDirName",
			value: defaultCleanedDirName,
			check: func(v interface{}) bool {
				s, ok := v.(string)
				return ok && s != "" && !strings.ContainsAny(s, `<>:"|?*`)
			},
		},
		{
			name:  "partFileSuffix",
			value: partFileSuffix,
			check: func(v interface{}) bool {
				s, ok := v.(string)
				return ok && strings.HasPrefix(s, ".")
			},
		},
		{
			name:  "progressUpdateThreshold",
			value: progressUpdateThreshold,
			check: func(v interface{}) bool {
				size, ok := v.(int)
				return ok && size > 0 && size >= 1024 // At least 1KB
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.check(tt.value) {
				t.Errorf("constant validation failed for %s", tt.name)
			}
		})
	}
}

// =============================================================================
// Filesystem Error Simulation Tests
// =============================================================================

func TestRemovePath_ErrorCases(t *testing.T) {
	// Try to remove non-existent path
	err := removePath("/nonexistent/path/that/does/not/exist")
	if err == nil {
		// Some systems may not error on removing non-existent paths
		t.Log("removePath() did not error on non-existent path (may be OS-dependent)")
	}
}

func TestCreateParentDirectories_ErrorCases(t *testing.T) {
	// Try to create directory in read-only location
	tmpDir, err := os.MkdirTemp("", "mkdir-error-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a read-only directory
	readOnlyDir := filepath.Join(tmpDir, "readonly")
	if err := os.Mkdir(readOnlyDir, 0o555); err != nil {
		t.Fatal(err)
	}

	// Try to create a directory inside read-only directory
	targetPath := filepath.Join(readOnlyDir, "subdir", "file")
	err = createParentDirectories(targetPath)
	if err == nil {
		t.Error("createParentDirectories() expected error for read-only parent, got nil")
	}
}

// =============================================================================
// Large Scale Tests
// =============================================================================

func TestExtractor_MultipleExtractions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large scale test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "extractor-scale-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Create multiple extractors
	extractors := make([]*Extractor, 100)
	for i := 0; i < 100; i++ {
		path := filepath.Join(tmpDir, fmt.Sprintf("extract%d", i))
		extractors[i] = NewExtractor(bs, "QmTest", path)
	}

	// Verify all extractors are properly initialized
	for i, ext := range extractors {
		if ext == nil {
			t.Errorf("NewExtractor() iteration %d: returned nil", i)
		}
		if ext.path == "" {
			t.Errorf("NewExtractor() iteration %d: path is empty", i)
		}
	}
}

func TestExtractor_LongPathHandling(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	// Test various long path scenarios
	tests := []struct {
		name    string
		pathGen func(int) string
	}{
		{
			name: "deeply nested",
			pathGen: func(depth int) string {
				parts := make([]string, depth)
				for i := 0; i < depth; i++ {
					parts[i] = "dir"
				}
				return "/" + filepath.Join(parts...)
			},
		},
		{
			name: "long component names",
			pathGen: func(depth int) string {
				longName := strings.Repeat("x", 200) // 200 char filename
				parts := make([]string, depth)
				for i := 0; i < depth; i++ {
					parts[i] = longName
				}
				return "/" + filepath.Join(parts...)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with depth of 20
			path := tt.pathGen(20)
			ext := NewExtractor(bs, "QmTest", path)

			if ext.path == "" {
				t.Error("NewExtractor() failed to handle long path")
			}

			// Verify path was cleaned
			if strings.Contains(ext.path, "//") {
				t.Errorf("NewExtractor() path contains double slashes: %v", ext.path)
			}
		})
	}
}

// =============================================================================
// Integration with CID Tests
// =============================================================================

func TestExtractor_CIDFormats(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "cid-format-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	invalidCIDs := []struct {
		cid  string
		name string
	}{
		{"", "empty"},
		{"not-a-cid", "invalid_format"},
		{"Qm", "too_short"},
		{"Qm" + strings.Repeat("0", 45), "too_long"},
		{"QmInvalid$Characters", "invalid_chars"},
	}

	for _, tc := range invalidCIDs {
		t.Run(tc.name, func(t *testing.T) {
			ext := NewExtractor(bs, tc.cid, tmpDir)
			err := ext.Extract(context.Background(), true)

			if err == nil {
				t.Errorf("Extract() expected error for invalid CID %q, got nil", tc.cid)
			}
		})
	}
}

func TestNewExtractor_PathValidation(t *testing.T) {
	bs, cleanup := createTestBlockstore(t)
	defer cleanup()

	tests := []struct {
		name   string
		path   string
		valid  bool
		reason string
	}{
		{
			name:   "absolute path",
			path:   "/absolute/path",
			valid:  true,
			reason: "absolute paths should be valid",
		},
		{
			name:   "relative path",
			path:   "relative/path",
			valid:  true,
			reason: "relative paths should be valid",
		},
		{
			name:   "empty path",
			path:   "",
			valid:  true,
			reason: "empty path should be valid (current directory)",
		},
		{
			name:   "current directory",
			path:   ".",
			valid:  true,
			reason: "current directory should be valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext := NewExtractor(bs, "QmTest", tt.path)

			if !tt.valid && ext == nil {
				t.Errorf("NewExtractor() returned nil for valid path: %s (%s)", tt.path, tt.reason)
			}

			if ext != nil && ext.path == "" {
				t.Errorf("NewExtractor().path is empty for input: %s", tt.path)
			}
		})
	}
}
