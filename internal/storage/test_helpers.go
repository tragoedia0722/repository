package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// TestHelpers 提供测试辅助函数。
// 这些函数帮助减少测试代码中的重复。

// SetupTempDir 创建一个临时目录并返回其路径。
// 如果创建失败，会调用 t.Fatal。
//
// 建议在测试中使用 defer os.RemoveAll(dir) 来清理。
func SetupTempDir(t *testing.T, prefix string) string {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", prefix)
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	return tmpDir
}

// SetupStorage 创建并初始化一个新的存储实例。
// 返回存储实例和临时目录路径。
//
// 建议在测试中使用 defer CleanupTestData(t, dir, storage) 来清理。
func SetupStorage(t *testing.T) (*Storage, string) {
	t.Helper()
	tmpDir := SetupTempDir(t, "storage-test-*")

	s, err := NewStorage(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("NewStorage failed: %v", err)
	}

	t.Cleanup(func() {
		CleanupTestData(t, tmpDir, s)
	})

	return s, tmpDir
}

// CleanupTestData 清理测试数据。
// 如果是存储实例，会先关闭它。
func CleanupTestData(t *testing.T, path string, storage ...*Storage) {
	t.Helper()

	// Close storage if provided
	for _, s := range storage {
		if s != nil && !s.closed.Load() {
			if err := s.Close(); err != nil {
				t.Logf("warning: failed to close storage: %v", err)
			}
		}
	}

	// Remove the directory
	if err := os.RemoveAll(path); err != nil {
		t.Logf("warning: failed to cleanup %s: %v", path, err)
	}
}

// AssertError asserts that an error is not nil.
func AssertError(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Error(msg)
	}
}

// AssertNoError asserts that an error is nil.
func AssertNoError(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: %v", msg, err)
	}
}

// AssertFileExists asserts that a file exists.
func AssertFileExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("file should exist: %s", path)
	}
}

// AssertFileNotExists asserts that a file does not exist.
func AssertFileNotExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("file should not exist: %s", path)
	}
}

// CreateTestFile 在指定路径创建测试文件。
func CreateTestFile(t *testing.T, path string, content []byte) {
	t.Helper()
	err := os.WriteFile(path, content, 0o644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
}

// CreateTestDir 创建测试目录结构。
func CreateTestDir(t *testing.T, path string, subdirs ...string) {
	t.Helper()
	for _, subdir := range subdirs {
		fullPath := filepath.Join(path, subdir)
		if err := os.MkdirAll(fullPath, 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}
	}
}
