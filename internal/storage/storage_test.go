package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	ds "github.com/ipfs/go-datastore"
)

func cleanupStorage(t *testing.T, path string) {
	if err := os.RemoveAll(path); err != nil {
		t.Fatalf("failed to cleanup storage: %v", err)
	}
}

func TestNewStorage(t *testing.T) {
	t.Run("creates storage with valid path", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-new-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		s, err := newStorage(tmpDir)
		if err != nil {
			t.Fatalf("newStorage failed: %v", err)
		}

		if s == nil {
			t.Fatal("expected non-nil storage")
		}

		if s.path != tmpDir {
			t.Errorf("path mismatch: got %s, want %s", s.path, tmpDir)
		}
	})

	t.Run("expands tilde in path", func(t *testing.T) {
		// Note: This test assumes home directory exists
		homeDir, err := os.UserHomeDir()
		if err != nil {
			t.Skip("no home directory available")
		}

		s, err := newStorage("~/test")
		if err != nil {
			t.Fatalf("newStorage failed: %v", err)
		}

		expected := filepath.Join(homeDir, "test")
		if s.path != expected {
			t.Errorf("path not expanded: got %s, want %s", s.path, expected)
		}
	})

	t.Run("cleans path", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-clean-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		dirtyPath := filepath.Join(tmpDir, "subdir", "..", ".")
		cleanPath := filepath.Clean(tmpDir)

		s, err := newStorage(dirtyPath)
		if err != nil {
			t.Fatalf("newStorage failed: %v", err)
		}

		if s.path != cleanPath {
			t.Errorf("path not cleaned: got %s, want %s", s.path, cleanPath)
		}
	})

	t.Run("fails with empty path", func(t *testing.T) {
		_, err := newStorage("")
		if err == nil {
			t.Error("expected error for empty path, got nil")
		}
	})
}

func TestNewStorage_OpenClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-open-close-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer cleanupStorage(t, tmpDir)

	s, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if s == nil {
		t.Fatal("expected non-nil storage")
	}

	// Close
	err = s.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Close again (should be idempotent)
	err = s.Close()
	if err != nil {
		t.Logf("Second Close returned error: %v", err)
	}
}

func TestStorage_Datastore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-ds-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer cleanupStorage(t, tmpDir)

	s, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	defer s.Close()

	store := s.Datastore()
	if store == nil {
		t.Fatal("Datastore returned nil")
	}

	// Test basic datastore operations
	ctx := context.Background()
	key := ds.NewKey("/test/key")
	value := []byte("test value")

	err = store.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(value) {
		t.Errorf("value mismatch: got %q, want %q", retrieved, value)
	}

	err = store.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = store.Get(ctx, key)
	if err == nil {
		t.Error("expected error after delete, got nil")
	}
}

func TestStorage_GetStorageUsage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-usage-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer cleanupStorage(t, tmpDir)

	s, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Initial usage
	usage1, err := s.GetStorageUsage(ctx)
	if err != nil {
		t.Fatalf("GetStorageUsage failed: %v", err)
	}

	// Add some data
	store := s.Datastore()
	for i := 0; i < 100; i++ {
		key := ds.NewKey("/test/key" + string(rune('0'+i)))
		value := make([]byte, 1024) // 1KB each
		err = store.Put(ctx, key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Usage should increase
	usage2, err := s.GetStorageUsage(ctx)
	if err != nil {
		t.Fatalf("GetStorageUsage failed: %v", err)
	}

	if usage2 <= usage1 {
		t.Errorf("expected usage to increase: got %d, was %d", usage2, usage1)
	}
}

func TestStorage_Destroy(t *testing.T) {
	t.Run("destroys open storage", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-destroy-open-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		// Don't use defer cleanup since we're testing Destroy

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		// Add some data
		ctx := context.Background()
		store := s.Datastore()
		key := ds.NewKey("/test/key")
		err = store.Put(ctx, key, []byte("test"))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Destroy
		err = s.Destroy()
		if err != nil {
			t.Errorf("Destroy failed: %v", err)
		}

		// Verify directory is gone
		if _, err := os.Stat(tmpDir); !os.IsNotExist(err) {
			t.Error("storage directory should be removed after Destroy")
		}
	})

	t.Run("destroys closed storage", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-destroy-closed-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		s.Close()

		err = s.Destroy()
		if err != nil {
			t.Errorf("Destroy on closed storage failed: %v", err)
		}

		// Verify directory is gone
		if _, err := os.Stat(tmpDir); !os.IsNotExist(err) {
			t.Error("storage directory should be removed after Destroy")
		}
	})
}

func TestStorage_LockFile(t *testing.T) {
	t.Run("creates lock file", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-lock-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}
		defer s.Close()

		// Check lock file exists
		lockPath := filepath.Join(tmpDir, LockFile)
		if _, err := os.Stat(lockPath); os.IsNotExist(err) {
			t.Error("lock file should exist")
		}
	})

	t.Run("removes lock file on close", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-lock-close-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}

		s.Close()

		// Check lock file is removed
		lockPath := filepath.Join(tmpDir, LockFile)
		if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
			t.Error("lock file should be removed after close")
		}
	})
}

func TestWritable(t *testing.T) {
	t.Run("writable directory", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-writable-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		err = Writable(tmpDir)
		if err != nil {
			t.Errorf("Writable failed: %v", err)
		}
	})

	t.Run("non-writable directory", func(t *testing.T) {
		// Skip on Windows as permissions work differently
		if os.Getenv("GOOS") == "windows" {
			t.Skip("skipping on Windows")
		}

		tmpDir, err := os.MkdirTemp("", "storage-nonwritable-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		// Make directory read-only
		err = os.Chmod(tmpDir, 0o444)
		if err != nil {
			t.Fatalf("failed to chmod: %v", err)
		}
		defer os.Chmod(tmpDir, 0o755)

		err = Writable(tmpDir)
		if err == nil {
			t.Error("expected error for read-only directory")
		}
	})

	t.Run("creates directory if not exists", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "storage-writable-mkdir-test")
		defer cleanupStorage(t, tmpDir)

		// Ensure directory doesn't exist
		_, err := os.Stat(tmpDir)
		if !os.IsNotExist(err) {
			t.Skip("directory already exists")
		}

		err = Writable(tmpDir)
		if err != nil {
			t.Errorf("Writable failed: %v", err)
		}

		// Check directory was created
		if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
			t.Error("directory should be created")
		}
	})
}

func TestDatastoreSpecPath(t *testing.T) {
	tests := []struct {
		name     string
		repoPath string
		expected string
	}{
		{
			name:     "simple path",
			repoPath: "/path/to/repo",
			expected: "/path/to/repo/datastore_spec",
		},
		{
			name:     "relative path",
			repoPath: "repo",
			expected: "repo/datastore_spec",
		},
		{
			name:     "path with trailing separator",
			repoPath: "/path/to/repo/",
			expected: "/path/to/repo/datastore_spec",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DatastoreSpecPath(tt.repoPath)
			if result != tt.expected {
				t.Errorf("DatastoreSpecPath(%q) = %q, want %q", tt.repoPath, result, tt.expected)
			}
		})
	}
}

func TestFileExists(t *testing.T) {
	t.Run("existing file", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-fileexists-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		testFile := filepath.Join(tmpDir, "test.txt")
		err = os.WriteFile(testFile, []byte("test"), 0o644)
		if err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		if !FileExists(testFile) {
			t.Error("FileExists returned false for existing file")
		}
	})

	t.Run("non-existent file", func(t *testing.T) {
		if FileExists("/non/existent/file") {
			t.Error("FileExists returned true for non-existent file")
		}
	})

	t.Run("empty file", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-fileexists-empty-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		emptyFile := filepath.Join(tmpDir, "empty.txt")
		err = os.WriteFile(emptyFile, []byte{}, 0o644)
		if err != nil {
			t.Fatalf("failed to create empty file: %v", err)
		}

		if FileExists(emptyFile) {
			t.Error("FileExists returned true for empty file")
		}
	})

	t.Run("directory", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-fileexists-dir-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		// Directories typically have size > 0 (due to metadata)
		// The implementation checks fi.Size() > 0
		result := FileExists(tmpDir)
		t.Logf("FileExists on directory: %v", result)
	})
}

func TestInitSpec(t *testing.T) {
	t.Run("creates spec for new storage", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-initspec-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		err = initSpec(tmpDir, DefaultDiskSpec())
		if err != nil {
			t.Fatalf("initSpec failed: %v", err)
		}

		// Check spec file was created
		specPath := DatastoreSpecPath(tmpDir)
		if _, err := os.Stat(specPath); os.IsNotExist(err) {
			t.Error("spec file should be created")
		}
	})

	t.Run("does not overwrite existing spec", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-initspec-existing-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		// Create initial spec
		err = initSpec(tmpDir, DefaultDiskSpec())
		if err != nil {
			t.Fatalf("initSpec failed: %v", err)
		}

		specPath := DatastoreSpecPath(tmpDir)
		originalContent, _ := os.ReadFile(specPath)

		// Try to init again
		err = initSpec(tmpDir, DefaultDiskSpec())
		if err != nil {
			t.Fatalf("initSpec failed: %v", err)
		}

		// Content should be unchanged
		currentContent, _ := os.ReadFile(specPath)
		if string(originalContent) != string(currentContent) {
			t.Error("spec file should not be overwritten")
		}
	})
}

func TestReadSpec(t *testing.T) {
	t.Run("reads existing spec", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-readspec-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		// Create spec
		expectedSpec := DefaultDiskSpec().String()
		specPath := DatastoreSpecPath(tmpDir)
		err = os.WriteFile(specPath, []byte(expectedSpec), 0o600)
		if err != nil {
			t.Fatalf("failed to write spec: %v", err)
		}

		s, err := newStorage(tmpDir)
		if err != nil {
			t.Fatalf("newStorage failed: %v", err)
		}

		actualSpec, err := s.readSpec()
		if err != nil {
			t.Fatalf("readSpec failed: %v", err)
		}

		if actualSpec != expectedSpec {
			t.Errorf("spec mismatch: got %q, want %q", actualSpec, expectedSpec)
		}
	})

	t.Run("returns error for non-existent spec", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "storage-readspec-nospec-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer cleanupStorage(t, tmpDir)

		s, err := newStorage(tmpDir)
		if err != nil {
			t.Fatalf("newStorage failed: %v", err)
		}

		_, err = s.readSpec()
		if err == nil {
			t.Error("expected error for non-existent spec, got nil")
		}
	})
}

func TestDefaultDiskSpec(t *testing.T) {
	spec := DefaultDiskSpec()

	if spec == nil {
		t.Fatal("DefaultDiskSpec returned nil")
	}

	// Check required fields
	specType, ok := spec["type"].(string)
	if !ok || specType != "mount" {
		t.Error("spec type should be 'mount'")
	}

	// Should have mounts
	mounts, ok := spec["mounts"].([]interface{})
	if !ok || len(mounts) == 0 {
		t.Error("spec should have mounts")
	}

	// Check Bytes() works
	bytes := spec.Bytes()
	if len(bytes) == 0 {
		t.Error("Bytes() returned empty")
	}

	// Check String() works
	str := spec.String()
	if str == "" {
		t.Error("String() returned empty")
	}

	if str != string(bytes) {
		t.Error("String() should return Bytes() as string")
	}
}

func TestAnyDatastoreConfig(t *testing.T) {
	t.Run("creates mount config", func(t *testing.T) {
		params := map[string]interface{}{
			"type": "mount",
			"mounts": []interface{}{
				map[string]interface{}{
					"mountpoint": "/",
					"type":       "measure",
					"prefix":     "test",
					"child": map[string]interface{}{
						"type": "levelds",
						"path": "datastore",
					},
				},
			},
		}

		config, err := AnyDatastoreConfig(params)
		if err != nil {
			t.Fatalf("AnyDatastoreConfig failed: %v", err)
		}

		if config == nil {
			t.Fatal("config is nil")
		}
	})

	t.Run("creates measure config", func(t *testing.T) {
		params := map[string]interface{}{
			"type":   "measure",
			"prefix": "test",
			"child": map[string]interface{}{
				"type": "levelds",
				"path": "datastore",
			},
		}

		config, err := AnyDatastoreConfig(params)
		if err != nil {
			t.Fatalf("AnyDatastoreConfig failed: %v", err)
		}

		if config == nil {
			t.Fatal("config is nil")
		}
	})

	t.Run("creates levelds config", func(t *testing.T) {
		params := map[string]interface{}{
			"type": "levelds",
			"path": "datastore",
		}

		config, err := AnyDatastoreConfig(params)
		if err != nil {
			t.Fatalf("AnyDatastoreConfig failed: %v", err)
		}

		if config == nil {
			t.Fatal("config is nil")
		}
	})

	t.Run("creates flatfs config", func(t *testing.T) {
		params := map[string]interface{}{
			"type":      "flatfs",
			"path":      "blocks",
			"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
			"sync":      true,
		}

		config, err := AnyDatastoreConfig(params)
		if err != nil {
			t.Fatalf("AnyDatastoreConfig failed: %v", err)
		}

		if config == nil {
			t.Fatal("config is nil")
		}
	})

	t.Run("fails with missing type", func(t *testing.T) {
		params := map[string]interface{}{}

		_, err := AnyDatastoreConfig(params)
		if err == nil {
			t.Error("expected error for missing type, got nil")
		}
	})

	t.Run("fails with unknown type", func(t *testing.T) {
		params := map[string]interface{}{
			"type": "unknown",
		}

		_, err := AnyDatastoreConfig(params)
		if err == nil {
			t.Error("expected error for unknown type, got nil")
		}
	})
}

func TestStorage_ConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-concurrent-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer cleanupStorage(t, tmpDir)

	s, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	defer s.Close()

	// Test concurrent datastore access
	done := make(chan bool)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		go func(n int) {
			store := s.Datastore()
			key := ds.NewKey("/test/concurrent" + string(rune('0'+n)))
			value := []byte("test")

			_ = store.Put(ctx, key, value)
			_, _ = store.Get(ctx, key)
			_ = store.Delete(ctx, key)

			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}
