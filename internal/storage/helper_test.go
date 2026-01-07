package storage

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestResolvePath(t *testing.T) {
	tests := []struct {
		name     string
		rootPath string
		basePath string
		want     string
	}{
		{
			name:     "absolute path returns as-is",
			rootPath: "/root",
			basePath: "/absolute/path",
			want:     "/absolute/path",
		},
		{
			name:     "relative path joins to root",
			rootPath: "/root",
			basePath: "relative",
			want:     "/root/relative",
		},
		{
			name:     "relative path with subdirectory",
			rootPath: "/root",
			basePath: "sub/dir",
			want:     "/root/sub/dir",
		},
		{
			name:     "relative path with parent references",
			rootPath: "/root",
			basePath: "sub/../dir",
			want:     "/root/dir", // filepath.Join cleans the path
		},
		{
			name:     "empty basePath returns root",
			rootPath: "/root",
			basePath: "",
			want:     "/root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolvePath(tt.rootPath, tt.basePath)
			if got != tt.want {
				t.Errorf("resolvePath(%q, %q) = %q, want %q",
					tt.rootPath, tt.basePath, got, tt.want)
			}
		})
	}
}

func TestResolvePath_WindowsPaths(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("skipping Windows-specific path tests on non-Windows OS")
	}

	tests := []struct {
		name     string
		rootPath string
		basePath string
		want     string
	}{
		{
			name:     "Windows absolute path",
			rootPath: "C:\\root",
			basePath: "D:\\absolute",
			want:     "D:\\absolute",
		},
		{
			name:     "Windows relative path",
			rootPath: "C:\\root",
			basePath: "relative",
			want:     "C:\\root\\relative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolvePath(tt.rootPath, tt.basePath)
			if got != tt.want {
				t.Errorf("resolvePath(%q, %q) = %q, want %q",
					tt.rootPath, tt.basePath, got, tt.want)
			}
		})
	}
}

func TestResolvePath_CleanPaths(t *testing.T) {
	t.Run("root with trailing separator", func(t *testing.T) {
		root := filepath.Join("root", "") // adds separator
		base := "sub"
		got := resolvePath(root, base)
		// Result should be consistent
		expected := filepath.Join("root", "sub")
		if got != expected {
			t.Errorf("resolvePath(%q, %q) = %q, want %q", root, base, got, expected)
		}
	})

	t.Run("basePath cleans . and ..", func(t *testing.T) {
		// filepath.Join automatically cleans paths
		root := "/root"
		base := "./sub/../dir"
		got := resolvePath(root, base)
		// filepath.Join cleans the path
		want := "/root/dir"
		if got != want {
			t.Errorf("resolvePath(%q, %q) = %q, want %q", root, base, got, want)
		}
	})
}

func TestDatastoreConfigRegistry(t *testing.T) {
	t.Run("list returns all registered types", func(t *testing.T) {
		// Ensure registry is initialized
		ensureInitialized()

		types := globalConfigRegistry.list()

		if len(types) == 0 {
			t.Error("list should return at least one type")
		}

		// Check for expected types
		expectedTypes := map[string]bool{
			"mount":   false,
			"measure": false,
			"levelds": false,
			"flatfs":  false,
		}

		for _, typ := range types {
			if _, exists := expectedTypes[typ]; exists {
				expectedTypes[typ] = true
			}
		}

		for typ, found := range expectedTypes {
			if !found {
				t.Errorf("list should include type %q", typ)
			}
		}
	})

	t.Run("get returns registered factory", func(t *testing.T) {
		ensureInitialized()

		// Test getting a known type
		factory := globalConfigRegistry.get("levelds")
		if factory == nil {
			t.Error("get should return factory for 'levelds'")
		}

		// Test that factory works
		params := map[string]interface{}{
			"type": "levelds",
			"path": "datastore",
		}

		config, err := factory(params)
		if err != nil {
			t.Fatalf("factory failed: %v", err)
		}

		if config == nil {
			t.Error("factory should return non-nil config")
		}
	})

	t.Run("get returns nil for unknown type", func(t *testing.T) {
		factory := globalConfigRegistry.get("unknown-type-xyz")
		if factory != nil {
			t.Error("get should return nil for unknown type")
		}
	})

	t.Run("register adds new type", func(t *testing.T) {
		// Create a custom registry for this test
		customRegistry := &configRegistry{
			factories: make(map[string]ConfigFactory),
		}

		// Register a custom factory
		customFactory := func(params map[string]interface{}) (DatastoreConfig, error) {
			return nil, nil
		}

		customRegistry.register("custom", customFactory)

		// Verify it was registered
		factory := customRegistry.get("custom")
		if factory == nil {
			t.Error("register should add factory")
		}

		// Test that the factory works
		config, err := factory(map[string]interface{}{})
		if err != nil {
			t.Errorf("factory should work: %v", err)
		}
		if config != nil {
			t.Error("test factory should return nil config")
		}
	})

	t.Run("register overwrites existing type", func(t *testing.T) {
		customRegistry := &configRegistry{
			factories: make(map[string]ConfigFactory),
		}

		// Use a flag to track which factory was called
		called := false

		// Register initial factory
		firstFactory := func(params map[string]interface{}) (DatastoreConfig, error) {
			return nil, nil
		}
		customRegistry.register("test", firstFactory)

		// Register replacement factory that sets the flag
		secondFactory := func(params map[string]interface{}) (DatastoreConfig, error) {
			called = true
			return nil, nil
		}
		customRegistry.register("test", secondFactory)

		// Verify by calling the retrieved factory
		factory := customRegistry.get("test")
		factory(nil)

		if !called {
			t.Error("second factory should have been called (was overwritten)")
		}
	})
}

func TestAnyDatastoreConfig_CaseInsensitive(t *testing.T) {
	tests := []struct {
		name        string
		typeValue   string
		shouldError bool
	}{
		{
			name:        "lowercase",
			typeValue:   "levelds",
			shouldError: false,
		},
		{
			name:        "uppercase",
			typeValue:   "LEVELDS",
			shouldError: false,
		},
		{
			name:        "mixed case",
			typeValue:   "LevelDs",
			shouldError: false,
		},
		{
			name:        "unknown type",
			typeValue:   "unknown",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]interface{}{
				"type": tt.typeValue,
				"path": "datastore",
			}

			config, err := AnyDatastoreConfig(params)

			if tt.shouldError {
				if err == nil {
					t.Errorf("expected error for type %q", tt.typeValue)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for type %q: %v", tt.typeValue, err)
				}
				if config == nil {
					t.Error("config should not be nil")
				}
			}
		})
	}
}
