package storage

import (
	"errors"
	"testing"
)

func TestStorageError(t *testing.T) {
	t.Run("Error method returns formatted message", func(t *testing.T) {
		baseErr := errors.New("base error")
		err := &StorageError{
			Operation: "test operation",
			Path:      "/test/path",
			Err:       baseErr,
		}

		expected := "test operation failed at /test/path: base error"
		if err.Error() != expected {
			t.Errorf("Error() = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("Error without path", func(t *testing.T) {
		baseErr := errors.New("base error")
		err := &StorageError{
			Operation: "test operation",
			Path:      "",
			Err:       baseErr,
		}

		expected := "test operation failed: base error"
		if err.Error() != expected {
			t.Errorf("Error() = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("Unwrap returns underlying error", func(t *testing.T) {
		baseErr := errors.New("base error")
		err := &StorageError{
			Operation: "test",
			Path:      "/test",
			Err:       baseErr,
		}

		if !errors.Is(err, baseErr) {
			t.Error("Unwrap() should return base error")
		}
	})
}

func TestConfigError(t *testing.T) {
	t.Run("Error method returns formatted message", func(t *testing.T) {
		baseErr := errors.New("invalid config")
		err := &ConfigError{
			Field: "testField",
			Value: "testValue",
			Err:   baseErr,
		}

		expected := "config field 'testField' (value: testValue): invalid config"
		if err.Error() != expected {
			t.Errorf("Error() = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("Error without value", func(t *testing.T) {
		baseErr := errors.New("invalid config")
		err := &ConfigError{
			Field: "testField",
			Value: nil,
			Err:   baseErr,
		}

		expected := "config field 'testField': invalid config"
		if err.Error() != expected {
			t.Errorf("Error() = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("Unwrap returns underlying error", func(t *testing.T) {
		baseErr := errors.New("base error")
		err := &ConfigError{
			Field: "test",
			Err:   baseErr,
		}

		if !errors.Is(err, baseErr) {
			t.Error("Unwrap() should return base error")
		}
	})
}

func TestLockError(t *testing.T) {
	t.Run("Error method returns formatted message", func(t *testing.T) {
		baseErr := errors.New("lock held")
		err := &LockError{
			Path: "/test/lock",
			Err:  baseErr,
		}

		expected := "lock file error at /test/lock: lock held"
		if err.Error() != expected {
			t.Errorf("Error() = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("Unwrap returns underlying error", func(t *testing.T) {
		baseErr := errors.New("base error")
		err := &LockError{
			Path: "/test/lock",
			Err:  baseErr,
		}

		if !errors.Is(err, baseErr) {
			t.Error("Unwrap() should return base error")
		}
	})
}

func TestInvalidPathError(t *testing.T) {
	tests := []struct {
		name   string
		err    *InvalidPathError
		expect string
	}{
		{
			name: "empty path",
			err: &InvalidPathError{
				Path:   "",
				Reason: "path cannot be empty",
			},
			expect: "invalid path '': path cannot be empty",
		},
		{
			name: "invalid characters",
			err: &InvalidPathError{
				Path:   "/path\x00with/null",
				Reason: "contains null byte",
			},
			expect: "invalid path '/path\x00with/null': contains null byte",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Error() != tt.expect {
				t.Errorf("Error() = %q, want %q", tt.err.Error(), tt.expect)
			}
		})
	}
}

func TestErrorTypeMatching(t *testing.T) {
	t.Run("errors.As can extract StorageError", func(t *testing.T) {
		baseErr := errors.New("base")
		err := &StorageError{
			Operation: "test",
			Path:      "/test",
			Err:       baseErr,
		}

		var storageErr *StorageError
		if !errors.As(err, &storageErr) {
			t.Error("errors.As should extract StorageError")
		}

		if storageErr.Operation != "test" {
			t.Errorf("Operation = %q, want %q", storageErr.Operation, "test")
		}
	})

	t.Run("errors.As can extract ConfigError", func(t *testing.T) {
		baseErr := errors.New("base")
		err := &ConfigError{
			Field: "test",
			Err:   baseErr,
		}

		var configErr *ConfigError
		if !errors.As(err, &configErr) {
			t.Error("errors.As should extract ConfigError")
		}

		if configErr.Field != "test" {
			t.Errorf("Field = %q, want %q", configErr.Field, "test")
		}
	})

	t.Run("errors.As can extract LockError", func(t *testing.T) {
		baseErr := errors.New("base")
		err := &LockError{
			Path: "/test/lock",
			Err:  baseErr,
		}

		var lockErr *LockError
		if !errors.As(err, &lockErr) {
			t.Error("errors.As should extract LockError")
		}

		if lockErr.Path != "/test/lock" {
			t.Errorf("Path = %q, want %q", lockErr.Path, "/test/lock")
		}
	})

	t.Run("errors.As can extract InvalidPathError", func(t *testing.T) {
		err := &InvalidPathError{
			Path:   "",
			Reason: "empty",
		}

		var pathErr *InvalidPathError
		if !errors.As(err, &pathErr) {
			t.Error("errors.As should extract InvalidPathError")
		}

		if pathErr.Reason != "empty" {
			t.Errorf("Reason = %q, want %q", pathErr.Reason, "empty")
		}
	})
}

func TestErrorWrapping(t *testing.T) {
	t.Run("StorageError wraps underlying error", func(t *testing.T) {
		baseErr := errors.New("base error")
		err := &StorageError{
			Operation: "test",
			Err:       baseErr,
		}

		if !errors.Is(err, baseErr) {
			t.Error("StorageError should wrap base error")
		}
	})

	t.Run("ConfigError wraps underlying error", func(t *testing.T) {
		baseErr := errors.New("base error")
		err := &ConfigError{
			Field: "test",
			Err:   baseErr,
		}

		if !errors.Is(err, baseErr) {
			t.Error("ConfigError should wrap base error")
		}
	})

	t.Run("LockError wraps underlying error", func(t *testing.T) {
		baseErr := errors.New("base error")
		err := &LockError{
			Path: "/test",
			Err:  baseErr,
		}

		if !errors.Is(err, baseErr) {
			t.Error("LockError should wrap base error")
		}
	})
}
