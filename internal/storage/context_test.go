package storage

import (
	"context"
	"testing"
	"time"
)

func TestNewStorageWithContext(t *testing.T) {
	t.Run("creates storage with valid context", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := SetupTempDir(t, "storage-ctx-*")
		defer CleanupTestData(t, tmpDir)

		s, err := NewStorageWithContext(ctx, tmpDir)
		if err != nil {
			t.Fatalf("NewStorageWithContext failed: %v", err)
		}
		defer s.Close()

		if s == nil {
			t.Fatal("expected non-nil storage")
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		tmpDir := SetupTempDir(t, "storage-ctx-cancel-*")
		defer CleanupTestData(t, tmpDir)

		_, err := NewStorageWithContext(ctx, tmpDir)
		if err == nil {
			t.Error("expected error for cancelled context")
		}
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("respects context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Wait for context to expire
		<-ctx.Done()

		tmpDir := SetupTempDir(t, "storage-ctx-timeout-*")
		defer CleanupTestData(t, tmpDir)

		_, err := NewStorageWithContext(ctx, tmpDir)
		if err == nil {
			t.Error("expected error for expired context")
		}
		if err != context.DeadlineExceeded {
			t.Errorf("expected context.DeadlineExceeded, got %v", err)
		}
	})
}

func TestCloseWithContext(t *testing.T) {
	t.Run("closes storage with valid context", func(t *testing.T) {
		ctx := context.Background()
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		err := s.CloseWithContext(ctx)
		if err != nil {
			t.Errorf("CloseWithContext failed: %v", err)
		}

		// Verify it's closed
		if !s.closed.Load() {
			t.Error("storage should be marked as closed")
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		s, tmpDir := SetupStorage(t)

		err := s.CloseWithContext(ctx)
		if err == nil {
			t.Error("expected error for cancelled context")
		}
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}

		// Clean up manually since Close failed
		s.Close()
		CleanupTestData(t, tmpDir)
	})

	t.Run("idempotent close with context", func(t *testing.T) {
		ctx := context.Background()
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		// First close
		err := s.CloseWithContext(ctx)
		if err != nil {
			t.Fatalf("first CloseWithContext failed: %v", err)
		}

		// Second close should not error
		err = s.CloseWithContext(ctx)
		if err != nil {
			t.Errorf("second CloseWithContext should not error, got: %v", err)
		}
	})
}

func TestDestroyWithContext(t *testing.T) {
	t.Run("destroys storage with valid context", func(t *testing.T) {
		ctx := context.Background()
		s, tmpDir := SetupStorage(t)
		// Don't use CleanupTestData since we're testing Destroy

		err := s.DestroyWithContext(ctx)
		if err != nil {
			t.Errorf("DestroyWithContext failed: %v", err)
		}

		// Verify directory is gone
		AssertFileNotExists(t, tmpDir)
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		s, _ := SetupStorage(t)

		err := s.DestroyWithContext(ctx)
		if err == nil {
			t.Error("expected error for cancelled context")
		}
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}

		// Clean up manually since Destroy failed
		s.Destroy()
	})

	t.Run("destroys closed storage with context", func(t *testing.T) {
		ctx := context.Background()
		s, tmpDir := SetupStorage(t)

		s.Close()

		err := s.DestroyWithContext(ctx)
		if err != nil {
			t.Errorf("DestroyWithContext on closed storage failed: %v", err)
		}

		AssertFileNotExists(t, tmpDir)
	})
}

func TestNewStorage_BackwardCompatibility(t *testing.T) {
	t.Run("NewStorage works without context", func(t *testing.T) {
		tmpDir := SetupTempDir(t, "storage-compat-*")
		defer CleanupTestData(t, tmpDir)

		s, err := NewStorage(tmpDir)
		if err != nil {
			t.Fatalf("NewStorage failed: %v", err)
		}
		defer s.Close()

		if s == nil {
			t.Fatal("expected non-nil storage")
		}
	})
}

func TestClose_BackwardCompatibility(t *testing.T) {
	t.Run("Close works without context", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		defer CleanupTestData(t, tmpDir)

		err := s.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}

		if !s.closed.Load() {
			t.Error("storage should be marked as closed")
		}
	})
}

func TestDestroy_BackwardCompatibility(t *testing.T) {
	t.Run("Destroy works without context", func(t *testing.T) {
		s, tmpDir := SetupStorage(t)
		// Don't use CleanupTestData since we're testing Destroy

		err := s.Destroy()
		if err != nil {
			t.Errorf("Destroy failed: %v", err)
		}

		AssertFileNotExists(t, tmpDir)
	})
}

func TestContextMethods_Equivalence(t *testing.T) {
	t.Run("Close and CloseWithContext are equivalent with Background", func(t *testing.T) {
		s1, tmpDir1 := SetupStorage(t)
		s2, tmpDir2 := SetupStorage(t)
		defer CleanupTestData(t, tmpDir1)
		defer CleanupTestData(t, tmpDir2)

		ctx := context.Background()

		// Both should succeed
		err1 := s1.Close()
		err2 := s2.CloseWithContext(ctx)

		if (err1 == nil) != (err2 == nil) {
			t.Errorf("Close and CloseWithContext should behave the same: err1=%v, err2=%v", err1, err2)
		}
	})

	t.Run("Destroy and DestroyWithContext are equivalent with Background", func(t *testing.T) {
		s1, tmpDir1 := SetupStorage(t)
		s2, tmpDir2 := SetupStorage(t)
		// Don't use CleanupTestData

		ctx := context.Background()

		// Both should succeed
		err1 := s1.Destroy()
		err2 := s2.DestroyWithContext(ctx)

		if (err1 == nil) != (err2 == nil) {
			t.Errorf("Destroy and DestroyWithContext should behave the same: err1=%v, err2=%v", err1, err2)
		}

		CleanupTestData(t, tmpDir1)
		CleanupTestData(t, tmpDir2)
	})
}
