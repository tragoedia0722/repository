package storage

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkDatastore(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bench-datastore-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	s, err := NewStorage(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("NewStorage failed: %v", err)
	}
	defer s.Destroy()
	defer os.RemoveAll(tmpDir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Datastore()
	}
}

func BenchmarkDatastore_Parallel(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bench-parallel-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	s, err := NewStorage(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("NewStorage failed: %v", err)
	}
	defer s.Destroy()
	defer os.RemoveAll(tmpDir)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = s.Datastore()
		}
	})
}

func BenchmarkClose(b *testing.B) {
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir, err := os.MkdirTemp("", "bench-close-*")
		if err != nil {
			b.Fatalf("Failed to create temp dir: %v", err)
		}
		s, err := NewStorage(tmpDir)
		if err != nil {
			os.RemoveAll(tmpDir)
			b.Fatalf("NewStorage failed: %v", err)
		}
		b.StartTimer()

		_ = s.CloseWithContext(ctx)

		b.StopTimer()
		s.Destroy()
		os.RemoveAll(tmpDir)
	}
}

func BenchmarkNewStorage(b *testing.B) {
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir, err := os.MkdirTemp("", "bench-new-*")
		if err != nil {
			b.Fatalf("Failed to create temp dir: %v", err)
		}
		b.StartTimer()

		s, err := NewStorageWithContext(ctx, tmpDir)
		if err != nil {
			b.Fatalf("NewStorageWithContext failed: %v", err)
		}

		b.StopTimer()
		s.Destroy()
		os.RemoveAll(tmpDir)
	}
}

func BenchmarkContextOverhead(b *testing.B) {
	ctx := context.Background()

	// 预热
	tmpDir, _ := os.MkdirTemp("", "bench-warmup-*")
	s, _ := NewStorage(tmpDir)
	s.Close()
	s.Destroy()
	os.RemoveAll(tmpDir)

	b.Run("WithoutContext", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			tmpDir, err := os.MkdirTemp("", "bench-no-ctx-*")
			if err != nil {
				b.Fatalf("Failed to create temp dir: %v", err)
			}
			s, err := NewStorage(tmpDir)
			if err != nil {
				os.RemoveAll(tmpDir)
				b.Fatalf("NewStorage failed: %v", err)
			}
			b.StartTimer()

			_ = s.Close()

			b.StopTimer()
			s.Destroy()
			os.RemoveAll(tmpDir)
		}
	})

	b.Run("WithContext", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			tmpDir, err := os.MkdirTemp("", "bench-with-ctx-*")
			if err != nil {
				b.Fatalf("Failed to create temp dir: %v", err)
			}
			s, err := NewStorage(tmpDir)
			if err != nil {
				os.RemoveAll(tmpDir)
				b.Fatalf("NewStorage failed: %v", err)
			}
			b.StartTimer()

			_ = s.CloseWithContext(ctx)

			b.StopTimer()
			s.Destroy()
			os.RemoveAll(tmpDir)
		}
	})
}

func BenchmarkConcurrentAccess(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bench-concurrent-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	s, err := NewStorage(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("NewStorage failed: %v", err)
	}
	defer s.Destroy()
	defer os.RemoveAll(tmpDir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					_ = s.Datastore()
				}
			}()
		}
		wg.Wait()
	}
}

func BenchmarkGetStorageUsage(b *testing.B) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "bench-usage-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	s, err := NewStorage(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("NewStorage failed: %v", err)
	}
	defer s.Destroy()
	defer os.RemoveAll(tmpDir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.GetStorageUsage(ctx)
	}
}

func BenchmarkAtomicVsMutex(b *testing.B) {
	// 测试 atomic.Bool vs mutex bool 的性能差异

	b.Run("AtomicBool", func(b *testing.B) {
		var closed atomic.Bool
		for i := 0; i < b.N; i++ {
			_ = closed.Load()
		}
	})

	b.Run("MutexBool", func(b *testing.B) {
		var mu sync.Mutex
		var closed bool
		for i := 0; i < b.N; i++ {
			mu.Lock()
			_ = closed
			mu.Unlock()
		}
	})
}

func BenchmarkResolvePath(b *testing.B) {
	tests := []struct {
		name string
		root string
		base string
	}{
		{"absolute", "/root", "/absolute/path"},
		{"relative", "/root", "relative/path"},
		{"parent_ref", "/root", "sub/../dir"},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = resolvePath(tt.root, tt.base)
			}
		})
	}
}

// 实际使用场景的基准测试
func BenchmarkRealWorldScenario(b *testing.B) {
	ctx := context.Background()

	b.Run("CreateUseAndClose", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			tmpDir, err := os.MkdirTemp("", "bench-real-*")
			if err != nil {
				b.Fatalf("Failed to create temp dir: %v", err)
			}
			b.StartTimer()

			// 创建
			s, err := NewStorageWithContext(ctx, tmpDir)
			if err != nil {
				b.Fatalf("NewStorageWithContext failed: %v", err)
			}

			// 使用
			_ = s.Datastore()

			// 关闭
			_ = s.CloseWithContext(ctx)

			b.StopTimer()
			s.Destroy()
			os.RemoveAll(tmpDir)
		}
	})

	b.Run("ConcurrentAccess", func(b *testing.B) {
		tmpDir, err := os.MkdirTemp("", "bench-real-concurrent-*")
		if err != nil {
			b.Fatalf("Failed to create temp dir: %v", err)
		}
		s, err := NewStorage(tmpDir)
		if err != nil {
			os.RemoveAll(tmpDir)
			b.Fatalf("NewStorage failed: %v", err)
		}
		defer s.Destroy()
		defer os.RemoveAll(tmpDir)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			for g := 0; g < 10; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 10; j++ {
						_ = s.Datastore()
					}
				}()
			}
			wg.Wait()
		}
	})
}
