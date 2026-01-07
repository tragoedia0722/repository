package extractor

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tragoedia0722/repository/pkg/helper"
)

// BenchmarkNewExtractor 测试 Extractor 创建性能
func BenchmarkNewExtractor(b *testing.B) {
	paths := []struct {
		name string
		path string
	}{
		{"Simple", "/simple/path"},
		{"Deep", "/path/with/many/levels/and/subdirectories"},
		{"Special", "/path/with/special/chars"},
		{"Unicode", "/path/with/测试/文件"},
		{"Reserved", "/path/CON/PRN/AUX"},
	}

	for _, p := range paths {
		b.Run(p.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = NewExtractor(nil, "QmTest", p.path)
			}
		})
	}
}

// BenchmarkPathCleaning 测试路径清理性能
func BenchmarkPathCleaning(b *testing.B) {
	paths := []struct {
		name string
		path string
	}{
		{"Normal", "/normal/path"},
		{"Special", "/path/with/<>:\"/\\|?*chars"},
		{"Deep", "/very/deep/path/with/many/levels"},
		{"Unicode", "/path/with/测试/file"},
		{"Dots", "/path/../with/./duplicated/slashes//"},
		{"Reserved", "/CON/PRN/AUX/com1/lpt9"},
	}

	for _, p := range paths {
		b.Run(p.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				parts := strings.Split(p.path, string(filepath.Separator))
				for _, part := range parts {
					if part != "" {
						_ = helper.CleanFilename(part)
					}
				}
			}
		})
	}
}

// BenchmarkIsSubPath 测试路径检查性能
func BenchmarkIsSubPath(b *testing.B) {
	ext := &Extractor{}
	tests := []struct {
		name string
		path string
		base string
	}{
		{"Same", "/path/to/dir", "/path/to/dir"},
		{"SubPath", "/path/to/dir/sub", "/path/to/dir"},
		{"NotSubPath", "/other/path", "/path/to/dir"},
		{"DeepSub", "/path/to/dir/deep/nested/path", "/path/to/dir"},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = ext.isSubPath(tt.path, tt.base)
			}
		})
	}
}

// BenchmarkBufferPool 测试缓冲池性能
func BenchmarkBufferPool(b *testing.B) {
	pool := &Extractor{
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, defaultWriteBufferSize)
			},
		},
	}

	b.Run("WithPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := pool.bufferPool.Get().([]byte)
			buf[0] = 'x'
			pool.bufferPool.Put(buf)
		}
	})

	b.Run("WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := make([]byte, defaultWriteBufferSize)
			buf[0] = 'x'
		}
	})
}

// BenchmarkCleanFilenameVariants 测试不同场景的文件名清理性能
func BenchmarkCleanFilenameVariants(b *testing.B) {
	cases := []struct {
		name  string
		input string
	}{
		{"Simple", "normal_file.txt"},
		{"Invalid", `test<>:"/\|?*file.txt`},
		{"Unicode", "测试文件.txt"},
		{"Spaces", "file   name   test.txt"},
		{"Dots", "file...name...test.txt"},
		{"Reserved", "CON.txt"},
		{"Long", strings.Repeat("x", 300) + ".txt"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = helper.CleanFilename(tc.input)
			}
		})
	}
}

// TestExtractorPerformance 性能测试辅助函数
func TestExtractorPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// 测试构造函数性能
	t.Run("Constructor", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < 10000; i++ {
			_ = NewExtractor(nil, "QmTest", "/test/path")
		}
		elapsed := time.Since(start)
		t.Logf("10,000 constructors in %v (%.2f μs/op)", elapsed, float64(elapsed.Microseconds())/10000)
	})

	// 测试路径清理性能
	t.Run("PathCleaning", func(t *testing.T) {
		path := "/path/with/many/levels/and/测试/files"
		start := time.Now()
		for i := 0; i < 1000; i++ {
			parts := strings.Split(path, string(filepath.Separator))
			for _, part := range parts {
				if part != "" {
					_ = helper.CleanFilename(part)
				}
			}
		}
		elapsed := time.Since(start)
		t.Logf("1,000 path cleanings in %v (%.2f μs/op)", elapsed, float64(elapsed.Microseconds())/1000)
	})

	// 测试缓冲池性能
	t.Run("BufferPool", func(t *testing.T) {
		pool := &Extractor{
			bufferPool: sync.Pool{
				New: func() interface{} {
					return make([]byte, defaultWriteBufferSize)
				},
			},
		}

		iterations := 10000
		start := time.Now()
		for i := 0; i < iterations; i++ {
			buf := pool.bufferPool.Get().([]byte)
			buf[0] = 'x'
			pool.bufferPool.Put(buf)
		}
		elapsed := time.Since(start)
		nsPerOp := float64(elapsed.Nanoseconds()) / float64(iterations)
		t.Logf("%d buffer pool operations in %v (%.0f ns/op)", iterations, elapsed, nsPerOp)
	})
}

// BenchmarkWriteToFile 模拟文件写入性能
func BenchmarkWriteToFile(b *testing.B) {
	b.Run("SmallFile", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tmpfile, err := os.CreateTemp("", "bench")
			if err != nil {
				b.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())

			data := make([]byte, 1024) // 1KB
			tmpfile.Write(data)
			tmpfile.Close()
		}
	})
}
