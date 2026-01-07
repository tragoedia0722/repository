package helper

import (
	"fmt"
	"strings"
	"testing"
)

// Benchmark for CleanFilename with various input types
func BenchmarkCleanFilename_Simple(b *testing.B) {
	filename := "simple_filename.txt"
	for i := 0; i < b.N; i++ {
		CleanFilename(filename)
	}
}

func BenchmarkCleanFilename_WithInvalidChars(b *testing.B) {
	filename := `test<>:"/\|?*file.txt`
	for i := 0; i < b.N; i++ {
		CleanFilename(filename)
	}
}

func BenchmarkCleanFilename_WithUnicode(b *testing.B) {
	filename := "test\u200E\u200F\u00A0\u3000file测试.txt"
	for i := 0; i < b.N; i++ {
		CleanFilename(filename)
	}
}

func BenchmarkCleanFilename_LongFilename(b *testing.B) {
	filename := strings.Repeat("a", 200) + ".txt"
	for i := 0; i < b.N; i++ {
		CleanFilename(filename)
	}
}

func BenchmarkCleanFilename_ReservedName(b *testing.B) {
	filename := "CON.txt"
	for i := 0; i < b.N; i++ {
		CleanFilename(filename)
	}
}

func BenchmarkCleanFilename_NeedsTruncation(b *testing.B) {
	filename := strings.Repeat("a", 300) + ".txt"
	for i := 0; i < b.N; i++ {
		CleanFilename(filename)
	}
}

// Benchmark individual operations
func BenchmarkHandleReservedNames(b *testing.B) {
	filename := "CON.txt"
	for i := 0; i < b.N; i++ {
		HandleReservedNames(filename)
	}
}

func BenchmarkTruncateFilename(b *testing.B) {
	filename := strings.Repeat("a", 300) + ".txt"
	for i := 0; i < b.N; i++ {
		TruncateFilename(filename, 255)
	}
}

// Memory allocation benchmarks
func BenchmarkCleanFilename_Allocs(b *testing.B) {
	filename := `test<>:"/\|?*file测试\u200E\u200F.txt`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		CleanFilename(filename)
	}
}

// Compare different scenarios
func BenchmarkScenarios(b *testing.B) {
	scenarios := []struct {
		name     string
		filename string
	}{
		{"simple", "normal_file.txt"},
		{"invalid_chars", `test<>:"/\|?*file.txt`},
		{"unicode_spaces", "test\u00A0\u3000file.txt"},
		{"reserved", "CON.txt"},
		{"long", strings.Repeat("x", 300) + ".txt"},
		{"complex", `test<>:"/\|?*file\u200E\u200F\u00A0.txt`},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				CleanFilename(scenario.filename)
			}
		})
	}
}

// Test throughput for batch operations
func BenchmarkBatchClean(b *testing.B) {
	filenames := make([]string, 1000)
	for i := range filenames {
		filenames[i] = fmt.Sprintf("test_file_%d<>:\"/\\|?*.txt", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, f := range filenames {
			CleanFilename(f)
		}
	}
}
