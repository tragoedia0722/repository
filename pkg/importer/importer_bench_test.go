package importer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	"github.com/tragoedia0722/repository/pkg/repository"
)

// createBenchmarkBlockstore creates a blockstore for benchmarking
func createBenchmarkBlockstore(b *testing.B) (blockstore.Blockstore, func()) {
	tmpDir, err := os.MkdirTemp("", "importer-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}

	repo, err := repository.NewRepository(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("failed to create repository: %v", err)
	}

	cleanup := func() {
		repo.Close()
		os.RemoveAll(tmpDir)
	}

	return repo.BlockStore(), cleanup
}

// Benchmark_Import_SmallFile benchmarks importing a small file (1MB)
func Benchmark_Import_SmallFile(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "bench-small-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create 1MB test file
	testFile := filepath.Join(tmpDir, "test_1mb.bin")
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, data, 0o644); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		imp := NewImporter(bs, testFile)
		_, err := imp.Import(context.Background())
		if err != nil {
			b.Fatalf("Import failed: %v", err)
		}
	}
}

// Benchmark_Import_MediumFile benchmarks importing a medium file (10MB)
func Benchmark_Import_MediumFile(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "bench-medium-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create 10MB test file
	testFile := filepath.Join(tmpDir, "test_10mb.bin")
	data := make([]byte, 10*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, data, 0o644); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		imp := NewImporter(bs, testFile)
		_, err := imp.Import(context.Background())
		if err != nil {
			b.Fatalf("Import failed: %v", err)
		}
	}
}

// Benchmark_Import_LargeFile benchmarks importing a large file (100MB)
func Benchmark_Import_LargeFile(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large file benchmark in short mode")
	}

	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "bench-large-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create 100MB test file
	testFile := filepath.Join(tmpDir, "test_100mb.bin")
	data := make([]byte, 100*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, data, 0o644); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		imp := NewImporter(bs, testFile)
		_, err := imp.Import(context.Background())
		if err != nil {
			b.Fatalf("Import failed: %v", err)
		}
	}
}

// Benchmark_Import_ManySmallFiles benchmarks importing many small files
func Benchmark_Import_ManySmallFiles(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "bench-many-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create 100 small files (1KB each)
	for i := 0; i < 100; i++ {
		fileName := filepath.Join(tmpDir, filepath.Join("subdir", "file", "test"))
		if err := os.MkdirAll(filepath.Dir(fileName), 0o755); err != nil {
			b.Fatal(err)
		}
		data := make([]byte, 1024)
		if err := os.WriteFile(fileName, data, 0o644); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		imp := NewImporter(bs, tmpDir)
		_, err := imp.Import(context.Background())
		if err != nil {
			b.Fatalf("Import failed: %v", err)
		}
	}
}

// Benchmark_Import_WithProgress benchmarks importing with progress tracking
func Benchmark_Import_WithProgress(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "bench-progress-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create 10MB test file
	testFile := filepath.Join(tmpDir, "test_10mb.bin")
	data := make([]byte, 10*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, data, 0o644); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		imp := NewImporter(bs, testFile)
		progressCalls := 0
		imp.WithProgress(func(completed, total int64, currentFile string) {
			progressCalls++
		})
		_, err := imp.Import(context.Background())
		if err != nil {
			b.Fatalf("Import failed: %v", err)
		}
	}
}

// Benchmark_calcPackage benchmarks package hash calculation
func Benchmark_calcPackage(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	imp := NewImporter(bs, "/test/path")

	// Create mock blocks (typical CID strings)
	blocks := make([]string, 100)
	for i := 0; i < 100; i++ {
		blocks[i] = "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = imp.calcPackage(blocks)
	}
}

// Benchmark_calcPackage_Small benchmarks package hash calculation with few blocks
func Benchmark_calcPackage_Small(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	imp := NewImporter(bs, "/test/path")

	blocks := []string{
		"QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo",
		"QmNZJiFADfZe42S8AoGi8zjFQSGpg4YkNNKTmQZyeyt7qD",
		"QmPVYMLuEyUQSmpoBfD2QyWYtLKCM8348t3PkgT2p1YGk3",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = imp.calcPackage(blocks)
	}
}

// Benchmark_calcPackage_Large benchmarks package hash calculation with many blocks
func Benchmark_calcPackage_Large(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	imp := NewImporter(bs, "/test/path")

	// Large package (1000 blocks)
	blocks := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		blocks[i] = "QmY8YgM11EA5ai1mAEzFZGB2D2FkGTWke1vUgTxBQxLsXo"
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = imp.calcPackage(blocks)
	}
}

// Benchmark_sliceDirectory_File benchmarks file slicing
func Benchmark_sliceDirectory_File(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "bench-slice-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.txt")
	data := make([]byte, 1024*1024)
	if err := os.WriteFile(testFile, data, 0o644); err != nil {
		b.Fatal(err)
	}

	imp := NewImporter(bs, testFile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := imp.sliceDirectory(testFile)
		if err != nil {
			b.Fatalf("sliceDirectory failed: %v", err)
		}
	}
}

// Benchmark_sliceDirectory_Directory benchmarks directory slicing
func Benchmark_sliceDirectory_Directory(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "bench-slice-dir-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create directory with files
	for i := 0; i < 50; i++ {
		fileName := filepath.Join(tmpDir, "file", "test.txt")
		if err := os.MkdirAll(filepath.Dir(fileName), 0o755); err != nil {
			b.Fatal(err)
		}
		data := make([]byte, 1024)
		if err := os.WriteFile(fileName, data, 0o644); err != nil {
			b.Fatal(err)
		}
	}

	imp := NewImporter(bs, tmpDir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := imp.sliceDirectory(tmpDir)
		if err != nil {
			b.Fatalf("sliceDirectory failed: %v", err)
		}
	}
}

// Benchmark_NewImporter benchmarks importer creation
func Benchmark_NewImporter(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewImporter(bs, "/test/path")
	}
}

// Benchmark_WithProgress benchmarks progress callback setup
func Benchmark_WithProgress(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	imp := NewImporter(bs, "/test/path")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = imp.WithProgress(func(completed, total int64, currentFile string) {
			// No-op
		})
	}
}

// Benchmark_updateProgress benchmarks progress update performance
func Benchmark_updateProgress(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	imp := NewImporter(bs, "/test/path")
	imp.WithProgress(func(completed, total int64, currentFile string) {
		// No-op callback
	})
	imp.tracker = newProgressTracker(100*1024*1024, imp.progress) // 100MB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		imp.updateProgress(1024, "test.txt")
	}
}

// Benchmark_ProgressReader benchmarks progress reader overhead
func Benchmark_ProgressReader(b *testing.B) {
	b.Run("WithoutProgress", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			pr := &progressReader{
				reader:     nil, // Will be replaced in actual benchmark
				onProgress: nil,
			}
			// Simulate read without actual I/O
			_ = pr
		}
	})

	b.Run("WithProgress", func(b *testing.B) {
		callCount := 0
		pr := &progressReader{
			reader:     nil,
			onProgress: func(n int64) { callCount++ },
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Simulate progress callback
			if pr.onProgress != nil {
				pr.onProgress(1024)
			}
		}
	})
}

// Benchmark_Import_EmptyFile benchmarks empty file import
func Benchmark_Import_EmptyFile(b *testing.B) {
	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "bench-empty-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "empty.txt")
	if err := os.WriteFile(testFile, []byte{}, 0o644); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		imp := NewImporter(bs, testFile)
		_, err := imp.Import(context.Background())
		if err != nil {
			b.Fatalf("Import failed: %v", err)
		}
	}
}

// Benchmark_Import_VeryLargeFile benchmarks very large file import (1GB)
func Benchmark_Import_VeryLargeFile(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping very large file benchmark in short mode")
	}

	bs, cleanup := createBenchmarkBlockstore(b)
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "bench-xl-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create 1GB test file
	testFile := filepath.Join(tmpDir, "test_1gb.bin")
	data := make([]byte, 1024*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, data, 0o644); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	// Only run once for very large files
	b.N = 1

	imp := NewImporter(bs, testFile)
	_, err = imp.Import(context.Background())
	if err != nil {
		b.Fatalf("Import failed: %v", err)
	}
}
