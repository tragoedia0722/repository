package importer

const (
	// Cache management
	liveCacheSize = uint64(256 << 10) // 256K nodes max in memory before flushing

	// Chunking configuration
	chunkSize = 1024 * 1024 // 1MB chunks for file splitting

	// Batch processing
	defaultBatchSize = 100 << 20 // 100MB batch size for buffered DAG operations

	// Package configuration
	blocksPerPackage     = 100 // Max blocks per package
	initialBlockCapacity = 100 // Pre-allocated block slice capacity
	cidStringBufferSize  = 64  // Estimated CID string length for hash builder

	// Default names
	defaultFileName = "unnamed_file"
	defaultDirName  = "unnamed_directory"
)
