package extractor

const (
	// defaultWriteBufferSize is the default buffer size for writing files (4MB)
	defaultWriteBufferSize = 4 * 1024 * 1024

	// dirPermissions is the default permissions for directories (rwxr-xr-x)
	dirPermissions = 0o755

	// filePermissions is the default permissions for files (rw-r--r--)
	filePermissions = 0o644

	// defaultCleanedDirName is the default name used when a directory component
	// cannot be cleaned to a valid filename
	defaultCleanedDirName = "cleaned_dir"

	// partFileSuffix is the suffix used for temporary files during atomic writes
	partFileSuffix = ".part"

	// progressUpdateThreshold is the minimum number of bytes that must be
	// read before triggering a progress callback update (256KB).
	// This reduces callback frequency from ~250K to ~4K calls per GB.
	progressUpdateThreshold = 256 * 1024
)
