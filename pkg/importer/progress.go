package importer

import (
	"sync/atomic"
)

// progressCallback reports import progress
type progressCallback func(completed, total int64, currentFile string)

// progressTracker manages progress tracking and interruption state atomically
type progressTracker struct {
	processedSize atomic.Int64 // Total bytes processed
	totalSize     int64        // Total bytes to process (set once, never modified)
	isInterrupted atomic.Int32 // 1 = interrupted, 0 = running (atomic flag)
	callback      progressCallback
}

// newProgressTracker creates a new progress tracker with the given total size and callback
func newProgressTracker(totalSize int64, callback progressCallback) *progressTracker {
	return &progressTracker{
		totalSize: totalSize,
		callback:  callback,
	}
}

// update adds bytes to processed count and triggers callback if set
func (pt *progressTracker) update(size int64, filename string) {
	completed := pt.processedSize.Add(size)
	if pt.callback != nil {
		pt.callback(completed, pt.totalSize, filename)
	}
}

// interrupt flags the import as interrupted using atomic operation
func (pt *progressTracker) interrupt() {
	pt.isInterrupted.Store(1)
}

// checkInterrupted returns true if import was interrupted (atomic read)
func (pt *progressTracker) checkInterrupted() bool {
	return pt.isInterrupted.Load() == 1
}

// getProcessed returns the current processed size (atomic read)
func (pt *progressTracker) getProcessed() int64 {
	return pt.processedSize.Load()
}
