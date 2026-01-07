package extractor

import (
	"sync/atomic"
)

// progressCallback is called periodically during extraction to report progress.
// Parameters: completed bytes, total bytes, current file being extracted
type progressCallback func(completed, total int64, currentFile string)

// progressTracker tracks extraction progress and handles concurrency-safe state
type progressTracker struct {
	totalBytes     int64            // Total bytes to extract
	completedBytes atomic.Int64     // Bytes extracted so far
	isInterrupted  atomic.Int32     // 1 if extraction is interrupted, 0 otherwise
	callback       progressCallback // Optional callback for progress updates
}

// newProgressTracker creates a new progress tracker
func newProgressTracker(totalBytes int64, callback progressCallback) *progressTracker {
	return &progressTracker{
		totalBytes: totalBytes,
		callback:   callback,
	}
}

// setTotal updates the total bytes to extract (atomic operation)
func (pt *progressTracker) setTotal(totalBytes int64) {
	atomic.StoreInt64(&pt.totalBytes, totalBytes)
}

// update adds the specified number of bytes to the completed count and triggers
// the progress callback if one is registered.
func (pt *progressTracker) update(bytes int64, filename string) {
	completed := pt.completedBytes.Add(bytes)
	if pt.callback != nil {
		total := atomic.LoadInt64(&pt.totalBytes)
		pt.callback(completed, total, filename)
	}
}

// getTotal returns the total bytes to extract
func (pt *progressTracker) getTotal() int64 {
	return pt.totalBytes
}

// getCompleted returns the number of bytes extracted so far
func (pt *progressTracker) getCompleted() int64 {
	return pt.completedBytes.Load()
}

// interrupt flags the extraction as interrupted
func (pt *progressTracker) interrupt() {
	pt.isInterrupted.Store(1)
}

// isSet returns true if the extraction has been interrupted
func (pt *progressTracker) isSet() bool {
	return pt.isInterrupted.Load() == 1
}

// reset clears the interrupted flag
func (pt *progressTracker) reset() {
	pt.isInterrupted.Store(0)
}

// Test helper methods

// getCallback returns the progress callback (for testing)
func (pt *progressTracker) getCallback() progressCallback {
	return pt.callback
}

// hasCallback returns true if a callback is registered (for testing)
func (pt *progressTracker) hasCallback() bool {
	return pt.callback != nil
}
