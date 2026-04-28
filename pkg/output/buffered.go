package output

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unsafe"

	"github.com/scaledb-io/scaledb/pkg/model"
)

// BufferedWriter accumulates rows in memory and flushes to an underlying
// DataWriter when a configurable trigger fires (row count, estimated size,
// or time interval). Time-based flushing is driven externally by the caller
// invoking Flush() on a ticker — BufferedWriter does not start its own goroutine.
type BufferedWriter struct {
	sink       DataWriter
	flushRows  int   // -1 = disabled
	flushSize  int64 // bytes, -1 = disabled
	logger     *slog.Logger
	mu         sync.Mutex
	metrics    map[string][]model.Metric
	digests    map[string][]model.QueryDigest
	indexUsage map[string][]model.IndexUsage
	samples    map[string][]model.QuerySample
	waitEvents map[string][]model.WaitEventSummary
}

// BufferedWriterConfig holds flush trigger configuration.
type BufferedWriterConfig struct {
	FlushRows int   // flush when total rows for an instance exceeds this; -1 = disabled
	FlushSize int64 // flush when estimated bytes for an instance exceeds this; -1 = disabled
}

// NewBufferedWriter wraps a DataWriter with in-memory buffering.
func NewBufferedWriter(sink DataWriter, cfg BufferedWriterConfig, logger *slog.Logger) *BufferedWriter {
	return &BufferedWriter{
		sink:       sink,
		flushRows:  cfg.FlushRows,
		flushSize:  cfg.FlushSize,
		logger:     logger,
		metrics:    make(map[string][]model.Metric),
		digests:    make(map[string][]model.QueryDigest),
		indexUsage: make(map[string][]model.IndexUsage),
		samples:    make(map[string][]model.QuerySample),
		waitEvents: make(map[string][]model.WaitEventSummary),
	}
}

func (w *BufferedWriter) WriteMetrics(instanceID string, rows []model.Metric) error {
	if len(rows) == 0 {
		return nil
	}
	w.mu.Lock()
	w.metrics[instanceID] = append(w.metrics[instanceID], rows...)
	w.mu.Unlock()
	return w.checkAndFlush(instanceID)
}

func (w *BufferedWriter) WriteDigests(instanceID string, rows []model.QueryDigest) error {
	if len(rows) == 0 {
		return nil
	}
	w.mu.Lock()
	w.digests[instanceID] = append(w.digests[instanceID], rows...)
	w.mu.Unlock()
	return w.checkAndFlush(instanceID)
}

func (w *BufferedWriter) WriteIndexUsage(instanceID string, rows []model.IndexUsage) error {
	if len(rows) == 0 {
		return nil
	}
	w.mu.Lock()
	w.indexUsage[instanceID] = append(w.indexUsage[instanceID], rows...)
	w.mu.Unlock()
	return w.checkAndFlush(instanceID)
}

func (w *BufferedWriter) WriteQuerySamples(instanceID string, rows []model.QuerySample) error {
	if len(rows) == 0 {
		return nil
	}
	w.mu.Lock()
	w.samples[instanceID] = append(w.samples[instanceID], rows...)
	w.mu.Unlock()
	return w.checkAndFlush(instanceID)
}

func (w *BufferedWriter) WriteWaitEvents(instanceID string, rows []model.WaitEventSummary) error {
	if len(rows) == 0 {
		return nil
	}
	w.mu.Lock()
	w.waitEvents[instanceID] = append(w.waitEvents[instanceID], rows...)
	w.mu.Unlock()
	return w.checkAndFlush(instanceID)
}

// checkAndFlush checks row count and size triggers for an instance.
func (w *BufferedWriter) checkAndFlush(instanceID string) error {
	if flush, reason := w.shouldFlush(instanceID); flush {
		return w.flushInstance(instanceID, reason)
	}
	return nil
}

// shouldFlush checks whether the buffer for an instance has exceeded any trigger.
func (w *BufferedWriter) shouldFlush(instanceID string) (bool, string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	totalRows := len(w.metrics[instanceID]) +
		len(w.digests[instanceID]) +
		len(w.indexUsage[instanceID]) +
		len(w.samples[instanceID]) +
		len(w.waitEvents[instanceID])

	if w.flushRows > 0 && totalRows >= w.flushRows {
		return true, "rows"
	}

	if w.flushSize > 0 {
		est := w.estimateBytes(instanceID)
		if est >= w.flushSize {
			return true, "size"
		}
	}

	return false, ""
}

// estimateBytes returns a rough estimate of memory used by the buffers for an instance.
// Must be called with w.mu held.
func (w *BufferedWriter) estimateBytes(instanceID string) int64 {
	var total int64
	total += int64(len(w.metrics[instanceID])) * int64(unsafe.Sizeof(model.Metric{}))
	total += int64(len(w.digests[instanceID])) * int64(unsafe.Sizeof(model.QueryDigest{}))
	total += int64(len(w.indexUsage[instanceID])) * int64(unsafe.Sizeof(model.IndexUsage{}))
	total += int64(len(w.samples[instanceID])) * int64(unsafe.Sizeof(model.QuerySample{}))
	total += int64(len(w.waitEvents[instanceID])) * int64(unsafe.Sizeof(model.WaitEventSummary{}))

	// Strings in Go structs are header-only (16 bytes); add rough estimate for string data.
	// Average ~50 bytes per string field across all types.
	stringFields := len(w.metrics[instanceID])*5 +
		len(w.digests[instanceID])*16 +
		len(w.indexUsage[instanceID])*8 +
		len(w.samples[instanceID])*14 +
		len(w.waitEvents[instanceID])*7
	total += int64(stringFields) * 50

	return total
}

// flushInstance drains all buffers for a single instance to the underlying sink.
func (w *BufferedWriter) flushInstance(instanceID, reason string) error {
	w.mu.Lock()
	metrics := w.metrics[instanceID]
	digests := w.digests[instanceID]
	indexUsage := w.indexUsage[instanceID]
	samples := w.samples[instanceID]
	waitEvents := w.waitEvents[instanceID]

	// Clear buffers while holding lock.
	delete(w.metrics, instanceID)
	delete(w.digests, instanceID)
	delete(w.indexUsage, instanceID)
	delete(w.samples, instanceID)
	delete(w.waitEvents, instanceID)
	w.mu.Unlock()

	totalRows := len(metrics) + len(digests) + len(indexUsage) + len(samples) + len(waitEvents)
	if totalRows == 0 {
		return nil
	}

	var errs []error
	if len(metrics) > 0 {
		if err := w.sink.WriteMetrics(instanceID, metrics); err != nil {
			errs = append(errs, fmt.Errorf("metrics: %w", err))
		}
	}
	if len(digests) > 0 {
		if err := w.sink.WriteDigests(instanceID, digests); err != nil {
			errs = append(errs, fmt.Errorf("digests: %w", err))
		}
	}
	if len(indexUsage) > 0 {
		if err := w.sink.WriteIndexUsage(instanceID, indexUsage); err != nil {
			errs = append(errs, fmt.Errorf("index-usage: %w", err))
		}
	}
	if len(samples) > 0 {
		if err := w.sink.WriteQuerySamples(instanceID, samples); err != nil {
			errs = append(errs, fmt.Errorf("samples: %w", err))
		}
	}
	if len(waitEvents) > 0 {
		if err := w.sink.WriteWaitEvents(instanceID, waitEvents); err != nil {
			errs = append(errs, fmt.Errorf("wait-events: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("flushing instance %s: %v", instanceID, errs)
	}

	w.logger.Info("flush triggered",
		"reason", reason,
		"instance", instanceID,
		"rows", totalRows,
	)
	return nil
}

// Flush drains all buffered data to the underlying sink (time-based trigger).
func (w *BufferedWriter) Flush() error {
	w.mu.Lock()
	instances := make(map[string]struct{})
	for id := range w.metrics {
		instances[id] = struct{}{}
	}
	for id := range w.digests {
		instances[id] = struct{}{}
	}
	for id := range w.indexUsage {
		instances[id] = struct{}{}
	}
	for id := range w.samples {
		instances[id] = struct{}{}
	}
	for id := range w.waitEvents {
		instances[id] = struct{}{}
	}
	w.mu.Unlock()

	var errs []error
	for id := range instances {
		if err := w.flushInstance(id, "interval"); err != nil {
			errs = append(errs, err)
		}
	}

	if err := w.sink.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("sink flush: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("flush: %v", errs)
	}
	return nil
}

// Close flushes all remaining data and closes the underlying sink.
func (w *BufferedWriter) Close() error {
	if err := w.Flush(); err != nil {
		// Log but continue to close the sink.
		w.logger.Warn("flush on close failed", "error", err)
	}
	return w.sink.Close()
}

// Stats returns the current buffer state for monitoring/testing.
func (w *BufferedWriter) Stats() (instances int, totalRows int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	seen := make(map[string]struct{})
	for id := range w.metrics {
		seen[id] = struct{}{}
	}
	for id := range w.digests {
		seen[id] = struct{}{}
	}
	for id := range w.indexUsage {
		seen[id] = struct{}{}
	}
	for id := range w.samples {
		seen[id] = struct{}{}
	}
	for id := range w.waitEvents {
		seen[id] = struct{}{}
	}

	for id := range seen {
		totalRows += len(w.metrics[id]) +
			len(w.digests[id]) +
			len(w.indexUsage[id]) +
			len(w.samples[id]) +
			len(w.waitEvents[id])
	}

	return len(seen), totalRows
}

// Ensure BufferedWriter satisfies DataWriter at compile time.
var _ DataWriter = (*BufferedWriter)(nil)

// flushTickerDuration is used by the collector to determine the flush interval.
// Exposed here so tests can verify the value.
func FlushTickerDuration(interval time.Duration) time.Duration {
	if interval < 0 {
		// Time-based flush disabled; return a long duration so the ticker exists
		// but practically never fires.
		return 24 * time.Hour
	}
	return interval
}
