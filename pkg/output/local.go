package output

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

// LocalWriter writes Parquet files to local disk in Hive-style partitions:
//
//	{basePath}/{dataType}/instance_id={id}/date={YYYY-MM-DD}/chunk_NNN.parquet
type LocalWriter struct {
	basePath string
	mu       sync.Mutex
	seq      int
}

// NewLocalWriter creates a writer that outputs Parquet files to the given directory.
func NewLocalWriter(basePath string) (*LocalWriter, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("creating output directory %s: %w", basePath, err)
	}
	return &LocalWriter{basePath: basePath}, nil
}

// WriteRows writes typed rows to a new Parquet file in the appropriate partition.
// Each call creates a complete, self-contained Parquet file.
// File layout: {base}/{dataType}/instance_id={id}/date={YYYY-MM-DD}/chunk_NNNNNN.parquet
func WriteRows[T any](w *LocalWriter, dataType, instanceID string, rows []T) error {
	if len(rows) == 0 {
		return nil
	}

	now := time.Now().UTC()
	date := now.Format("2006-01-02")

	w.mu.Lock()
	w.seq++
	seq := w.seq
	w.mu.Unlock()

	// Create partition directory.
	dir := filepath.Join(w.basePath, dataType,
		fmt.Sprintf("instance_id=%s", instanceID),
		fmt.Sprintf("date=%s", date))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating partition dir %s: %w", dir, err)
	}

	// Write a complete Parquet file (header + rows + footer).
	path := filepath.Join(dir, fmt.Sprintf("chunk_%06d.parquet", seq))
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating parquet file %s: %w", path, err)
	}
	defer f.Close()

	pw := parquet.NewGenericWriter[T](f)
	if _, err := pw.Write(rows); err != nil {
		return fmt.Errorf("writing %d rows to %s: %w", len(rows), path, err)
	}
	if err := pw.Close(); err != nil {
		return fmt.Errorf("closing parquet writer for %s: %w", path, err)
	}

	return nil
}

// Flush is a no-op for LocalWriter since each WriteRows creates a complete file.
func (w *LocalWriter) Flush() error {
	return nil
}

// Close flushes and releases all resources.
func (w *LocalWriter) Close() error {
	return w.Flush()
}
