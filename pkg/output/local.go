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
	maxRows  int
	mu       sync.Mutex
	buffers  map[bufferKey]*fileBuffer
}

type bufferKey struct {
	dataType   string
	instanceID string
	date       string
}

type fileBuffer struct {
	path  string
	file  *os.File
	rows  int
	chunk int
}

// NewLocalWriter creates a writer that outputs Parquet files to the given directory.
func NewLocalWriter(basePath string) (*LocalWriter, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("creating output directory %s: %w", basePath, err)
	}
	return &LocalWriter{
		basePath: basePath,
		maxRows:  100_000,
		buffers:  make(map[bufferKey]*fileBuffer),
	}, nil
}

// WriteRows writes typed rows to the appropriate Parquet partition.
// This is the generic entry point used by the collector for all data types.
func WriteRows[T any](w *LocalWriter, dataType, instanceID string, rows []T) error {
	if len(rows) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	date := time.Now().UTC().Format("2006-01-02")
	key := bufferKey{dataType: dataType, instanceID: instanceID, date: date}

	buf, ok := w.buffers[key]
	if !ok {
		buf = &fileBuffer{chunk: 1}
		w.buffers[key] = buf
	}

	// Rotate if over max rows or file not yet opened.
	if buf.file == nil || buf.rows >= w.maxRows {
		if buf.file != nil {
			buf.file.Close()
			buf.chunk++
			buf.rows = 0
		}

		dir := filepath.Join(w.basePath, dataType,
			fmt.Sprintf("instance_id=%s", instanceID),
			fmt.Sprintf("date=%s", date))
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating partition dir %s: %w", dir, err)
		}

		path := filepath.Join(dir, fmt.Sprintf("chunk_%03d.parquet", buf.chunk))
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("creating parquet file %s: %w", path, err)
		}
		buf.file = f
		buf.path = path
	}

	// Write rows using parquet-go.
	pw := parquet.NewGenericWriter[T](buf.file)
	if _, err := pw.Write(rows); err != nil {
		return fmt.Errorf("writing %d rows to %s: %w", len(rows), buf.path, err)
	}
	if err := pw.Close(); err != nil {
		return fmt.Errorf("closing parquet writer for %s: %w", buf.path, err)
	}

	buf.rows += len(rows)
	return nil
}

// Flush closes all open Parquet files, finalizing their footers.
func (w *LocalWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	for key, buf := range w.buffers {
		if buf.file != nil {
			if err := buf.file.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("closing %s: %w", buf.path, err)
			}
		}
		delete(w.buffers, key)
	}
	return firstErr
}

// Close flushes and releases all resources.
func (w *LocalWriter) Close() error {
	return w.Flush()
}
