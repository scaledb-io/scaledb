// Package compact merges older Parquet chunk files into fewer, larger files.
//
// The scaledb collector writes Parquet files in Hive-style partitions:
//
//	{base}/{dataType}/instance_id={id}/date={YYYY-MM-DD}/chunk_NNNNNN.parquet
//
// Over time, many flush-sized files accumulate per partition. A query that
// scans 90 days of metrics opens hundreds of files — DuckDB / Athena queries
// slow down even when individual file sizes are healthy.
//
// Compact walks the data tree and rewrites old partitions into fewer, larger
// files named compacted_NNNN.parquet. Partitions newer than --older-than are
// skipped to avoid racing with the live collector.
package compact

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// Options configures a compaction run.
type Options struct {
	// TargetSize is the target output file size in bytes (default 512 MiB).
	// A new output file is started when the previous one meets or exceeds this.
	TargetSize int64

	// OlderThan skips partitions whose date is within this duration of now.
	// Default is 7 days — protects partitions that the collector may still
	// be writing to.
	OlderThan time.Duration

	// DataTypes restricts compaction to specific data types
	// (e.g. "metrics", "query-digests"). Empty means all types.
	DataTypes []string

	// DryRun reports what would be compacted without modifying any files.
	DryRun bool
}

// PartitionResult describes the outcome for a single (dataType, instance, date) partition.
type PartitionResult struct {
	DataType   string
	InstanceID string
	Date       string
	InputFiles int
	InputBytes int64
	// OutputFiles and OutputBytes are 0 for skipped/dry-run partitions.
	OutputFiles int
	OutputBytes int64
	Skipped     bool
	SkipReason  string
}

// Report summarises a compaction run.
type Report struct {
	Partitions  []PartitionResult
	TotalIn     int64
	TotalOut    int64
	FilesIn     int
	FilesOut    int
	PartSkipped int
	DryRun      bool
}

const (
	defaultTargetSize = 512 * 1024 * 1024 // 512 MiB
	defaultOlderThan  = 7 * 24 * time.Hour
)

// Compact walks basePath and compacts old Parquet partitions according to opts.
func Compact(ctx context.Context, basePath string, opts Options) (*Report, error) {
	if opts.TargetSize <= 0 {
		opts.TargetSize = defaultTargetSize
	}
	if opts.OlderThan <= 0 {
		opts.OlderThan = defaultOlderThan
	}

	cutoff := time.Now().UTC().Add(-opts.OlderThan)
	allowedTypes := make(map[string]bool, len(opts.DataTypes))
	for _, dt := range opts.DataTypes {
		allowedTypes[dt] = true
	}

	report := &Report{DryRun: opts.DryRun}

	// Enumerate data-type directories directly under basePath.
	dataTypeDirs, err := os.ReadDir(basePath)
	if err != nil {
		return nil, fmt.Errorf("reading base path %s: %w", basePath, err)
	}

	for _, dtEntry := range dataTypeDirs {
		if !dtEntry.IsDir() {
			continue
		}
		dataType := dtEntry.Name()
		if len(allowedTypes) > 0 && !allowedTypes[dataType] {
			continue
		}

		dataTypeDir := filepath.Join(basePath, dataType)
		// Walk instance_id=* directories.
		instanceDirs, err := os.ReadDir(dataTypeDir)
		if err != nil {
			return nil, fmt.Errorf("reading data type dir %s: %w", dataTypeDir, err)
		}

		for _, instEntry := range instanceDirs {
			if !instEntry.IsDir() || !strings.HasPrefix(instEntry.Name(), "instance_id=") {
				continue
			}
			instanceID := strings.TrimPrefix(instEntry.Name(), "instance_id=")
			instanceDir := filepath.Join(dataTypeDir, instEntry.Name())

			// Walk date=* directories.
			dateDirs, err := os.ReadDir(instanceDir)
			if err != nil {
				return nil, fmt.Errorf("reading instance dir %s: %w", instanceDir, err)
			}

			for _, dateEntry := range dateDirs {
				if !dateEntry.IsDir() || !strings.HasPrefix(dateEntry.Name(), "date=") {
					continue
				}

				select {
				case <-ctx.Done():
					return report, ctx.Err()
				default:
				}

				dateStr := strings.TrimPrefix(dateEntry.Name(), "date=")
				partitionDir := filepath.Join(instanceDir, dateEntry.Name())

				result, err := compactPartition(ctx, partitionDir, dataType, instanceID, dateStr, cutoff, opts)
				if err != nil {
					return report, fmt.Errorf("compacting partition %s: %w", partitionDir, err)
				}

				report.Partitions = append(report.Partitions, result)
				if result.Skipped {
					report.PartSkipped++
				} else {
					report.FilesIn += result.InputFiles
					report.FilesOut += result.OutputFiles
					report.TotalIn += result.InputBytes
					report.TotalOut += result.OutputBytes
				}
			}
		}
	}

	return report, nil
}

// compactPartition processes a single (dataType, instance_id, date) partition.
func compactPartition(
	ctx context.Context,
	partitionDir, dataType, instanceID, dateStr string,
	cutoff time.Time,
	opts Options,
) (PartitionResult, error) {
	result := PartitionResult{
		DataType:   dataType,
		InstanceID: instanceID,
		Date:       dateStr,
	}

	// Parse the partition date.
	partDate, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		result.Skipped = true
		result.SkipReason = fmt.Sprintf("unparseable date: %v", err)
		return result, nil
	}

	// Skip partitions that are too recent.
	if !partDate.Before(cutoff) {
		result.Skipped = true
		result.SkipReason = "within --older-than threshold"
		return result, nil
	}

	// List chunk_*.parquet files (skip already-compacted files).
	entries, err := os.ReadDir(partitionDir)
	if err != nil {
		return result, fmt.Errorf("reading partition dir: %w", err)
	}

	var chunkFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, "chunk_") && strings.HasSuffix(name, ".parquet") {
			chunkFiles = append(chunkFiles, filepath.Join(partitionDir, name))
		}
	}
	sort.Strings(chunkFiles)

	if len(chunkFiles) == 0 {
		result.Skipped = true
		result.SkipReason = "no chunk files"
		return result, nil
	}
	if len(chunkFiles) == 1 {
		info, _ := os.Stat(chunkFiles[0])
		if info != nil {
			result.InputBytes = info.Size()
		}
		result.Skipped = true
		result.SkipReason = "single chunk, nothing to merge"
		return result, nil
	}

	// Tally input sizes.
	result.InputFiles = len(chunkFiles)
	for _, cf := range chunkFiles {
		if info, err := os.Stat(cf); err == nil {
			result.InputBytes += info.Size()
		}
	}

	if opts.DryRun {
		// Estimate: how many target-size files would result.
		result.OutputFiles = int((result.InputBytes + opts.TargetSize - 1) / opts.TargetSize)
		if result.OutputFiles == 0 {
			result.OutputFiles = 1
		}
		return result, nil
	}

	// Perform actual compaction.
	outFiles, outBytes, err := mergeChunks(ctx, partitionDir, chunkFiles, opts.TargetSize)
	if err != nil {
		return result, fmt.Errorf("merging chunks: %w", err)
	}
	result.OutputFiles = outFiles
	result.OutputBytes = outBytes

	// Remove the original chunk files.
	for _, cf := range chunkFiles {
		if err := os.Remove(cf); err != nil && !os.IsNotExist(err) {
			return result, fmt.Errorf("removing original chunk %s: %w", cf, err)
		}
	}

	return result, nil
}

// mergeChunks reads all chunkFiles and writes them into compacted_NNNN.parquet
// files in partitionDir, splitting at targetSize boundaries.
// Returns the number of output files written and total bytes written.
//
// Atomic semantics: each compacted file is written to a .compact_tmp sibling
// directory, fsync'd, then renamed into partitionDir. The collector uses its
// own exclusive seq counter so the names "compacted_NNNN.parquet" will not
// collide with any live writer.
func mergeChunks(ctx context.Context, partitionDir string, chunkFiles []string, targetSize int64) (int, int64, error) {
	// Derive schema from the first file.
	schema, err := schemaFromFile(chunkFiles[0])
	if err != nil {
		return 0, 0, fmt.Errorf("reading schema from %s: %w", chunkFiles[0], err)
	}

	tmpDir := filepath.Join(partitionDir, ".compact_tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return 0, 0, fmt.Errorf("creating tmp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	var (
		fileIdx    int
		outFiles   int
		totalBytes int64
		cur        *os.File
		curWriter  *parquet.GenericWriter[parquet.Row]
		curBytes   int64
	)

	openNext := func() error {
		if cur != nil {
			if err := curWriter.Close(); err != nil {
				return fmt.Errorf("closing compacted writer: %w", err)
			}
			if err := cur.Sync(); err != nil {
				return fmt.Errorf("syncing compacted file: %w", err)
			}
			info, _ := cur.Stat()
			if info != nil {
				totalBytes += info.Size()
				outFiles++
			}
			_ = cur.Close()

			// Rename from tmp dir into partition dir.
			tmpPath := cur.Name()
			finalPath := filepath.Join(partitionDir, fmt.Sprintf("compacted_%04d.parquet", fileIdx-1))
			if err := os.Rename(tmpPath, finalPath); err != nil {
				return fmt.Errorf("renaming %s → %s: %w", tmpPath, finalPath, err)
			}
			cur = nil
			curWriter = nil
			curBytes = 0
		}

		tmpPath := filepath.Join(tmpDir, fmt.Sprintf("compacted_%04d.parquet", fileIdx))
		fileIdx++
		f, err := os.Create(tmpPath)
		if err != nil {
			return fmt.Errorf("creating output file %s: %w", tmpPath, err)
		}
		cur = f
		curWriter = parquet.NewGenericWriter[parquet.Row](f, schema)
		return nil
	}

	if err := openNext(); err != nil {
		return 0, 0, err
	}

	for _, chunkPath := range chunkFiles {
		select {
		case <-ctx.Done():
			_ = cur.Close()
			return 0, 0, ctx.Err()
		default:
		}

		if err := copyChunkRows(chunkPath, curWriter); err != nil {
			_ = cur.Close()
			return 0, 0, fmt.Errorf("copying %s: %w", chunkPath, err)
		}

		// Check size after each source file.
		if info, err := cur.Stat(); err == nil {
			curBytes = info.Size()
		}
		if curBytes >= targetSize {
			if err := openNext(); err != nil {
				return 0, 0, err
			}
		}
	}

	// Close and rename the last file.
	if curWriter != nil {
		if err := curWriter.Close(); err != nil {
			return 0, 0, fmt.Errorf("closing final compacted writer: %w", err)
		}
		if err := cur.Sync(); err != nil {
			return 0, 0, fmt.Errorf("syncing final compacted file: %w", err)
		}
		info, _ := cur.Stat()
		if info != nil {
			totalBytes += info.Size()
			outFiles++
		}
		tmpPath := cur.Name()
		_ = cur.Close()

		finalPath := filepath.Join(partitionDir, fmt.Sprintf("compacted_%04d.parquet", fileIdx-1))
		if err := os.Rename(tmpPath, finalPath); err != nil {
			return 0, 0, fmt.Errorf("renaming final file: %w", err)
		}
	}

	return outFiles, totalBytes, nil
}

// schemaFromFile returns the Parquet schema of the first row group in path.
func schemaFromFile(path string) (*parquet.Schema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close() //nolint:errcheck

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}

	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return nil, fmt.Errorf("opening parquet file %s: %w", path, err)
	}

	return pf.Schema(), nil
}

// copyChunkRows reads all rows from a single Parquet chunk and writes them to dst.
func copyChunkRows(path string, dst parquet.RowWriter) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close() //nolint:errcheck

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}

	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return fmt.Errorf("opening parquet %s: %w", path, err)
	}

	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()
		if _, err := parquet.CopyRows(dst, rows); err != nil {
			return fmt.Errorf("copying rows from %s: %w", path, err)
		}
	}
	return nil
}
