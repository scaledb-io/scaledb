package compact

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/scaledb-io/scaledb/pkg/model"
)

// writeChunkFile creates a single chunk_NNNNNN.parquet file at path
// containing rows as Parquet-encoded model.Metric rows.
func writeChunkFile(t *testing.T, path string, rows []model.Metric) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close() //nolint:errcheck

	pw := parquet.NewGenericWriter[model.Metric](f)
	if _, err := pw.Write(rows); err != nil {
		t.Fatalf("write rows to %s: %v", path, err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close writer for %s: %v", path, err)
	}
}

// setupPartition creates n chunk files in the Hive partition layout under basePath.
// Each chunk contains rowsPerChunk model.Metric rows.
// Returns the partition directory path.
func setupPartition(t *testing.T, basePath, dataType, instanceID, dateStr string, n, rowsPerChunk int) string {
	t.Helper()
	partDir := filepath.Join(basePath, dataType,
		fmt.Sprintf("instance_id=%s", instanceID),
		fmt.Sprintf("date=%s", dateStr))

	for i := 0; i < n; i++ {
		rows := make([]model.Metric, rowsPerChunk)
		for j := range rows {
			rows[j] = model.Metric{
				InstanceID: instanceID,
				ClusterID:  "cluster-1",
				MetricName: fmt.Sprintf("metric_%d_%d", i, j),
				Value:      float64(i*rowsPerChunk + j),
				Timestamp:  "2026-01-01T00:00:00Z",
			}
		}
		path := filepath.Join(partDir, fmt.Sprintf("chunk_%06d.parquet", i+1))
		writeChunkFile(t, path, rows)
	}
	return partDir
}

// filesWithPrefix returns all non-directory entries in dir whose names start with prefix.
func filesWithPrefix(t *testing.T, dir, prefix string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out
}

// countRows returns the total row count across all RowGroups in a Parquet file.
func countRows(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close() //nolint:errcheck

	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		t.Fatalf("open parquet %s: %v", path, err)
	}
	total := 0
	for _, rg := range pf.RowGroups() {
		total += int(rg.NumRows())
	}
	return total
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestCompact_BasicMerge verifies that N chunk files are merged into 1 compacted file
// and the original chunk files are removed.
func TestCompact_BasicMerge(t *testing.T) {
	basePath := t.TempDir()
	oldDate := time.Now().UTC().AddDate(0, 0, -10).Format("2006-01-02")
	partDir := setupPartition(t, basePath, "metrics", "inst-1", oldDate, 5, 10)

	report, err := Compact(context.Background(), basePath, Options{
		TargetSize: 512 * 1024 * 1024,
		OlderThan:  7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if len(report.Partitions) != 1 {
		t.Fatalf("expected 1 partition result, got %d", len(report.Partitions))
	}
	pr := report.Partitions[0]
	if pr.Skipped {
		t.Fatalf("partition should not be skipped: %s", pr.SkipReason)
	}
	if pr.InputFiles != 5 {
		t.Errorf("InputFiles: got %d, want 5", pr.InputFiles)
	}
	if pr.OutputFiles != 1 {
		t.Errorf("OutputFiles: got %d, want 1", pr.OutputFiles)
	}

	// All chunk files should be gone.
	if chunks := filesWithPrefix(t, partDir, "chunk_"); len(chunks) != 0 {
		t.Errorf("expected 0 chunk files after compaction, got %d", len(chunks))
	}

	// Exactly 1 compacted file.
	compacted := filesWithPrefix(t, partDir, "compacted_")
	if len(compacted) != 1 {
		t.Fatalf("expected 1 compacted file, got %d", len(compacted))
	}

	// Row count must be preserved: 5 chunks × 10 rows = 50.
	if got := countRows(t, compacted[0]); got != 50 {
		t.Errorf("row count: got %d, want 50", got)
	}
}

// TestCompact_SkipsRecentPartition verifies that partitions within --older-than are skipped.
func TestCompact_SkipsRecentPartition(t *testing.T) {
	basePath := t.TempDir()
	recentDate := time.Now().UTC().Format("2006-01-02") // today
	partDir := setupPartition(t, basePath, "metrics", "inst-1", recentDate, 3, 5)

	report, err := Compact(context.Background(), basePath, Options{
		TargetSize: 512 * 1024 * 1024,
		OlderThan:  7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if len(report.Partitions) != 1 {
		t.Fatalf("expected 1 partition result, got %d", len(report.Partitions))
	}
	if !report.Partitions[0].Skipped {
		t.Error("expected recent partition to be skipped")
	}

	// Files must be untouched.
	if chunks := filesWithPrefix(t, partDir, "chunk_"); len(chunks) != 3 {
		t.Errorf("expected 3 chunk files still present, got %d", len(chunks))
	}
}

// TestCompact_DryRun verifies that dry-run reports without modifying files.
func TestCompact_DryRun(t *testing.T) {
	basePath := t.TempDir()
	oldDate := time.Now().UTC().AddDate(0, 0, -10).Format("2006-01-02")
	partDir := setupPartition(t, basePath, "metrics", "inst-1", oldDate, 4, 10)

	report, err := Compact(context.Background(), basePath, Options{
		TargetSize: 512 * 1024 * 1024,
		OlderThan:  7 * 24 * time.Hour,
		DryRun:     true,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if !report.DryRun {
		t.Error("expected report.DryRun = true")
	}

	// Files must be untouched.
	if chunks := filesWithPrefix(t, partDir, "chunk_"); len(chunks) != 4 {
		t.Errorf("expected 4 chunk files still present, got %d", len(chunks))
	}
	if compacted := filesWithPrefix(t, partDir, "compacted_"); len(compacted) != 0 {
		t.Errorf("expected 0 compacted files after dry-run, got %d", len(compacted))
	}
}

// TestCompact_DataTypeFilter verifies --datatypes restricts compaction to specified types.
func TestCompact_DataTypeFilter(t *testing.T) {
	basePath := t.TempDir()
	oldDate := time.Now().UTC().AddDate(0, 0, -10).Format("2006-01-02")

	metricsDir := setupPartition(t, basePath, "metrics", "inst-1", oldDate, 3, 5)
	digestDir := setupPartition(t, basePath, "query-digests", "inst-1", oldDate, 3, 5)

	_, err := Compact(context.Background(), basePath, Options{
		TargetSize: 512 * 1024 * 1024,
		OlderThan:  7 * 24 * time.Hour,
		DataTypes:  []string{"metrics"},
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// metrics: chunks gone, compacted present.
	if chunks := filesWithPrefix(t, metricsDir, "chunk_"); len(chunks) != 0 {
		t.Errorf("metrics chunks: expected 0, got %d", len(chunks))
	}

	// query-digests: untouched.
	if chunks := filesWithPrefix(t, digestDir, "chunk_"); len(chunks) != 3 {
		t.Errorf("query-digests chunks: expected 3, got %d", len(chunks))
	}
}

// TestCompact_SingleChunkSkipped verifies that a partition with one file is left alone.
func TestCompact_SingleChunkSkipped(t *testing.T) {
	basePath := t.TempDir()
	oldDate := time.Now().UTC().AddDate(0, 0, -10).Format("2006-01-02")
	partDir := setupPartition(t, basePath, "metrics", "inst-1", oldDate, 1, 10)

	report, err := Compact(context.Background(), basePath, Options{
		TargetSize: 512 * 1024 * 1024,
		OlderThan:  7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if len(report.Partitions) != 1 {
		t.Fatalf("expected 1 partition result, got %d", len(report.Partitions))
	}
	if !report.Partitions[0].Skipped {
		t.Error("expected single-chunk partition to be skipped")
	}

	// File should still be there.
	if chunks := filesWithPrefix(t, partDir, "chunk_"); len(chunks) != 1 {
		t.Errorf("expected 1 chunk file to remain, got %d", len(chunks))
	}
}

// TestCompact_MultipleInstances verifies all instances under a data type are compacted.
func TestCompact_MultipleInstances(t *testing.T) {
	basePath := t.TempDir()
	oldDate := time.Now().UTC().AddDate(0, 0, -15).Format("2006-01-02")

	setupPartition(t, basePath, "metrics", "inst-a", oldDate, 3, 5)
	setupPartition(t, basePath, "metrics", "inst-b", oldDate, 4, 5)

	report, err := Compact(context.Background(), basePath, Options{
		TargetSize: 512 * 1024 * 1024,
		OlderThan:  7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	var processed int
	for _, p := range report.Partitions {
		if !p.Skipped {
			processed++
		}
	}
	if processed != 2 {
		t.Errorf("expected 2 processed partitions, got %d", processed)
	}
	if report.FilesIn != 7 {
		t.Errorf("FilesIn: got %d, want 7", report.FilesIn)
	}
	if report.FilesOut != 2 {
		t.Errorf("FilesOut: got %d, want 2", report.FilesOut)
	}
}

// TestCompact_ContextCancellation verifies the compactor respects ctx cancellation.
func TestCompact_ContextCancellation(t *testing.T) {
	basePath := t.TempDir()
	oldDate := time.Now().UTC().AddDate(0, 0, -10).Format("2006-01-02")
	setupPartition(t, basePath, "metrics", "inst-1", oldDate, 3, 5)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling

	_, err := Compact(ctx, basePath, Options{
		TargetSize: 512 * 1024 * 1024,
		OlderThan:  7 * 24 * time.Hour,
	})
	if err == nil {
		t.Error("expected error on cancelled context, got nil")
	}
}

// TestCompact_PreservesExistingCompactedFiles verifies that already-compacted files
// in a partition are not re-read or deleted.
func TestCompact_PreservesExistingCompactedFiles(t *testing.T) {
	basePath := t.TempDir()
	oldDate := time.Now().UTC().AddDate(0, 0, -10).Format("2006-01-02")
	partDir := setupPartition(t, basePath, "metrics", "inst-1", oldDate, 3, 10)

	// Simulate a pre-existing compacted file from a prior run.
	existingCompacted := filepath.Join(partDir, "compacted_0000.parquet")
	writeChunkFile(t, existingCompacted, []model.Metric{
		{InstanceID: "inst-1", MetricName: "old_metric", Value: 999, Timestamp: "2025-01-01T00:00:00Z"},
	})

	report, err := Compact(context.Background(), basePath, Options{
		TargetSize: 512 * 1024 * 1024,
		OlderThan:  7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	pr := report.Partitions[0]
	// Only the 3 chunk_ files should be counted as input.
	if pr.InputFiles != 3 {
		t.Errorf("InputFiles: got %d, want 3 (compacted_ should not be counted)", pr.InputFiles)
	}

	// The pre-existing compacted_0000.parquet should still be there.
	if _, err := os.Stat(existingCompacted); os.IsNotExist(err) {
		t.Error("pre-existing compacted_0000.parquet was incorrectly deleted")
	}
}
