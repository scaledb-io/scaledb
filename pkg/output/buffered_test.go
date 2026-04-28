package output_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/scaledb-io/scaledb/pkg/model"
	"github.com/scaledb-io/scaledb/pkg/output"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func TestBufferedWriter_AccumulatesAndFlushes(t *testing.T) {
	dir := t.TempDir()
	lw, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	sink := output.WrapLocal(lw)

	bw := output.NewBufferedWriter(sink, output.BufferedWriterConfig{
		FlushRows: -1, // disabled — only manual flush
		FlushSize: -1,
	}, testLogger())

	// Write 3 batches for the same instance.
	for i := 0; i < 3; i++ {
		err := bw.WriteMetrics("inst-1", []model.Metric{
			{InstanceID: "inst-1", ClusterID: "c1", MetricName: "m1", Value: float64(i), Timestamp: "2026-04-27 10:00:00"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Before flush: no files should exist.
	files := globParquet(t, dir)
	if len(files) != 0 {
		t.Fatalf("expected 0 files before flush, got %d", len(files))
	}

	// Stats should show buffered rows.
	instances, rows := bw.Stats()
	if instances != 1 || rows != 3 {
		t.Errorf("Stats() = (%d, %d), want (1, 3)", instances, rows)
	}

	// Flush.
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}

	// After flush: exactly 1 file with 3 rows.
	files = globParquet(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected 1 parquet file after flush, got %d", len(files))
	}

	rows = readParquetRowCount[model.Metric](t, files[0])
	if rows != 3 {
		t.Errorf("parquet file has %d rows, want 3", rows)
	}

	// Stats should be empty after flush.
	instances, totalRows := bw.Stats()
	if instances != 0 || totalRows != 0 {
		t.Errorf("Stats() after flush = (%d, %d), want (0, 0)", instances, totalRows)
	}
}

func TestBufferedWriter_RowTrigger(t *testing.T) {
	dir := t.TempDir()
	lw, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	sink := output.WrapLocal(lw)

	bw := output.NewBufferedWriter(sink, output.BufferedWriterConfig{
		FlushRows: 5,
		FlushSize: -1,
	}, testLogger())

	// Write 4 rows — should not trigger.
	for i := 0; i < 4; i++ {
		_ = bw.WriteMetrics("inst-1", []model.Metric{
			{InstanceID: "inst-1", ClusterID: "c1", MetricName: "m1", Value: float64(i), Timestamp: "2026-04-27 10:00:00"},
		})
	}
	if files := globParquet(t, dir); len(files) != 0 {
		t.Fatalf("expected 0 files at 4 rows, got %d", len(files))
	}

	// Write 1 more row — hits threshold of 5, should auto-flush.
	_ = bw.WriteMetrics("inst-1", []model.Metric{
		{InstanceID: "inst-1", ClusterID: "c1", MetricName: "m1", Value: 99, Timestamp: "2026-04-27 10:00:00"},
	})
	files := globParquet(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected 1 file after row trigger, got %d", len(files))
	}

	rows := readParquetRowCount[model.Metric](t, files[0])
	if rows != 5 {
		t.Errorf("parquet file has %d rows, want 5", rows)
	}
}

func TestBufferedWriter_MixedDataTypes(t *testing.T) {
	dir := t.TempDir()
	lw, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	sink := output.WrapLocal(lw)

	bw := output.NewBufferedWriter(sink, output.BufferedWriterConfig{
		FlushRows: -1,
		FlushSize: -1,
	}, testLogger())

	// Write different data types for the same instance.
	_ = bw.WriteMetrics("inst-1", []model.Metric{
		{InstanceID: "inst-1", ClusterID: "c1", MetricName: "m1", Value: 1, Timestamp: "2026-04-27 10:00:00"},
	})
	_ = bw.WriteDigests("inst-1", []model.QueryDigest{
		{InstanceID: "inst-1", ClusterID: "c1", Digest: "abc", DigestText: "SELECT 1", Timestamp: "2026-04-27 10:00:00"},
	})
	_ = bw.WriteIndexUsage("inst-1", []model.IndexUsage{
		{InstanceID: "inst-1", ClusterID: "c1", SchemaName: "db", TableName: "t", IndexName: "idx", Timestamp: "2026-04-27 10:00:00"},
	})

	// Stats should show 3 rows across types.
	instances, totalRows := bw.Stats()
	if instances != 1 || totalRows != 3 {
		t.Errorf("Stats() = (%d, %d), want (1, 3)", instances, totalRows)
	}

	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}

	// Should produce separate files for each data type.
	files := globParquet(t, dir)
	if len(files) != 3 {
		t.Fatalf("expected 3 parquet files (one per data type), got %d", len(files))
	}
}

func TestBufferedWriter_MultipleInstances(t *testing.T) {
	dir := t.TempDir()
	lw, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	sink := output.WrapLocal(lw)

	bw := output.NewBufferedWriter(sink, output.BufferedWriterConfig{
		FlushRows: -1,
		FlushSize: -1,
	}, testLogger())

	_ = bw.WriteMetrics("inst-1", []model.Metric{
		{InstanceID: "inst-1", ClusterID: "c1", MetricName: "m1", Value: 1, Timestamp: "2026-04-27 10:00:00"},
	})
	_ = bw.WriteMetrics("inst-2", []model.Metric{
		{InstanceID: "inst-2", ClusterID: "c1", MetricName: "m1", Value: 2, Timestamp: "2026-04-27 10:00:00"},
	})

	instances, totalRows := bw.Stats()
	if instances != 2 || totalRows != 2 {
		t.Errorf("Stats() = (%d, %d), want (2, 2)", instances, totalRows)
	}

	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}

	// Should have 2 files — one per instance.
	files := globParquet(t, dir)
	if len(files) != 2 {
		t.Fatalf("expected 2 parquet files (one per instance), got %d", len(files))
	}
}

func TestBufferedWriter_CloseFlushesRemaining(t *testing.T) {
	dir := t.TempDir()
	lw, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	sink := output.WrapLocal(lw)

	bw := output.NewBufferedWriter(sink, output.BufferedWriterConfig{
		FlushRows: -1,
		FlushSize: -1,
	}, testLogger())

	_ = bw.WriteMetrics("inst-1", []model.Metric{
		{InstanceID: "inst-1", ClusterID: "c1", MetricName: "m1", Value: 1, Timestamp: "2026-04-27 10:00:00"},
	})

	// Close should flush.
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}

	files := globParquet(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected 1 file after Close, got %d", len(files))
	}
}

func TestBufferedWriter_EmptyFlush(t *testing.T) {
	dir := t.TempDir()
	lw, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	sink := output.WrapLocal(lw)

	bw := output.NewBufferedWriter(sink, output.BufferedWriterConfig{
		FlushRows: -1,
		FlushSize: -1,
	}, testLogger())

	// Flushing empty buffers should not error or create files.
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}

	files := globParquet(t, dir)
	if len(files) != 0 {
		t.Fatalf("expected 0 files for empty flush, got %d", len(files))
	}
}

func TestBufferedWriter_RowTriggerPerInstance(t *testing.T) {
	dir := t.TempDir()
	lw, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	sink := output.WrapLocal(lw)

	// Row trigger at 3 — counts are per-instance.
	bw := output.NewBufferedWriter(sink, output.BufferedWriterConfig{
		FlushRows: 3,
		FlushSize: -1,
	}, testLogger())

	// Write 2 rows to inst-1 and 2 to inst-2. Neither should trigger.
	_ = bw.WriteMetrics("inst-1", []model.Metric{
		{InstanceID: "inst-1", ClusterID: "c1", MetricName: "m1", Value: 1, Timestamp: "2026-04-27 10:00:00"},
		{InstanceID: "inst-1", ClusterID: "c1", MetricName: "m2", Value: 2, Timestamp: "2026-04-27 10:00:00"},
	})
	_ = bw.WriteMetrics("inst-2", []model.Metric{
		{InstanceID: "inst-2", ClusterID: "c1", MetricName: "m1", Value: 1, Timestamp: "2026-04-27 10:00:00"},
		{InstanceID: "inst-2", ClusterID: "c1", MetricName: "m2", Value: 2, Timestamp: "2026-04-27 10:00:00"},
	})

	if files := globParquet(t, dir); len(files) != 0 {
		t.Fatalf("expected 0 files, got %d", len(files))
	}

	// Adding 1 more to inst-1 triggers flush for inst-1 only.
	_ = bw.WriteMetrics("inst-1", []model.Metric{
		{InstanceID: "inst-1", ClusterID: "c1", MetricName: "m3", Value: 3, Timestamp: "2026-04-27 10:00:00"},
	})

	// inst-1 should have flushed, inst-2 still buffered.
	instances, totalRows := bw.Stats()
	if instances != 1 || totalRows != 2 {
		t.Errorf("Stats() = (%d, %d), want (1, 2) — only inst-2 buffered", instances, totalRows)
	}
}

// helpers

func globParquet(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "**", "**", "**", "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	// Also check one level deeper for Hive partitions.
	deeper, _ := filepath.Glob(filepath.Join(dir, "*", "*", "*", "*.parquet"))
	// Deduplicate.
	seen := make(map[string]struct{})
	var all []string
	for _, f := range append(matches, deeper...) {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			all = append(all, f)
		}
	}
	return all
}

func readParquetRowCount[T any](t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	stat, _ := f.Stat()
	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericReader[T](pf)
	defer func() { _ = reader.Close() }()

	buf := make([]T, 10000)
	n, _ := reader.Read(buf)
	return n
}
