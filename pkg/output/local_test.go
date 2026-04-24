package output_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/scaledb-io/scaledb/pkg/model"
	"github.com/scaledb-io/scaledb/pkg/output"
)

func TestLocalWriter_WriteMetrics(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	metrics := []model.Metric{
		{InstanceID: "inst-1", ClusterID: "cluster-1", MetricName: "global_status.Queries", Value: 42, Timestamp: "2026-04-24 10:00:00"},
		{InstanceID: "inst-1", ClusterID: "cluster-1", MetricName: "global_status.Uptime", Value: 3600, Timestamp: "2026-04-24 10:00:00"},
	}

	if err := output.WriteRows(w, "metrics", "inst-1", metrics); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Read back the Parquet file.
	pattern := filepath.Join(dir, "metrics", "instance_id=inst-1", "date=*", "chunk_000001.parquet")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 parquet file, got %d (pattern: %s)", len(matches), pattern)
	}

	f, err := os.Open(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	stat, _ := f.Stat()
	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewGenericReader[model.Metric](pf)
	defer reader.Close()

	readBack := make([]model.Metric, 10)
	n, _ := reader.Read(readBack)
	readBack = readBack[:n]

	if n != 2 {
		t.Fatalf("read back %d rows, want 2", n)
	}
	if readBack[0].MetricName != "global_status.Queries" {
		t.Errorf("row[0].MetricName = %q, want global_status.Queries", readBack[0].MetricName)
	}
	if readBack[0].Value != 42 {
		t.Errorf("row[0].Value = %f, want 42", readBack[0].Value)
	}
}

func TestLocalWriter_WriteDigests(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	digests := []model.QueryDigest{
		{
			InstanceID:   "inst-1",
			ClusterID:    "cluster-1",
			Digest:       "abc123",
			DigestText:   "SELECT * FROM orders WHERE id = ?",
			SchemaName:   "shop",
			ExecCount:    1000,
			SumTimerWait: 5000000000,
			Timestamp:    "2026-04-24 10:00:00",
		},
	}

	if err := output.WriteRows(w, "query-digests", "inst-1", digests); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Verify the file exists in the right partition.
	pattern := filepath.Join(dir, "query-digests", "instance_id=inst-1", "date=*", "chunk_000001.parquet")
	matches, _ := filepath.Glob(pattern)
	if len(matches) != 1 {
		t.Fatalf("expected 1 parquet file, got %d", len(matches))
	}
}

func TestLocalWriter_PartitionLayout(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Write to two different instances.
	m1 := []model.Metric{{InstanceID: "inst-1", ClusterID: "c1", MetricName: "m1", Value: 1, Timestamp: "2026-04-24 10:00:00"}}
	m2 := []model.Metric{{InstanceID: "inst-2", ClusterID: "c1", MetricName: "m1", Value: 2, Timestamp: "2026-04-24 10:00:00"}}

	output.WriteRows(w, "metrics", "inst-1", m1)
	output.WriteRows(w, "metrics", "inst-2", m2)
	w.Flush()

	// Check both partitions exist.
	for _, inst := range []string{"inst-1", "inst-2"} {
		pattern := filepath.Join(dir, "metrics", "instance_id="+inst, "date=*", "*.parquet")
		matches, _ := filepath.Glob(pattern)
		if len(matches) != 1 {
			t.Errorf("expected 1 file for %s, got %d", inst, len(matches))
		}
	}
}

func TestLocalWriter_EmptyWrite(t *testing.T) {
	dir := t.TempDir()
	w, err := output.NewLocalWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Writing empty slices should be a no-op.
	if err := output.WriteRows(w, "metrics", "inst-1", []model.Metric(nil)); err != nil {
		t.Errorf("output.WriteRows(nil) returned error: %v", err)
	}
	if err := output.WriteRows(w, "metrics", "inst-1", []model.Metric{}); err != nil {
		t.Errorf("output.WriteRows([]) returned error: %v", err)
	}
}
