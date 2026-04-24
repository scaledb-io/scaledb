// Package output provides writers for collected MySQL performance data.
// Phase 1 supports local disk (Parquet files in Hive-style partitions).
// Phase 2 will add S3 output.
package output

// Writer defines the interface for writing collected data to storage.
// Data is written as typed Parquet rows via the generic writeRows function.
// The concrete Write* methods on LocalWriter accept collect.* types directly.
type Writer interface {
	Flush() error
	Close() error
}
