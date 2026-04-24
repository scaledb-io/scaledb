// Package output provides writers for collected MySQL performance data.
// Supports local disk (Parquet files) and S3-compatible object storage.
package output

import "github.com/scaledb-io/scaledb/pkg/model"

// DataWriter provides typed write methods for all collected data types.
// Implemented by both LocalWriter and S3Writer via the wrapper functions below.
type DataWriter interface {
	WriteMetrics(instanceID string, metrics []model.Metric) error
	WriteDigests(instanceID string, digests []model.QueryDigest) error
	WriteIndexUsage(instanceID string, usage []model.IndexUsage) error
	WriteQuerySamples(instanceID string, samples []model.QuerySample) error
	WriteWaitEvents(instanceID string, events []model.WaitEventSummary) error
	Flush() error
	Close() error
}

// localDataWriter wraps LocalWriter to implement DataWriter.
type localDataWriter struct {
	w *LocalWriter
}

// WrapLocal wraps a LocalWriter into a DataWriter.
func WrapLocal(w *LocalWriter) DataWriter {
	return &localDataWriter{w: w}
}

func (l *localDataWriter) WriteMetrics(id string, rows []model.Metric) error {
	return WriteRows(l.w, "metrics", id, rows)
}
func (l *localDataWriter) WriteDigests(id string, rows []model.QueryDigest) error {
	return WriteRows(l.w, "query-digests", id, rows)
}
func (l *localDataWriter) WriteIndexUsage(id string, rows []model.IndexUsage) error {
	return WriteRows(l.w, "index-usage", id, rows)
}
func (l *localDataWriter) WriteQuerySamples(id string, rows []model.QuerySample) error {
	return WriteRows(l.w, "samples", id, rows)
}
func (l *localDataWriter) WriteWaitEvents(id string, rows []model.WaitEventSummary) error {
	return WriteRows(l.w, "wait-events", id, rows)
}
func (l *localDataWriter) Flush() error { return l.w.Flush() }
func (l *localDataWriter) Close() error { return l.w.Close() }

// s3DataWriter wraps S3Writer to implement DataWriter.
type s3DataWriter struct {
	w *S3Writer
}

// WrapS3 wraps an S3Writer into a DataWriter.
func WrapS3(w *S3Writer) DataWriter {
	return &s3DataWriter{w: w}
}

func (s *s3DataWriter) WriteMetrics(id string, rows []model.Metric) error {
	return S3WriteRows(s.w, "metrics", id, rows)
}
func (s *s3DataWriter) WriteDigests(id string, rows []model.QueryDigest) error {
	return S3WriteRows(s.w, "query-digests", id, rows)
}
func (s *s3DataWriter) WriteIndexUsage(id string, rows []model.IndexUsage) error {
	return S3WriteRows(s.w, "index-usage", id, rows)
}
func (s *s3DataWriter) WriteQuerySamples(id string, rows []model.QuerySample) error {
	return S3WriteRows(s.w, "samples", id, rows)
}
func (s *s3DataWriter) WriteWaitEvents(id string, rows []model.WaitEventSummary) error {
	return S3WriteRows(s.w, "wait-events", id, rows)
}
func (s *s3DataWriter) Flush() error { return s.w.Flush() }
func (s *s3DataWriter) Close() error { return s.w.Close() }
