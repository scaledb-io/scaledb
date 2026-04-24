package output

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
)

// S3Writer writes Parquet files to an S3-compatible bucket in Hive-style partitions:
//
//	s3://{bucket}/{prefix}/{dataType}/instance_id={id}/date={YYYY-MM-DD}/chunk_NNN.parquet
type S3Writer struct {
	client *s3.Client
	bucket string
	prefix string // path prefix within bucket (may be empty)
	region string
}

// S3Config holds the configuration for the S3 writer.
type S3Config struct {
	Bucket   string // "s3://bucket-name/optional/prefix/" or just "bucket-name"
	Region   string // AWS region, empty = use default
	Endpoint string // Custom S3 endpoint (MinIO, R2, etc.), empty = AWS
}

// NewS3Writer creates a writer that uploads Parquet files to S3.
func NewS3Writer(ctx context.Context, cfg S3Config) (*S3Writer, error) {
	bucket, prefix := parseBucketURI(cfg.Bucket)
	if bucket == "" {
		return nil, fmt.Errorf("invalid S3 bucket: %q", cfg.Bucket)
	}

	// Load AWS config.
	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	// Build S3 client options.
	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true // Required for MinIO/R2.
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &S3Writer{
		client: client,
		bucket: bucket,
		prefix: prefix,
		region: cfg.Region,
	}, nil
}

// S3WriteRows writes typed rows as a Parquet file and uploads to S3.
// Each call produces one Parquet object in S3. Rows are written to an
// in-memory buffer, encoded as Parquet, then uploaded.
func S3WriteRows[T any](w *S3Writer, dataType, instanceID string, rows []T) error {
	if len(rows) == 0 {
		return nil
	}

	now := time.Now().UTC()
	date := now.Format("2006-01-02")
	ts := now.Format("150405")

	// Write Parquet to memory buffer.
	var parquetBuf bytes.Buffer
	pw := parquet.NewGenericWriter[T](&parquetBuf)
	if _, err := pw.Write(rows); err != nil {
		return fmt.Errorf("writing parquet rows: %w", err)
	}
	if err := pw.Close(); err != nil {
		return fmt.Errorf("closing parquet writer: %w", err)
	}

	// Build S3 key.
	s3Key := fmt.Sprintf("%s%s/instance_id=%s/date=%s/%s.parquet",
		w.prefix, dataType, instanceID, date, ts)

	// Upload to S3.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := w.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(w.bucket),
		Key:         aws.String(s3Key),
		Body:        bytes.NewReader(parquetBuf.Bytes()),
		ContentType: aws.String("application/vnd.apache.parquet"),
	})
	if err != nil {
		return fmt.Errorf("uploading s3://%s/%s: %w", w.bucket, s3Key, err)
	}

	return nil
}

// Flush is a no-op for S3Writer — each WriteRows call uploads immediately.
func (w *S3Writer) Flush() error {
	return nil
}

// Close is a no-op for S3Writer.
func (w *S3Writer) Close() error {
	return nil
}

// parseBucketURI extracts bucket name and prefix from various URI formats:
//
//	"s3://my-bucket/prefix/"      → ("my-bucket", "prefix/")
//	"s3://my-bucket"              → ("my-bucket", "")
//	"my-bucket/prefix"            → ("my-bucket", "prefix/")
//	"my-bucket"                   → ("my-bucket", "")
func parseBucketURI(uri string) (bucket, prefix string) {
	// Strip s3:// scheme.
	uri = strings.TrimPrefix(uri, "s3://")

	// Split on first slash.
	parts := strings.SplitN(uri, "/", 2)
	bucket = parts[0]
	if len(parts) > 1 {
		prefix = strings.TrimSuffix(parts[1], "/")
		if prefix != "" {
			prefix += "/"
		}
	}
	return bucket, prefix
}
