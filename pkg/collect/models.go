// Package collect provides continuous MySQL performance data collection.
// It connects to Aurora MySQL clusters, auto-discovers instances,
// polls performance_schema metrics, and writes Parquet files.
package collect

// Metric represents a single numeric metric sample from global_status,
// processlist summary, or innodb_metrics.
type Metric struct {
	InstanceID string  `parquet:"instance_id"`
	ClusterID  string  `parquet:"cluster_id"`
	MetricName string  `parquet:"metric_name"`
	Value      float64 `parquet:"value"`
	Timestamp  string  `parquet:"timestamp"` // UTC "2006-01-02 15:04:05"
}

// QueryDigest represents a top-N digest from
// performance_schema.events_statements_summary_by_digest.
type QueryDigest struct {
	InstanceID              string `parquet:"instance_id"`
	ClusterID               string `parquet:"cluster_id"`
	Digest                  string `parquet:"digest"`
	DigestText              string `parquet:"digest_text"`
	SchemaName              string `parquet:"schema_name"`
	ExecCount               uint64 `parquet:"exec_count"`
	SumTimerWait            uint64 `parquet:"sum_timer_wait"`
	SumRowsExamined         uint64 `parquet:"sum_rows_examined"`
	SumRowsSent             uint64 `parquet:"sum_rows_sent"`
	SumRowsAffected         uint64 `parquet:"sum_rows_affected"`
	SumCreatedTmpTables     uint64 `parquet:"sum_created_tmp_tables"`
	SumCreatedTmpDiskTables uint64 `parquet:"sum_created_tmp_disk_tables"`
	SumNoIndexUsed          uint64 `parquet:"sum_no_index_used"`
	FirstSeen               string `parquet:"first_seen"`
	LastSeen                string `parquet:"last_seen"`
	Timestamp               string `parquet:"timestamp"`
}

// IndexUsage represents per-index I/O wait counters from
// performance_schema.table_io_waits_summary_by_index_usage.
type IndexUsage struct {
	InstanceID string `parquet:"instance_id"`
	ClusterID  string `parquet:"cluster_id"`
	SchemaName string `parquet:"schema_name"`
	TableName  string `parquet:"table_name"`
	IndexName  string `parquet:"index_name"`
	CountRead  uint64 `parquet:"count_read"`
	CountWrite uint64 `parquet:"count_write"`
	Timestamp  string `parquet:"timestamp"`
}

// QuerySample represents a single captured statement from
// performance_schema.events_statements_history_long.
// Contains real SQL text with actual parameter values — PII risk.
type QuerySample struct {
	InstanceID              string `parquet:"instance_id"`
	ClusterID               string `parquet:"cluster_id"`
	EventID                 uint64 `parquet:"event_id"`
	Digest                  string `parquet:"digest"`
	SQLText                 string `parquet:"sql_text"`
	TimerWait               uint64 `parquet:"timer_wait"`
	RowsExamined            uint64 `parquet:"rows_examined"`
	RowsSent                uint64 `parquet:"rows_sent"`
	RowsAffected            uint64 `parquet:"rows_affected"`
	CreatedTmpTables        uint64 `parquet:"created_tmp_tables"`
	CreatedTmpDiskTables    uint64 `parquet:"created_tmp_disk_tables"`
	NoIndexUsed             uint64 `parquet:"no_index_used"`
	CurrentSchema           string `parquet:"current_schema"`
	Timestamp               string `parquet:"timestamp"`
}

// WaitEventSummary represents aggregated wait events bucketed by 5-second
// intervals, correlated to parent statements via digest (QUID).
type WaitEventSummary struct {
	InstanceID   string `parquet:"instance_id"`
	ClusterID    string `parquet:"cluster_id"`
	ParentDigest string `parquet:"parent_digest"`
	EventName    string `parquet:"event_name"`
	Count        uint64 `parquet:"count"`
	TotalWait    uint64 `parquet:"total_wait"`
	Timestamp    string `parquet:"timestamp"`
}

// DiscoveredInstance represents an Aurora instance found via topology discovery.
type DiscoveredInstance struct {
	ServerID string
	Endpoint string
	IsWriter bool
}

// PollResult aggregates the output from a single poll cycle for one instance.
type PollResult struct {
	Metrics    []Metric
	Digests    []QueryDigest
	IndexUsage []IndexUsage
	Samples    []QuerySample
	WaitEvents []WaitEventSummary
}
