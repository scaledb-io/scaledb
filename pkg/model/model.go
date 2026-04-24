// Package model defines the data types shared between collection and output packages.
package model

// Metric represents a single numeric metric sample.
type Metric struct {
	InstanceID string  `parquet:"instance_id"`
	ClusterID  string  `parquet:"cluster_id"`
	MetricName string  `parquet:"metric_name"`
	Value      float64 `parquet:"value"`
	Timestamp  string  `parquet:"timestamp"`
}

// QueryDigest represents a top-N digest from events_statements_summary_by_digest.
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

// IndexUsage represents per-index I/O wait counters.
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

// QuerySample represents a captured statement with real SQL text — PII risk.
type QuerySample struct {
	InstanceID           string `parquet:"instance_id"`
	ClusterID            string `parquet:"cluster_id"`
	EventID              uint64 `parquet:"event_id"`
	Digest               string `parquet:"digest"`
	SQLText              string `parquet:"sql_text"`
	TimerWait            uint64 `parquet:"timer_wait"`
	RowsExamined         uint64 `parquet:"rows_examined"`
	RowsSent             uint64 `parquet:"rows_sent"`
	RowsAffected         uint64 `parquet:"rows_affected"`
	CreatedTmpTables     uint64 `parquet:"created_tmp_tables"`
	CreatedTmpDiskTables uint64 `parquet:"created_tmp_disk_tables"`
	NoIndexUsed          uint64 `parquet:"no_index_used"`
	CurrentSchema        string `parquet:"current_schema"`
	Timestamp            string `parquet:"timestamp"`
}

// WaitEventSummary represents aggregated wait events bucketed by 5-second intervals.
type WaitEventSummary struct {
	InstanceID   string `parquet:"instance_id"`
	ClusterID    string `parquet:"cluster_id"`
	ParentDigest string `parquet:"parent_digest"`
	EventName    string `parquet:"event_name"`
	Count        uint64 `parquet:"count"`
	TotalWait    uint64 `parquet:"total_wait"`
	Timestamp    string `parquet:"timestamp"`
}
