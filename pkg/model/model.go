// Package model defines the data types shared between collection and output packages.
package model

// Metric represents a single numeric metric sample.
type Metric struct {
	InstanceID string  `json:"instance_id" parquet:"instance_id"`
	ClusterID  string  `json:"cluster_id" parquet:"cluster_id"`
	MetricName string  `json:"metric_name" parquet:"metric_name"`
	Value      float64 `json:"value" parquet:"value"`
	Timestamp  string  `json:"timestamp" parquet:"timestamp"`
}

// QueryDigest represents a top-N digest from events_statements_summary_by_digest.
type QueryDigest struct {
	InstanceID              string `json:"instance_id" parquet:"instance_id"`
	ClusterID               string `json:"cluster_id" parquet:"cluster_id"`
	Digest                  string `json:"digest" parquet:"digest"`
	DigestText              string `json:"digest_text" parquet:"digest_text"`
	SchemaName              string `json:"schema_name" parquet:"schema_name"`
	ExecCount               uint64 `json:"exec_count" parquet:"exec_count"`
	SumTimerWait            uint64 `json:"sum_timer_wait" parquet:"sum_timer_wait"`
	SumRowsExamined         uint64 `json:"sum_rows_examined" parquet:"sum_rows_examined"`
	SumRowsSent             uint64 `json:"sum_rows_sent" parquet:"sum_rows_sent"`
	SumRowsAffected         uint64 `json:"sum_rows_affected" parquet:"sum_rows_affected"`
	SumCreatedTmpTables     uint64 `json:"sum_created_tmp_tables" parquet:"sum_created_tmp_tables"`
	SumCreatedTmpDiskTables uint64 `json:"sum_created_tmp_disk_tables" parquet:"sum_created_tmp_disk_tables"`
	SumNoIndexUsed          uint64 `json:"sum_no_index_used" parquet:"sum_no_index_used"`
	FirstSeen               string `json:"first_seen" parquet:"first_seen"`
	LastSeen                string `json:"last_seen" parquet:"last_seen"`
	Timestamp               string `json:"timestamp" parquet:"timestamp"`
}

// IndexUsage represents per-index I/O wait counters.
type IndexUsage struct {
	InstanceID string `json:"instance_id" parquet:"instance_id"`
	ClusterID  string `json:"cluster_id" parquet:"cluster_id"`
	SchemaName string `json:"schema_name" parquet:"schema_name"`
	TableName  string `json:"table_name" parquet:"table_name"`
	IndexName  string `json:"index_name" parquet:"index_name"`
	CountRead  uint64 `json:"count_read" parquet:"count_read"`
	CountWrite uint64 `json:"count_write" parquet:"count_write"`
	Timestamp  string `json:"timestamp" parquet:"timestamp"`
}

// QuerySample represents a captured statement with real SQL text — PII risk.
type QuerySample struct {
	InstanceID           string `json:"instance_id" parquet:"instance_id"`
	ClusterID            string `json:"cluster_id" parquet:"cluster_id"`
	EventID              uint64 `json:"event_id" parquet:"event_id"`
	Digest               string `json:"digest" parquet:"digest"`
	SQLText              string `json:"sql_text" parquet:"sql_text"`
	TimerWait            uint64 `json:"timer_wait" parquet:"timer_wait"`
	RowsExamined         uint64 `json:"rows_examined" parquet:"rows_examined"`
	RowsSent             uint64 `json:"rows_sent" parquet:"rows_sent"`
	RowsAffected         uint64 `json:"rows_affected" parquet:"rows_affected"`
	CreatedTmpTables     uint64 `json:"created_tmp_tables" parquet:"created_tmp_tables"`
	CreatedTmpDiskTables uint64 `json:"created_tmp_disk_tables" parquet:"created_tmp_disk_tables"`
	NoIndexUsed          uint64 `json:"no_index_used" parquet:"no_index_used"`
	CurrentSchema        string `json:"current_schema" parquet:"current_schema"`
	Timestamp            string `json:"timestamp" parquet:"timestamp"`
}

// WaitEventSummary represents aggregated wait events bucketed by 5-second intervals.
type WaitEventSummary struct {
	InstanceID   string `json:"instance_id" parquet:"instance_id"`
	ClusterID    string `json:"cluster_id" parquet:"cluster_id"`
	ParentDigest string `json:"parent_digest" parquet:"parent_digest"`
	EventName    string `json:"event_name" parquet:"event_name"`
	Count        uint64 `json:"count" parquet:"count"`
	TotalWait    uint64 `json:"total_wait" parquet:"total_wait"`
	Timestamp    string `json:"timestamp" parquet:"timestamp"`
}
