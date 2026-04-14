package analyze

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// MySQLSummary is the structured replacement for pt-mysql-summary output.
// Every field is populated from standard MySQL queries (SHOW STATUS, SHOW VARIABLES,
// information_schema, SHOW SLAVE STATUS) so no external tooling is required.
type MySQLSummary struct {
	Version      string            `json:"version"`
	Uptime       int64             `json:"uptime_seconds"`
	QPS          float64           `json:"qps"`
	Connections  ConnectionStats   `json:"connections"`
	BufferPool   BufferPoolStats   `json:"buffer_pool"`
	InnoDB       InnoDBStats       `json:"innodb"`
	Replication  ReplicationStats  `json:"replication"`
	Engines      []EngineInfo      `json:"engines"`
	SchemaStats  []SchemaSize      `json:"schema_stats"`
	KeyVariables map[string]string `json:"key_variables"`
}

// ConnectionStats summarises thread and connection counters.
type ConnectionStats struct {
	MaxConnections   int64 `json:"max_connections"`
	CurrentActive    int64 `json:"current_active"`
	CurrentSleeping  int64 `json:"current_sleeping"`
	TotalConnections int64 `json:"total_connections"`
	AbortedConnects  int64 `json:"aborted_connects"`
}

// BufferPoolStats captures InnoDB buffer pool sizing and efficiency.
type BufferPoolStats struct {
	SizeBytes    int64   `json:"size_bytes"`
	TotalPages   int64   `json:"total_pages"`
	FreePages    int64   `json:"free_pages"`
	DirtyPages   int64   `json:"dirty_pages"`
	ReadRequests int64   `json:"read_requests"`
	Reads        int64   `json:"reads"`
	HitRatio     float64 `json:"hit_ratio_pct"`
}

// InnoDBStats captures row-level operation and lock counters.
type InnoDBStats struct {
	RowsRead     int64 `json:"rows_read"`
	RowsInserted int64 `json:"rows_inserted"`
	RowsUpdated  int64 `json:"rows_updated"`
	RowsDeleted  int64 `json:"rows_deleted"`
	RowLockWaits int64 `json:"row_lock_waits"`
	RowLockTime  int64 `json:"row_lock_time_ms"`
	Deadlocks    int64 `json:"deadlocks"`
}

// ReplicationStats captures replica lag and thread state.
// SecondsBehind is nil when the instance is not a replica or the value is NULL.
type ReplicationStats struct {
	IsReplica       bool   `json:"is_replica"`
	SlaveIORunning  string `json:"slave_io_running"`
	SlaveSQLRunning string `json:"slave_sql_running"`
	SecondsBehind   *int64 `json:"seconds_behind_master"`
}

// EngineInfo describes an available storage engine.
type EngineInfo struct {
	Engine       string `json:"engine"`
	Support      string `json:"support"`
	Transactions string `json:"transactions"`
}

// SchemaSize reports the table count and on-disk footprint of a user schema.
type SchemaSize struct {
	Schema    string `json:"schema"`
	Tables    int64  `json:"tables"`
	SizeBytes int64  `json:"size_bytes"`
}

// keyVariableNames is the list of GLOBAL VARIABLES we surface to the LLM.
var keyVariableNames = []string{
	"innodb_buffer_pool_size",
	"max_connections",
	"innodb_log_file_size",
	"innodb_flush_method",
	"innodb_file_per_table",
	"slow_query_log",
	"long_query_time",
}

// CollectSummary gathers a structured snapshot of a MySQL instance's health.
// It replaces pt-mysql-summary with direct SQL queries that return typed data
// instead of free-form text.
func CollectSummary(ctx context.Context, db *sql.DB) (*MySQLSummary, error) {
	s := &MySQLSummary{
		KeyVariables: make(map[string]string),
	}

	// 1. Server info
	if err := collectServerInfo(ctx, db, s); err != nil {
		return nil, fmt.Errorf("collecting server info: %w", err)
	}

	// 2. SHOW GLOBAL STATUS → map
	status, err := showToMap(ctx, db, "SHOW GLOBAL STATUS")
	if err != nil {
		return nil, fmt.Errorf("collecting global status: %w", err)
	}

	// 3. SHOW GLOBAL VARIABLES → map
	variables, err := showToMap(ctx, db, "SHOW GLOBAL VARIABLES")
	if err != nil {
		return nil, fmt.Errorf("collecting global variables: %w", err)
	}

	// Populate from status/variables
	populateUptime(status, s)
	populateConnections(status, variables, s)
	populateBufferPool(status, variables, s)
	populateInnoDB(status, s)
	populateKeyVariables(variables, s)

	// 4. Storage engines
	if err := collectEngines(ctx, db, s); err != nil {
		return nil, fmt.Errorf("collecting engines: %w", err)
	}

	// 5. Schema sizes
	if err := collectSchemaStats(ctx, db, s); err != nil {
		return nil, fmt.Errorf("collecting schema stats: %w", err)
	}

	// 6. Replication
	if err := collectReplication(ctx, db, s); err != nil {
		return nil, fmt.Errorf("collecting replication status: %w", err)
	}

	return s, nil
}

// collectServerInfo reads @@version and @@hostname.
func collectServerInfo(ctx context.Context, db *sql.DB, s *MySQLSummary) error {
	var hostname string
	return db.QueryRowContext(ctx, "SELECT @@version, @@hostname").Scan(&s.Version, &hostname)
}

// showToMap runs a SHOW … statement that returns (Variable_name, Value) rows
// and returns them as a lower-cased key map.
func showToMap(ctx context.Context, db *sql.DB, query string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("executing %s: %w", query, err)
	}
	defer rows.Close() //nolint:errcheck

	m := make(map[string]string, 512)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, fmt.Errorf("scanning %s row: %w", query, err)
		}
		m[strings.ToLower(name)] = value
	}
	return m, rows.Err()
}

// statusToInt64 extracts a numeric global status value, returning 0 for missing
// or non-numeric entries. Status counters that don't exist yet (e.g. after a
// fresh restart) are safely treated as zero.
func statusToInt64(m map[string]string, key string) int64 {
	v, ok := m[strings.ToLower(key)]
	if !ok {
		return 0
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

// statusToFloat64 is the float counterpart of statusToInt64.
func statusToFloat64(m map[string]string, key string) float64 {
	v, ok := m[strings.ToLower(key)]
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(v, 64)
	return f
}

func populateUptime(status map[string]string, s *MySQLSummary) {
	s.Uptime = statusToInt64(status, "Uptime")
	questions := statusToFloat64(status, "Questions")
	if s.Uptime > 0 {
		s.QPS = questions / float64(s.Uptime)
	}
}

func populateConnections(status, variables map[string]string, s *MySQLSummary) {
	s.Connections = ConnectionStats{
		MaxConnections:   statusToInt64(variables, "max_connections"),
		CurrentActive:    statusToInt64(status, "Threads_running"),
		CurrentSleeping:  statusToInt64(status, "Threads_connected") - statusToInt64(status, "Threads_running"),
		TotalConnections: statusToInt64(status, "Connections"),
		AbortedConnects:  statusToInt64(status, "Aborted_connects"),
	}
}

func populateBufferPool(status, variables map[string]string, s *MySQLSummary) {
	readRequests := statusToInt64(status, "Innodb_buffer_pool_read_requests")
	reads := statusToInt64(status, "Innodb_buffer_pool_reads")

	var hitRatio float64
	if readRequests > 0 {
		hitRatio = (1 - float64(reads)/float64(readRequests)) * 100
	}

	s.BufferPool = BufferPoolStats{
		SizeBytes:    statusToInt64(variables, "innodb_buffer_pool_size"),
		TotalPages:   statusToInt64(status, "Innodb_buffer_pool_pages_total"),
		FreePages:    statusToInt64(status, "Innodb_buffer_pool_pages_free"),
		DirtyPages:   statusToInt64(status, "Innodb_buffer_pool_pages_dirty"),
		ReadRequests: readRequests,
		Reads:        reads,
		HitRatio:     hitRatio,
	}
}

func populateInnoDB(status map[string]string, s *MySQLSummary) {
	s.InnoDB = InnoDBStats{
		RowsRead:     statusToInt64(status, "Innodb_rows_read"),
		RowsInserted: statusToInt64(status, "Innodb_rows_inserted"),
		RowsUpdated:  statusToInt64(status, "Innodb_rows_updated"),
		RowsDeleted:  statusToInt64(status, "Innodb_rows_deleted"),
		RowLockWaits: statusToInt64(status, "Innodb_row_lock_waits"),
		RowLockTime:  statusToInt64(status, "Innodb_row_lock_time"),
		Deadlocks:    statusToInt64(status, "Innodb_deadlocks"),
	}
}

func populateKeyVariables(variables map[string]string, s *MySQLSummary) {
	for _, name := range keyVariableNames {
		if v, ok := variables[name]; ok {
			s.KeyVariables[name] = v
		}
	}
}

func collectEngines(ctx context.Context, db *sql.DB, s *MySQLSummary) error {
	rows, err := db.QueryContext(ctx,
		"SELECT ENGINE, SUPPORT, TRANSACTIONS FROM information_schema.ENGINES WHERE SUPPORT != 'NO'")
	if err != nil {
		return fmt.Errorf("querying engines: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	for rows.Next() {
		var e EngineInfo
		var txn sql.NullString
		if err := rows.Scan(&e.Engine, &e.Support, &txn); err != nil {
			return fmt.Errorf("scanning engine row: %w", err)
		}
		e.Transactions = txn.String
		s.Engines = append(s.Engines, e)
	}
	return rows.Err()
}

func collectSchemaStats(ctx context.Context, db *sql.DB, s *MySQLSummary) error {
	query := `
		SELECT TABLE_SCHEMA,
		       COUNT(*)                                       AS tables,
		       COALESCE(SUM(DATA_LENGTH + INDEX_LENGTH), 0)   AS size
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
		GROUP BY TABLE_SCHEMA
		ORDER BY size DESC`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("querying schema stats: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	for rows.Next() {
		var ss SchemaSize
		if err := rows.Scan(&ss.Schema, &ss.Tables, &ss.SizeBytes); err != nil {
			return fmt.Errorf("scanning schema stats row: %w", err)
		}
		s.SchemaStats = append(s.SchemaStats, ss)
	}
	return rows.Err()
}

func collectReplication(ctx context.Context, db *sql.DB, s *MySQLSummary) error {
	rows, err := db.QueryContext(ctx, "SHOW SLAVE STATUS")
	if err != nil {
		// Some environments (e.g. Aurora readers) may disallow SHOW SLAVE STATUS.
		// Treat as "not a replica" rather than a fatal error.
		s.Replication = ReplicationStats{IsReplica: false}
		return nil
	}
	defer rows.Close() //nolint:errcheck

	// SHOW SLAVE STATUS returns 0 rows on a primary.
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("reading slave status columns: %w", err)
	}
	if !rows.Next() {
		s.Replication = ReplicationStats{IsReplica: false}
		return rows.Err()
	}

	// Build a dynamic scan target — the column set varies across MySQL versions.
	values := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return fmt.Errorf("scanning slave status: %w", err)
	}

	// Index by column name for stable access.
	colMap := make(map[string]sql.NullString, len(cols))
	for i, name := range cols {
		colMap[name] = values[i]
	}

	s.Replication.IsReplica = true
	s.Replication.SlaveIORunning = colMap["Slave_IO_Running"].String
	s.Replication.SlaveSQLRunning = colMap["Slave_SQL_Running"].String

	if sbm, ok := colMap["Seconds_Behind_Master"]; ok && sbm.Valid {
		n, err := strconv.ParseInt(sbm.String, 10, 64)
		if err == nil {
			s.Replication.SecondsBehind = &n
		}
	}

	return rows.Err()
}
