package collect

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

// RelevantGlobalStatus is the set of SHOW GLOBAL STATUS variables we collect.
var RelevantGlobalStatus = map[string]struct{}{
	"Queries":                          {},
	"Threads_connected":                {},
	"Threads_running":                  {},
	"Innodb_buffer_pool_reads":         {},
	"Innodb_buffer_pool_read_requests": {},
	"Innodb_rows_read":                 {},
	"Innodb_rows_inserted":             {},
	"Innodb_rows_updated":              {},
	"Innodb_rows_deleted":              {},
	"Connections":                      {},
	"Aborted_connects":                 {},
	"Slow_queries":                     {},
	"Uptime":                           {},
}

// QueryDigests queries performance_schema.events_statements_summary_by_digest
// for the top 100 statements by total wait time.
func QueryDigests(ctx context.Context, db *sql.DB, instanceID, clusterID, timestamp string) ([]QueryDigest, error) {
	const q = `
		SELECT
			DIGEST,
			DIGEST_TEXT,
			SCHEMA_NAME,
			COUNT_STAR,
			SUM_TIMER_WAIT,
			SUM_ROWS_EXAMINED,
			SUM_ROWS_SENT,
			SUM_ROWS_AFFECTED,
			SUM_CREATED_TMP_TABLES,
			SUM_CREATED_TMP_DISK_TABLES,
			SUM_NO_INDEX_USED,
			FIRST_SEEN,
			LAST_SEEN
		FROM performance_schema.events_statements_summary_by_digest
		WHERE DIGEST IS NOT NULL AND COUNT_STAR > 0
		ORDER BY SUM_TIMER_WAIT DESC
		LIMIT 100`

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying digests: %w", err)
	}
	defer func() { _ = rows.Close() }()

	const dtFmt = "2006-01-02 15:04:05"

	var digests []QueryDigest
	for rows.Next() {
		var (
			d          QueryDigest
			digest     sql.NullString
			digestText sql.NullString
			schemaName sql.NullString
			firstSeen  sql.NullTime
			lastSeen   sql.NullTime
		)
		if err := rows.Scan(
			&digest,
			&digestText,
			&schemaName,
			&d.ExecCount,
			&d.SumTimerWait,
			&d.SumRowsExamined,
			&d.SumRowsSent,
			&d.SumRowsAffected,
			&d.SumCreatedTmpTables,
			&d.SumCreatedTmpDiskTables,
			&d.SumNoIndexUsed,
			&firstSeen,
			&lastSeen,
		); err != nil {
			return nil, fmt.Errorf("scanning digest row: %w", err)
		}

		d.InstanceID = instanceID
		d.ClusterID = clusterID
		d.Timestamp = timestamp
		d.Digest = digest.String
		d.DigestText = digestText.String
		d.SchemaName = schemaName.String
		if firstSeen.Valid {
			d.FirstSeen = firstSeen.Time.UTC().Format(dtFmt)
		}
		if lastSeen.Valid {
			d.LastSeen = lastSeen.Time.UTC().Format(dtFmt)
		}

		digests = append(digests, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating digest rows: %w", err)
	}
	return digests, nil
}

// QueryGlobalStatus runs SHOW GLOBAL STATUS and returns metrics for the
// curated set of variables in RelevantGlobalStatus.
func QueryGlobalStatus(ctx context.Context, db *sql.DB, instanceID, clusterID, timestamp string) ([]Metric, error) {
	rows, err := db.QueryContext(ctx, "SHOW GLOBAL STATUS")
	if err != nil {
		return nil, fmt.Errorf("querying global status: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var metrics []Metric
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, fmt.Errorf("scanning global status row: %w", err)
		}
		if _, ok := RelevantGlobalStatus[name]; !ok {
			continue
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			continue
		}
		metrics = append(metrics, Metric{
			InstanceID: instanceID,
			ClusterID:  clusterID,
			MetricName: "global_status." + name,
			Value:      v,
			Timestamp:  timestamp,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating global status rows: %w", err)
	}
	return metrics, nil
}

// QueryIndexUsage queries performance_schema.table_io_waits_summary_by_index_usage
// for read/write I/O counts per index, excluding system schemas.
// Returns structured IndexUsage rows (not flat metrics) for Parquet ergonomics.
func QueryIndexUsage(ctx context.Context, db *sql.DB, instanceID, clusterID, timestamp string) ([]IndexUsage, error) {
	const q = `
		SELECT
			OBJECT_SCHEMA,
			OBJECT_NAME,
			INDEX_NAME,
			COUNT_READ,
			COUNT_WRITE
		FROM performance_schema.table_io_waits_summary_by_index_usage
		WHERE INDEX_NAME IS NOT NULL
			AND OBJECT_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')`

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying index usage: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var usage []IndexUsage
	for rows.Next() {
		var u IndexUsage
		if err := rows.Scan(&u.SchemaName, &u.TableName, &u.IndexName, &u.CountRead, &u.CountWrite); err != nil {
			return nil, fmt.Errorf("scanning index usage row: %w", err)
		}
		u.InstanceID = instanceID
		u.ClusterID = clusterID
		u.Timestamp = timestamp
		usage = append(usage, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating index usage rows: %w", err)
	}
	return usage, nil
}

// QueryProcesslist queries information_schema.PROCESSLIST and returns three
// metrics: processlist.total, processlist.active, processlist.sleeping.
func QueryProcesslist(ctx context.Context, db *sql.DB, instanceID, clusterID, timestamp string) ([]Metric, error) {
	const q = `
		SELECT
			COUNT(*)                                    AS total,
			SUM(IF(COMMAND != 'Sleep', 1, 0))           AS active,
			SUM(IF(COMMAND = 'Sleep', 1, 0))            AS sleeping
		FROM information_schema.PROCESSLIST`

	var total, active, sleeping float64
	if err := db.QueryRowContext(ctx, q).Scan(&total, &active, &sleeping); err != nil {
		return nil, fmt.Errorf("querying processlist: %w", err)
	}

	return []Metric{
		{InstanceID: instanceID, ClusterID: clusterID, MetricName: "processlist.total", Value: total, Timestamp: timestamp},
		{InstanceID: instanceID, ClusterID: clusterID, MetricName: "processlist.active", Value: active, Timestamp: timestamp},
		{InstanceID: instanceID, ClusterID: clusterID, MetricName: "processlist.sleeping", Value: sleeping, Timestamp: timestamp},
	}, nil
}

// QueryInnoDBMetrics queries information_schema.INNODB_METRICS for all enabled
// counters. Each counter becomes a metric with name "innodb_metrics.<NAME>".
func QueryInnoDBMetrics(ctx context.Context, db *sql.DB, instanceID, clusterID, timestamp string) ([]Metric, error) {
	const q = `SELECT NAME, COUNT FROM information_schema.INNODB_METRICS WHERE STATUS = 'enabled'`

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying innodb metrics: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var metrics []Metric
	for rows.Next() {
		var name string
		var count int64
		if err := rows.Scan(&name, &count); err != nil {
			return nil, fmt.Errorf("scanning innodb metrics row: %w", err)
		}
		metrics = append(metrics, Metric{
			InstanceID: instanceID,
			ClusterID:  clusterID,
			MetricName: "innodb_metrics." + name,
			Value:      float64(count),
			Timestamp:  timestamp,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating innodb metrics rows: %w", err)
	}
	return metrics, nil
}

// PollInstance runs all 5 poller queries against a single instance and returns
// the aggregated results. Failed queries are logged and skipped — partial
// results are returned as long as at least one query succeeds.
func PollInstance(ctx context.Context, db *sql.DB, instanceID, clusterID, timestamp string) (*PollResult, error) {
	result := &PollResult{}
	var firstErr error

	record := func(name string, fn func() error) {
		if err := fn(); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", name, err)
			}
		}
	}

	record("digests", func() error {
		d, err := QueryDigests(ctx, db, instanceID, clusterID, timestamp)
		result.Digests = d
		return err
	})

	record("global_status", func() error {
		m, err := QueryGlobalStatus(ctx, db, instanceID, clusterID, timestamp)
		result.Metrics = append(result.Metrics, m...)
		return err
	})

	record("index_usage", func() error {
		u, err := QueryIndexUsage(ctx, db, instanceID, clusterID, timestamp)
		result.IndexUsage = u
		return err
	})

	record("processlist", func() error {
		m, err := QueryProcesslist(ctx, db, instanceID, clusterID, timestamp)
		result.Metrics = append(result.Metrics, m...)
		return err
	})

	record("innodb_metrics", func() error {
		m, err := QueryInnoDBMetrics(ctx, db, instanceID, clusterID, timestamp)
		result.Metrics = append(result.Metrics, m...)
		return err
	})

	// Return partial results if at least some data was collected.
	if len(result.Metrics) == 0 && len(result.Digests) == 0 && len(result.IndexUsage) == 0 {
		return nil, fmt.Errorf("all queries failed: %w", firstErr)
	}

	return result, firstErr
}
