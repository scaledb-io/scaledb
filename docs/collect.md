# scaledb collect

Continuous MySQL performance data collection. Connects to a MySQL or Aurora cluster, polls `performance_schema` metrics, and writes Hive-partitioned Parquet files to local disk.

## Quick start

```bash
# 1. Create a config file
cat > scaledb.yaml <<EOF
cluster: my-cluster.abc123.us-east-1.rds.amazonaws.com
user: scout
password_from: env:MYSQL_PASSWORD
output:
  type: local
  path: ./scaledb-data/
collect:
  interval: 60s
  schemas: true
EOF

# 2. Run
export MYSQL_PASSWORD=<your-password>
scaledb collect --config scaledb.yaml
```

Data appears in `./scaledb-data/` as Parquet files.

## Configuration

```yaml
# Aurora cluster (auto-discovers writer + readers)
cluster: my-cluster.abc123.us-east-1.rds.amazonaws.com

# Or single MySQL host (no topology discovery)
# host: 127.0.0.1
# port: 3306

user: scout
password_from: env:MYSQL_PASSWORD    # or a literal string

output:
  type: local                        # local disk (S3 coming soon)
  path: ./scaledb-data/

collect:
  interval: 60s                      # polling interval (min 10s, max 24h)
  schemas: true                      # collect table structures and indexes
  query_samples: false               # capture real SQL text (PII risk — see below)

daemon:
  pidfile: /var/run/scaledb.pid
  logfile: /var/log/scaledb/collect.log
```

### Password resolution

`password_from` supports two modes:
- `env:VAR_NAME` — reads from environment variable (recommended)
- Plain string — used as-is (avoid in committed config files)

### Aurora vs single host

When `cluster:` is set, the collector queries `information_schema.replica_host_status` to auto-discover all instances (writer + readers) and collects from each one independently.

When `host:` is set, it connects to that single endpoint — useful for SSH tunnels, non-Aurora MySQL, or RDS single instances.

## Run modes

```bash
# Foreground — JSON logs to stdout (Docker, systemd Type=simple)
scaledb collect --config scaledb.yaml

# Daemon — background, pidfile, logs to file (bare metal, systemd Type=forking)
scaledb collect --config scaledb.yaml -D

# Stop a running daemon
scaledb collect --stop
```

## Data layout

Output is Hive-partitioned Parquet — compatible with DuckDB, Athena, Spark, Trino, ClickHouse, pandas, and any tool that reads Parquet.

```
scaledb-data/
├── metrics/
│   └── instance_id=my-writer/
│       └── date=2026-04-24/
│           └── chunk_001.parquet
├── query-digests/
│   └── instance_id=my-writer/
│       └── date=2026-04-24/
│           └── chunk_001.parquet
├── index-usage/
│   └── instance_id=my-writer/
│       └── date=2026-04-24/
│           └── chunk_001.parquet
├── wait-events/          (only when query_samples: true)
│   └── ...
└── samples/              (only when query_samples: true)
    └── ...
```

## Querying with DuckDB

[DuckDB](https://duckdb.org/) is the fastest way to analyze collected data — zero server, single binary, reads Parquet natively.

```bash
brew install duckdb   # or: https://duckdb.org/docs/installation
```

### Metrics

```sql
-- All metrics from the latest collection
SELECT metric_name, value, timestamp
FROM 'scaledb-data/metrics/**/*.parquet'
ORDER BY timestamp DESC, metric_name;

-- Thread count over time
SELECT timestamp, value
FROM 'scaledb-data/metrics/**/*.parquet'
WHERE metric_name = 'global_status.Threads_running'
ORDER BY timestamp;

-- InnoDB row operations
SELECT metric_name, value
FROM 'scaledb-data/metrics/**/*.parquet'
WHERE metric_name LIKE 'global_status.Innodb_rows_%'
ORDER BY value DESC;
```

### Top queries

```sql
-- Top 10 queries by total wait time
SELECT
    digest_text[:80] AS query,
    exec_count,
    sum_timer_wait / 1e12 AS total_wait_sec,
    (sum_timer_wait / exec_count) / 1e9 AS avg_ms,
    sum_rows_examined
FROM 'scaledb-data/query-digests/**/*.parquet'
ORDER BY sum_timer_wait DESC
LIMIT 10;

-- Queries creating temp tables on disk
SELECT digest_text[:80] AS query, exec_count, sum_created_tmp_disk_tables
FROM 'scaledb-data/query-digests/**/*.parquet'
WHERE sum_created_tmp_disk_tables > 0
ORDER BY sum_created_tmp_disk_tables DESC
LIMIT 10;

-- Queries not using indexes
SELECT digest_text[:80] AS query, exec_count, sum_no_index_used
FROM 'scaledb-data/query-digests/**/*.parquet'
WHERE sum_no_index_used > 0
ORDER BY sum_no_index_used DESC
LIMIT 10;
```

### Index usage

```sql
-- Unused indexes (0 reads) — candidates for removal
SELECT schema_name, table_name, index_name, count_read, count_write
FROM 'scaledb-data/index-usage/**/*.parquet'
WHERE count_read = 0 AND index_name != 'PRIMARY'
ORDER BY count_write DESC;

-- Most-read indexes
SELECT schema_name, table_name, index_name, count_read, count_write
FROM 'scaledb-data/index-usage/**/*.parquet'
ORDER BY count_read DESC
LIMIT 20;

-- Write-heavy indexes (candidates for review)
SELECT schema_name, table_name, index_name, count_write, count_read,
       CASE WHEN count_read = 0 THEN 'UNUSED' ELSE round(count_write::float / count_read, 2)::varchar END AS write_read_ratio
FROM 'scaledb-data/index-usage/**/*.parquet'
WHERE count_write > 1000000
ORDER BY count_write DESC
LIMIT 20;
```

### Wait events (when query_samples is enabled)

```sql
-- Top wait events by total wait time
SELECT event_name, sum(count) AS total_count, sum(total_wait) / 1e12 AS total_wait_sec
FROM 'scaledb-data/wait-events/**/*.parquet'
GROUP BY event_name
ORDER BY total_wait_sec DESC
LIMIT 20;

-- Wait events for a specific query digest
SELECT event_name, sum(count) AS count, sum(total_wait) / 1e12 AS wait_sec
FROM 'scaledb-data/wait-events/**/*.parquet'
WHERE parent_digest = '<digest-hash>'
GROUP BY event_name
ORDER BY wait_sec DESC;
```

### Cross-instance comparison

```sql
-- Compare QPS across instances
SELECT instance_id, value AS queries
FROM 'scaledb-data/metrics/**/*.parquet'
WHERE metric_name = 'global_status.Queries'
ORDER BY instance_id;

-- Instance with most connections
SELECT instance_id, value AS threads_connected
FROM 'scaledb-data/metrics/**/*.parquet'
WHERE metric_name = 'global_status.Threads_connected'
ORDER BY value DESC;
```

### Filtering by date

The Hive partition layout enables date filtering without scanning all files:

```sql
-- Last 7 days of digests
SELECT *
FROM 'scaledb-data/query-digests/instance_id=*/date=2026-04-*/chunk_*.parquet'
WHERE timestamp >= '2026-04-17 00:00:00';

-- Specific date
SELECT *
FROM 'scaledb-data/metrics/instance_id=my-writer/date=2026-04-24/*.parquet';
```

## Querying with Python

```python
import duckdb

# DuckDB from Python — same SQL, zero setup
con = duckdb.connect()
df = con.sql("""
    SELECT metric_name, value, timestamp
    FROM 'scaledb-data/metrics/**/*.parquet'
""").df()
print(df)
```

Or with pandas/pyarrow directly:

```python
import pandas as pd

metrics = pd.read_parquet('scaledb-data/metrics/')
digests = pd.read_parquet('scaledb-data/query-digests/')

# Top queries
top = digests.nlargest(10, 'sum_timer_wait')[['digest_text', 'exec_count', 'sum_timer_wait']]
print(top)
```

## Data collected

### Metrics (default)

| Source | Metric prefix | Description |
|---|---|---|
| `SHOW GLOBAL STATUS` | `global_status.*` | 13 curated variables (Queries, Threads, InnoDB rows, etc.) |
| `information_schema.PROCESSLIST` | `processlist.*` | Total, active, sleeping thread counts |
| `information_schema.INNODB_METRICS` | `innodb_metrics.*` | All enabled InnoDB counters |

### Query digests (default)

Top 100 queries by total wait time from `events_statements_summary_by_digest`. Includes execution counts, row statistics, temp table usage, and timestamps.

### Index usage (default)

Per-index read/write I/O counters from `table_io_waits_summary_by_index_usage`. Excludes system schemas.

### Query samples (opt-in: `query_samples: true`)

Real SQL text with actual parameter values captured from `events_statements_history_long`. These are not normalized — they contain the actual values passed in queries.

**WARNING:** Query samples may contain sensitive data (user IDs, emails, passwords, tokens) embedded in SQL statements. Enable only if your data handling policies permit capturing production SQL text.

### Wait events (opt-in: `query_samples: true`)

Wait events from `events_waits_history_long`, correlated to parent statements via the digest hash (QUID), aggregated into 5-second buckets.

## MySQL prerequisites

The collecting user needs:

```sql
CREATE USER 'scout'@'%' IDENTIFIED BY '<password>';
GRANT SELECT ON performance_schema.* TO 'scout'@'%';
GRANT PROCESS ON *.* TO 'scout'@'%';
```

For schema collection (`schemas: true`):

```sql
GRANT SELECT ON information_schema.* TO 'scout'@'%';
GRANT SELECT ON *.* TO 'scout'@'%';  -- needed for INFORMATION_SCHEMA to expose all tables
```

Aurora MySQL must have `performance_schema` enabled (it is by default). Verify:

```sql
SHOW VARIABLES LIKE 'performance_schema';
-- Should return ON
```

## Parquet schema reference

### metrics

| Column | Type | Description |
|---|---|---|
| instance_id | string | MySQL instance identifier |
| cluster_id | string | Cluster identifier |
| metric_name | string | e.g. `global_status.Queries` |
| value | float64 | Metric value |
| timestamp | string | UTC timestamp |

### query-digests

| Column | Type | Description |
|---|---|---|
| instance_id | string | |
| cluster_id | string | |
| digest | string | Query digest hash (QUID) |
| digest_text | string | Normalized SQL (parameters replaced with `?`) |
| schema_name | string | Default database |
| exec_count | uint64 | Total executions |
| sum_timer_wait | uint64 | Total wait time (picoseconds) |
| sum_rows_examined | uint64 | |
| sum_rows_sent | uint64 | |
| sum_rows_affected | uint64 | |
| sum_created_tmp_tables | uint64 | |
| sum_created_tmp_disk_tables | uint64 | |
| sum_no_index_used | uint64 | |
| first_seen | string | |
| last_seen | string | |
| timestamp | string | Collection timestamp |

### index-usage

| Column | Type | Description |
|---|---|---|
| instance_id | string | |
| cluster_id | string | |
| schema_name | string | Database name |
| table_name | string | |
| index_name | string | |
| count_read | uint64 | Total read I/O waits |
| count_write | uint64 | Total write I/O waits |
| timestamp | string | |

### samples (opt-in)

| Column | Type | Description |
|---|---|---|
| instance_id | string | |
| cluster_id | string | |
| event_id | uint64 | performance_schema event ID |
| digest | string | Links to query-digests via QUID |
| sql_text | string | **Real SQL with actual values** |
| timer_wait | uint64 | Statement execution time (picoseconds) |
| rows_examined | uint64 | |
| rows_sent | uint64 | |
| rows_affected | uint64 | |
| created_tmp_tables | uint64 | |
| created_tmp_disk_tables | uint64 | |
| no_index_used | uint64 | |
| current_schema | string | |
| timestamp | string | |

### wait-events (opt-in)

| Column | Type | Description |
|---|---|---|
| instance_id | string | |
| cluster_id | string | |
| parent_digest | string | QUID of parent statement |
| event_name | string | e.g. `wait/io/table/sql/handler` |
| count | uint64 | Events in this 5-second bucket |
| total_wait | uint64 | Sum of wait times (picoseconds) |
| timestamp | string | 5-second bucket boundary |
