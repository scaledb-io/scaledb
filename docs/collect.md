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
# hostname: prod-db-01              # override instance identity (for tunnels/proxies)

output:
  type: local                        # "local" or "s3"
  path: ./scaledb-data/              # when type=local
  flush_interval: 5m                 # time trigger — flush every N (default: 5m, -1 = disabled)
  flush_size: 128MB                  # size trigger — flush when buffer exceeds N (default: 128MB, -1 = disabled)
  flush_rows: 1000000                # row trigger — flush when buffer exceeds N rows (default: 1000000, -1 = disabled)

collect:
  interval: 60s                      # polling interval (min 10s, max 24h)
  schemas: true                      # collect table structures and indexes
  query_samples: false               # capture real SQL text (PII risk — see below)
  reconnect_after: 3                 # consecutive total-failure polls before reconnecting (default: 3)
  give_up_after: "-1"                # exit if ALL instances unreachable for this long; -1 = never (default: -1)

daemon:
  pidfile: /var/run/scaledb.pid
  logfile: /var/log/scaledb/collect.log
```

### S3 output

```yaml
output:
  type: s3
  bucket: s3://my-bucket/scaledb/    # bucket name with optional prefix
  region: us-east-1                  # optional, uses AWS default if empty
  # endpoint: http://localhost:9000  # optional, for MinIO/Cloudflare R2/etc.
```

Authentication uses the standard AWS credential chain: environment variables (`AWS_ACCESS_KEY_ID`), `~/.aws/credentials`, IAM instance role, etc. No ScaleDB-specific credential configuration needed.

**S3-compatible storage** (MinIO, Cloudflare R2, Backblaze B2):

```yaml
output:
  type: s3
  bucket: my-bucket
  endpoint: https://your-r2-account.r2.cloudflarestorage.com
  region: auto
```

### Smart batching (flush config)

By default, the collector buffers rows in memory and writes larger, fewer Parquet files — better for query engines like DuckDB, Athena, and Spark. Three configurable triggers control when data is flushed to disk/S3:

| Trigger | Config key | Default | Disable |
|---|---|---|---|
| Time | `flush_interval` | `5m` | `-1` |
| Size (estimated) | `flush_size` | `128MB` | `-1` |
| Row count | `flush_rows` | `1000000` | `-1` |

At least one trigger must be active. When any trigger fires for an instance, all buffered data for that instance is written. On shutdown, remaining data is always flushed.

```yaml
# Example: aggressive batching for fewer files
output:
  type: local
  path: ./scaledb-data/
  flush_interval: 10m
  flush_size: 256MB
  flush_rows: -1         # disable row trigger, rely on time + size
```

Data lands in S3 with the same Hive-style partition layout as local disk:

```
s3://my-bucket/scaledb/metrics/instance_id=my-writer/date=2026-04-24/chunk_001.parquet
```

Query directly from S3 with DuckDB:

```sql
-- DuckDB can read S3 natively
SET s3_region = 'us-east-1';
SET s3_access_key_id = '<key>';
SET s3_secret_access_key = '<secret>';

SELECT * FROM 's3://my-bucket/scaledb/metrics/**/*.parquet' LIMIT 10;
```

Or with AWS Athena — create a table pointing at the S3 prefix and query with standard SQL.

### Password resolution

`password_from` supports two modes:
- `env:VAR_NAME` — reads from environment variable (recommended)
- Plain string — used as-is (avoid in committed config files)

### Instance identity (tunnels and proxies)

When connecting through an SSH tunnel, `kubectl port-forward`, or any proxy, the connection target is typically `127.0.0.1` — which is meaningless as a partition label. The collector automatically resolves the real instance identity by querying the remote server:

1. `@@aurora_server_id` — Aurora instances return their real name (e.g. `my-cluster-instance-1`)
2. `@@hostname` — all MySQL servers report their OS hostname
3. Falls back to the connection target if both fail

For cases where auto-detection isn't useful (e.g. `@@hostname` returns `ip-10-0-1-234`), set `hostname:` explicitly:

```yaml
host: 127.0.0.1
port: 13881
user: scout
password_from: env:MYSQL_PASSWORD
hostname: prod-db-01    # partitions become instance_id=prod-db-01
```

### Aurora vs single host

When `cluster:` is set, the collector queries `information_schema.replica_host_status` to auto-discover all instances (writer + readers) and collects from each one independently.

When `host:` is set, it connects to that single endpoint — useful for SSH tunnels, non-Aurora MySQL, or RDS single instances.

### Connection resilience

When connecting through SSH tunnels, SSM sessions, or `kubectl port-forward`, the proxy can die (session timeout, laptop sleep, pod restart) leaving the collector holding dead TCP sockets. The collector detects this and automatically reconnects.

**How it works:**

1. Each poll cycle, the collector tracks whether an instance returned data or failed completely (all 5 queries failed).
2. After `reconnect_after` consecutive total failures (default: 3), the old connection pool is closed and a fresh one is created.
3. If the reconnect fails (tunnel still down), retries use exponential backoff: 1s → 2s → 4s → ... → 60s cap.
4. When the tunnel comes back, the next reconnect attempt succeeds and polling resumes normally.

Partial failures (e.g., one query fails but others succeed) do **not** count toward the reconnect threshold — the connection is working, just some queries have issues.

**`give_up_after`** controls what happens when *all* instances are unreachable simultaneously. Set it to a duration like `30m` to exit after 30 minutes of total failure (useful when a process supervisor like systemd will restart the collector). The default `-1` means never give up — the collector keeps retrying indefinitely.

```yaml
collect:
  reconnect_after: 3     # reconnect after 3 consecutive total failures (default)
  give_up_after: 30m     # exit after 30 minutes if nothing is reachable
```

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

### Important: each file is a point-in-time snapshot

The collector writes cumulative counters every poll cycle. When you query across all files, you'll see the same queries/metrics repeated — once per cycle with slightly updated values. **Always filter by `timestamp`** to get clean results.

```sql
-- Latest snapshot only
SELECT * FROM 'scaledb-data/query-digests/**/*.parquet'
WHERE timestamp = (SELECT max(timestamp) FROM 'scaledb-data/query-digests/**/*.parquet');

-- Snapshot at a specific time
SELECT * FROM 'scaledb-data/query-digests/**/*.parquet'
WHERE timestamp = '2026-04-24 18:30:50';

-- Change over the last hour (delta between newest and oldest snapshot)
SELECT digest_text[:70] AS query,
       max(exec_count) - min(exec_count) AS new_execs,
       (max(sum_timer_wait) - min(sum_timer_wait)) / 1e12 AS added_wait_sec
FROM 'scaledb-data/query-digests/**/*.parquet'
WHERE timestamp >= '2026-04-24 17:30:00'
GROUP BY digest_text
HAVING new_execs > 0
ORDER BY added_wait_sec DESC
LIMIT 20;

-- Available timestamps (one per poll cycle)
SELECT DISTINCT timestamp FROM 'scaledb-data/metrics/**/*.parquet' ORDER BY timestamp;
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
