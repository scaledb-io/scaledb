# Variable Advisor Rules

72 rules across 7 categories. Aurora MySQL is detected automatically — rules tagged `aurora-skip` are excluded, and `aurora-downgrade` rules have their severity reduced.

## InnoDB (20 rules)

| Severity | Variable | Description |
|---|---|---|
| warn | `innodb_buffer_pool_size` | Buffer pool size is unconfigured |
| warn | `innodb_log_file_size` | Log file size is at the default value, not usable for production |
| note | `innodb_data_file_path` | Auto-extending InnoDB files consume disk space that is difficult to reclaim |
| note | `innodb_flush_method` | Should be O_DIRECT to avoid double-buffering on production systems |
| warn | `innodb_file_per_table` | Should be ON for better space management and tablespace recovery |
| warn | `innodb_flush_log_at_trx_commit` | Not set to 1; server is not strictly ACID-compliant |
| warn | `innodb_doublewrite` | Disabled; data is unsafe without partial-page write protection |
| note | `innodb_flush_neighbors` | Should be 0 for SSD storage (MySQL 8.0 default) |
| note | `innodb_io_capacity` | At the conservative default of 200; tune for actual storage IOPS |
| note | `innodb_io_capacity_max` | At the default of 2000; should match actual storage burst IOPS |
| warn | `innodb_log_buffer_size` | Exceeds 16MB; generally should not exceed 16MB |
| warn | `innodb_force_recovery` | Non-zero value; forced recovery mode is for temporary corruption recovery only |
| warn | `innodb_fast_shutdown` | Not set to 1; non-default shutdown can cause performance issues |
| warn | `innodb_lock_wait_timeout` | Exceeds 120 seconds; can cause system overload if locks are not released |
| note | `innodb_dedicated_server` | Enabled; MySQL auto-configures buffer pool, log file size, and flush method |
| note | `innodb_print_all_deadlocks` | Disabled; enable to monitor deadlock frequency in the error log |
| warn | `innodb_stats_persistent` | Disabled; non-persistent statistics cause query plan instability on restart |
| note | `innodb_checksum_algorithm` | Not crc32; crc32 is the fastest and recommended for MySQL 8.0+ |
| warn | `innodb_redo_log_capacity` | At the 100MB default (8.0.30+); increase for production workloads |
| note | `innodb_buffer_pool_instances` | Buffer pool exceeds 1GB with only 1 instance; recommend 1 per GB up to 64 |

## Replication (15 rules)

| Severity | Variable | Description |
|---|---|---|
| warn | `expire_logs_days` | Binary logs enabled but automatic purging is not |
| warn | `log_bin` | Binary logging disabled; point-in-time recovery and replication impossible |
| warn | `binlog_format` | Not ROW; ROW-based replication is the proven standard |
| warn | `sync_binlog` | Not 1; not flushing binlog every transaction risks data loss on crash |
| warn | `gtid_mode` | Not enabled; GTID-based replication is the operational standard |
| warn | `enforce_gtid_consistency` | Not ON while gtid_mode is ON; required for GTID-based replication |
| note | `binlog_row_image` | Not MINIMAL with ROW format; MINIMAL reduces replication bandwidth |
| note | `replica_parallel_workers` | Not configured; 4-8 workers recommended for parallel apply |
| warn | `replica_preserve_commit_order` | OFF with parallel workers > 1; replica can enter states the source never was in |
| note | `binlog_transaction_dependency_tracking` | Not WRITESET; WRITESET improves parallelization on replicas |
| warn | `binlog_expire_logs_seconds` | Binary logs enabled but set to 0 (no automatic purge) |
| warn | `log_replica_updates` | OFF; required for replication chains and Group Replication |
| note | `innodb_autoinc_lock_mode` | Not 2 (interleaved) with ROW-based replication |
| critical | `replica_skip_errors` | Configured; never skip replication errors to avoid silent data drift |
| note | `binlog_cache_size` | At the 32K default; recommend 512K+ for ROW-based replication |

## Connections (9 rules)

| Severity | Variable | Description |
|---|---|---|
| note | `max_connections` | Set very high; consider whether this is truly necessary |
| critical | `max_connections` | Above 5000; almost certainly misconfigured, can exhaust memory |
| warn | `thread_cache_size` | Set to 0; a new thread is created for every connection |
| note | `wait_timeout` | Exceeds the 8-hour default; idle connections waste resources |
| warn | `wait_timeout` | Below 60 seconds; may kill legitimate connections that are briefly idle |
| note | `interactive_timeout` | Differs from wait_timeout; these should usually match |
| note | `max_connect_errors` | Below 100000; a low value may block legitimate hosts after transient failures |
| note | `connect_timeout` | Exceeds 30 seconds; large value creates DoS vulnerability |
| note | `max_allowed_packet` | Below 64MB (the 8.0 default); may cause errors for large inserts or BLOBs |

## Memory (10 rules)

| Severity | Variable | Description |
|---|---|---|
| note | `sort_buffer_size` | Changed from default; should generally be left at its default |
| warn | `sort_buffer_size` | Exceeds 2MB; large values hurt performance due to allocation overhead |
| warn | `key_buffer_size` | At the default value; not good for most production systems |
| warn | `join_buffer_size` | Exceeds 4MB; allocated per join per session, causes memory pressure |
| warn | `read_buffer_size` | Exceeds 8MB; oversized buffers waste memory per session |
| warn | `read_rnd_buffer_size` | Exceeds 4MB; destabilizes the server under concurrent workloads |
| note | `tmp_table_size` | Larger than max_heap_table_size; effective limit is the minimum of the two |
| warn | `tmp_table_size` | Exceeds 256MB; very large in-memory temp tables cause severe memory pressure |
| note | `max_heap_table_size` | Less than tmp_table_size; should be >= tmp_table_size |
| note | `bulk_insert_buffer_size` | Exceeds 64MB; per-session allocation during bulk inserts |

## Security (7 rules)

| Severity | Variable | Description |
|---|---|---|
| warn | `local_infile` | Enabled; potential security exploit via LOAD DATA LOCAL INFILE |
| note | `skip_name_resolve` | OFF; DNS lookups on every connection add latency |
| note | `require_secure_transport` | OFF; clients can connect without TLS |
| warn | `sql_mode` | Missing STRICT_TRANS_TABLES; MySQL silently truncates data |
| note | `sql_mode` | Missing NO_ENGINE_SUBSTITUTION; CREATE TABLE may silently use a different engine |
| note | `default_authentication_plugin` | Not caching_sha2_password; older plugins are less secure |
| warn | `tls_version` | Includes TLSv1 or TLSv1.1; only TLSv1.2 and TLSv1.3 should be allowed |

## Performance Schema (1 rule)

| Severity | Variable | Description |
|---|---|---|
| warn | `performance_schema` | OFF; disables all instrumentation essential for monitoring and diagnostics |

## General (10 rules)

| Severity | Variable | Description |
|---|---|---|
| warn | `delay_key_write` | MyISAM index blocks are never flushed until necessary |
| warn | `myisam_recover_options` | Should be set to BACKUP,FORCE to ensure table corruption is noticed |
| note | `table_open_cache` | Below 4000 (the 8.0 default); causes frequent table opens |
| note | `table_definition_cache` | Below 2000; may cause frequent table definition reopens |
| note | `open_files_limit` | Below 65535; production servers need high file descriptor limits |
| warn | `log_output` | Set to TABLE; writing logs to tables has high performance impact |
| note | `character_set_server` | Not utf8mb4; the modern standard supporting full Unicode |
| note | `default_storage_engine` | Not InnoDB; the standard transactional engine for MySQL 8.0+ |
| note | `slow_query_log` | OFF; enable for performance monitoring and slow query analysis |
| note | `long_query_time` | Exceeds 10 seconds; recommend 1-2s for production |

# Duplicate Key Checker

Scans `INFORMATION_SCHEMA.STATISTICS` for redundant indexes where one index's columns are a left-prefix of another index on the same table.

**What it reports:**
- The redundant index and what it overlaps with
- Column lists for both indexes
- Ready-to-use `DROP INDEX` statement

**Exclusions:**
- System schemas (`mysql`, `information_schema`, `performance_schema`, `sys`)
- PRIMARY keys are never reported as duplicates
- When two indexes have identical columns, the non-unique one is the duplicate

# MySQL Summary

Collects instance health metrics via `SHOW GLOBAL STATUS`, `SHOW GLOBAL VARIABLES`, `INFORMATION_SCHEMA`, and `SHOW SLAVE STATUS`.

**What it reports:**
- **Server:** version, hostname, uptime, QPS
- **Buffer Pool:** size, hit ratio, free/dirty pages
- **InnoDB:** row operations (read/insert/update/delete), row lock waits, deadlocks
- **Connections:** active, sleeping, max, aborted
- **Schemas:** per-schema table count and disk size
- **Replication:** IO/SQL thread status, seconds behind master
- **Engines:** available storage engines with transaction support
- **Key Variables:** buffer pool size, max connections, log file size, flush method
