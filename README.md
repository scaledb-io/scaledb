# scaledb

MySQL analysis CLI. Checks variables, finds duplicate indexes, and summarizes instance health. Read-only — never writes to your database.

## Install

```bash
# Homebrew (macOS / Linux)
brew install scaledb-io/tap/scaledb

# Go
go install github.com/scaledb-io/scaledb/cmd/scaledb@latest

# Binary
# Download from https://github.com/scaledb-io/scaledb/releases
```

## Usage

```bash
# Check MySQL variables against 72 best-practice rules
scaledb check variables --host 127.0.0.1 --user root --password secret

# Find duplicate and redundant indexes
scaledb check indexes --host 127.0.0.1 --user root --password secret

# Instance summary (version, buffer pool, InnoDB stats, replication)
scaledb check summary --host 127.0.0.1 --user root --password secret

# JSON output
scaledb check variables --host 127.0.0.1 --user root --password secret --format json

# Filter by category
scaledb check variables --host 127.0.0.1 --user root --password secret --category innodb
```

### Variable categories

`innodb`, `replication`, `connections`, `memory`, `security`, `performance_schema`, `general`

### Using a my.cnf file

```bash
# Reads host, port, user, password from the [client] section
scaledb check variables --defaults-file ~/.my.cnf

# Explicit flags override defaults file values
scaledb check variables --defaults-file ~/.my.cnf --category innodb
```

### Password from environment

```bash
export MYSQL_PWD=secret
scaledb check variables --host 127.0.0.1 --user root --password-env MYSQL_PWD
```

## What it checks

| Tool | What it does |
|---|---|
| **Variable Advisor** | 72 rules across 7 categories (InnoDB, replication, connections, memory, security, performance_schema, general). Auto-detects Aurora and skips inapplicable rules. |
| **Duplicate Key Checker** | Finds indexes that are left-prefixes of other indexes. Generates `DROP INDEX` statements. |
| **MySQL Summary** | Version, uptime, QPS, buffer pool hit ratio, InnoDB row ops, connections, schema sizes, replication lag. |

See [docs/rules.md](docs/rules.md) for the full rule catalog with descriptions and severity levels.

## As a library

The analysis package is importable:

```go
import "github.com/scaledb-io/scaledb/pkg/analyze"

result, err := analyze.CheckVariables(ctx, db, analyze.CheckVariablesOptions{})
findings, err := analyze.CheckDuplicateKeys(ctx, db)
summary, err := analyze.CollectSummary(ctx, db)
```

## Requirements

- MySQL 5.7+ or 8.0+ (including Aurora MySQL)
- A user with `SELECT` on `information_schema` and `performance_schema`
- No special configuration needed — works with default MySQL grants

## License

Apache 2.0 — see [LICENSE](LICENSE).
