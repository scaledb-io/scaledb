# CLAUDE.md — scaledb

## What this is

Standalone open-source MySQL analysis CLI and importable Go library.

**Module:** `github.com/scaledb-io/scaledb`
**Current version:** v0.0.2

## Repository Structure

```
scaledb/
├── cmd/scaledb/main.go          # CLI entrypoint: check + version
├── pkg/analyze/                  # Public, importable analysis package
│   ├── variable_advisor.go       # MySQL variable checks (72 rules, 7 categories)
│   ├── variable_rules.go         # Rule definitions + Aurora detection
│   ├── rules_innodb.go           # InnoDB-specific rules
│   ├── rules_replication.go      # Replication rules
│   ├── rules_connections.go      # Connection/thread rules
│   ├── rules_memory.go           # Memory/buffer rules
│   ├── rules_security.go         # Security rules
│   ├── rules_perfschema.go       # Performance Schema rules
│   ├── rules_general.go          # General MySQL rules
│   ├── duplicate_key_checker.go  # Duplicate/redundant index detection
│   ├── mysql_summary.go          # MySQL instance summary collector
│   ├── schema_collector.go       # Schema + table metadata collector
│   └── runner.go                 # RunAll orchestrator + Envelope type
├── pkg/analyze/*_test.go         # 15 test files, 224+ tests
├── go.mod                        # Single dep: go-sql-driver/mysql
├── go.sum
├── .github/workflows/ci.yml     # lint + test + build
├── .github/workflows/release.yml # goreleaser on tag push
├── .gitignore
├── .golangci.yml
├── .goreleaser.yml               # Cross-platform release builds
├── CLAUDE.md
├── LICENSE                       # Apache 2.0
└── README.md
```

## CLI Commands

```
scaledb check variables [--host H --user U --password P] [--category C] [--format json]
scaledb check indexes   [--host H --user U --password P] [--format json]
scaledb check summary   [--host H --user U --password P] [--format json]
scaledb version
```

## Language & Dependencies

- **Go 1.24+**
- Single external dependency: `github.com/go-sql-driver/mysql`
- Binary size: ~6MB
- All analysis is pure SQL queries against `INFORMATION_SCHEMA`, `performance_schema`, and `SHOW VARIABLES/STATUS`

## Coding Conventions

- Standard Go project layout: `cmd/` for entrypoints, `pkg/` for public importable packages
- `pkg/analyze/` has **zero internal dependencies** — only stdlib + `database/sql`
- Tests: table-driven, in `_test.go` files alongside source, use `go-sqlmock`
- Errors: wrap with `fmt.Errorf("doing X: %w", err)`
- No global state; the analyze functions take `context.Context` + `*sql.DB`
- Version injected via `-ldflags "-X main.version=..."` at build time

## Testing

```bash
go test ./...                    # All tests (no MySQL needed — uses go-sqlmock)
go test -race -count=1 ./...     # With race detector
golangci-lint run ./...          # Lint (v2 config)
go build -o scaledb ./cmd/scaledb/  # Build binary
```

## What NOT To Do

- Do NOT add heavy dependencies — keep `pkg/analyze/` stdlib-only (except `database/sql` driver)
- Do NOT break the `pkg/analyze` public API — downstream consumers depend on `CheckVariables()`, `CheckDuplicateKeys()`, `CollectSummary()`, `RunAll()`, and the `Envelope`/`TopicForTool` types
- Do NOT write to databases — all analysis is read-only queries
