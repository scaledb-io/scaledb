// Command scaledb is a standalone CLI for running MySQL analysis tools.
//
// Usage:
//
//	scaledb check variables [flags]          — run variable advisor
//	scaledb check indexes [flags]           — run duplicate key checker
//	scaledb check summary [flags]           — run mysql summary
//	scaledb collect --config <file> [-D]    — continuous data collection
//	scaledb collect --stop                  — stop a running daemon
//	scaledb version                         — print version
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/scaledb-io/scaledb/pkg/analyze"
	"github.com/scaledb-io/scaledb/pkg/collect"
)

// version is set via ldflags at build time.
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	args := os.Args[1:]
	subcommand := args[0]

	switch subcommand {
	case "collect":
		runCollect(args[1:])
	case "check":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: scaledb check <variables|indexes|summary> [flags]\n")
			os.Exit(1)
		}
		runCheck(args[1], args[2:])
	case "version", "--version", "-v":
		fmt.Printf("scaledb %s\n", version)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: scaledb <command> [flags]

Commands:
  check variables    Run variable advisor
  check indexes      Run duplicate key checker
  check summary      Run mysql summary
  collect            Continuous data collection to Parquet files
  version            Print version

Check flags:
  --defaults-file Read connection options from a my.cnf file ([client] section)
  --host          MySQL host (required unless in defaults file)
  --port          MySQL port (default 3306)
  --user          MySQL user (required unless in defaults file)
  --password      MySQL password
  --password-env  Environment variable containing MySQL password
  --format        Output format: "table" (default) or "json"
  --category      Filter variable checks by category

Collect flags:
  --config        Path to YAML config file (required)
  -D              Run as daemon (background, pidfile, logfile)
  --stop          Stop a running daemon (reads pidfile, sends SIGTERM)
`)
}

// cliFlags holds parsed CLI flag values.
type cliFlags struct {
	defaultsFile string
	host         string
	port         int
	user         string
	password     string
	passwordEnv  string
	format       string
	category     string
}

func registerFlags(fs *flag.FlagSet, f *cliFlags) {
	fs.StringVar(&f.defaultsFile, "defaults-file", "", "Read MySQL connection options from this file (my.cnf format, [client] section)")
	fs.StringVar(&f.host, "host", "", "MySQL host")
	fs.IntVar(&f.port, "port", 3306, "MySQL port")
	fs.StringVar(&f.user, "user", "", "MySQL user")
	fs.StringVar(&f.password, "password", "", "MySQL password")
	fs.StringVar(&f.passwordEnv, "password-env", "", "Environment variable containing MySQL password")
	fs.StringVar(&f.format, "format", "table", "Output format: table or json")
	fs.StringVar(&f.category, "category", "", "Filter variable checks to a specific category (innodb, replication, connections, memory, security, performance_schema, general)")
}

// applyDefaultsFile reads connection options from a my.cnf-format file.
// Only the [client] section is read. Explicit CLI flags take precedence.
func (f *cliFlags) applyDefaultsFile(fs *flag.FlagSet) error {
	if f.defaultsFile == "" {
		return nil
	}

	opts, err := parseDefaultsFile(f.defaultsFile)
	if err != nil {
		return fmt.Errorf("reading defaults file: %w", err)
	}

	// Only apply values that weren't explicitly set on the command line.
	explicit := make(map[string]bool)
	fs.Visit(func(fl *flag.Flag) { explicit[fl.Name] = true })

	if v, ok := opts["host"]; ok && !explicit["host"] {
		f.host = v
	}
	if v, ok := opts["port"]; ok && !explicit["port"] {
		p, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid port in defaults file: %w", err)
		}
		f.port = p
	}
	if v, ok := opts["user"]; ok && !explicit["user"] {
		f.user = v
	}
	if v, ok := opts["password"]; ok && !explicit["password"] {
		f.password = v
	}

	return nil
}

// parseDefaultsFile reads key=value pairs from the [client] section of a my.cnf file.
func parseDefaultsFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close() //nolint:errcheck

	result := make(map[string]string)
	inClient := false

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments.
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}

		// Section header.
		if line[0] == '[' {
			inClient = strings.EqualFold(line, "[client]")
			continue
		}

		if !inClient {
			continue
		}

		// Parse key=value or key (bare keys are ignored).
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		// Strip surrounding quotes.
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}

		// Normalize key names: my.cnf uses dashes or underscores interchangeably.
		key = strings.ReplaceAll(key, "-", "_")

		result[key] = value
	}

	return result, scanner.Err()
}

func (f *cliFlags) validate() error {
	if f.host == "" {
		return fmt.Errorf("--host is required")
	}
	if f.user == "" {
		return fmt.Errorf("--user is required")
	}
	if f.password == "" && f.passwordEnv == "" {
		return fmt.Errorf("--password or --password-env is required")
	}
	if f.format != "table" && f.format != "json" {
		return fmt.Errorf("--format must be 'table' or 'json'")
	}
	return nil
}

func (f *cliFlags) resolvePassword() (string, error) {
	if f.password != "" {
		return f.password, nil
	}
	v := os.Getenv(f.passwordEnv)
	if v == "" {
		return "", fmt.Errorf("environment variable %s is empty or unset", f.passwordEnv)
	}
	return v, nil
}

func openDB(f *cliFlags) (*sql.DB, error) {
	pw, err := f.resolvePassword()
	if err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s&readTimeout=30s&writeTimeout=10s&parseTime=true",
		f.user, pw, f.host, f.port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening mysql connection: %w", err)
	}
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close() //nolint:errcheck
		return nil, fmt.Errorf("pinging mysql %s:%d: %w", f.host, f.port, err)
	}

	return db, nil
}

// ---------------------------------------------------------------------------
// scaledb check <tool>
// ---------------------------------------------------------------------------

func runCheck(checkName string, args []string) {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	var f cliFlags
	registerFlags(fs, &f)
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if err := f.applyDefaultsFile(fs); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if err := f.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	db, err := openDB(&f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	switch checkName {
	case "variables":
		runCheckVariables(ctx, db, &f)
	case "indexes":
		runCheckIndexes(ctx, db, &f)
	case "summary":
		runCheckSummary(ctx, db, &f)
	default:
		fmt.Fprintf(os.Stderr, "Unknown check: %s (must be variables, indexes, or summary)\n", checkName)
		os.Exit(1)
	}
}

func runCheckVariables(ctx context.Context, db *sql.DB, f *cliFlags) {
	result, err := analyze.CheckVariables(ctx, db, analyze.CheckVariablesOptions{
		Category: f.category,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if f.format == "json" {
		printJSON(result)
	} else {
		printVariableResult(result)
	}
}

func runCheckIndexes(ctx context.Context, db *sql.DB, f *cliFlags) {
	findings, err := analyze.CheckDuplicateKeys(ctx, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if f.format == "json" {
		printJSON(findings)
	} else {
		printDuplicateKeyFindings(findings)
	}
}

func runCheckSummary(ctx context.Context, db *sql.DB, f *cliFlags) {
	summary, err := analyze.CollectSummary(ctx, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if f.format == "json" {
		printJSON(summary)
	} else {
		printMySQLSummary(summary)
	}
}

// ---------------------------------------------------------------------------
// collect command
// ---------------------------------------------------------------------------

func runCollect(args []string) {
	fs := flag.NewFlagSet("collect", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to YAML config file")
	daemon := fs.Bool("D", false, "Run as daemon (background)")
	stop := fs.Bool("stop", false, "Stop a running daemon")
	fs.Parse(args)

	if *stop {
		if err := collect.StopDaemon(""); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *configPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --config is required\n")
		fmt.Fprintf(os.Stderr, "Usage: scaledb collect --config <file> [-D]\n")
		os.Exit(1)
	}

	ctx := context.Background()
	if err := collect.Run(ctx, *configPath, *daemon); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// Output formatting — table mode
// ---------------------------------------------------------------------------

// printVariableResult prints findings grouped by category.
func printVariableResult(result *analyze.CheckVariablesResult) {
	header := fmt.Sprintf("\n=== Variable Advisor (%d findings", len(result.Findings))
	if result.RulesSkipped > 0 {
		header += fmt.Sprintf(", %d skipped on Aurora", result.RulesSkipped)
	}
	header += ") ==="
	fmt.Println(header)

	if len(result.Findings) == 0 {
		fmt.Println("  No issues found.")
		return
	}

	// Print in category order.
	catOrder := []analyze.RuleCategory{
		analyze.CategoryInnoDB,
		analyze.CategoryReplication,
		analyze.CategoryConnections,
		analyze.CategoryMemory,
		analyze.CategorySecurity,
		analyze.CategoryPerfSchema,
		analyze.CategoryGeneral,
	}
	for _, cat := range catOrder {
		findings, ok := result.ByCategory[cat]
		if !ok || len(findings) == 0 {
			continue
		}
		fmt.Printf("\n── %s (%d findings) ──\n", categoryLabel(cat), len(findings))
		for _, f := range findings {
			sev := strings.ToUpper(f.Severity)
			fmt.Printf("  %-8s %-35s %-12s %s\n", sev, f.Variable, f.CurrentValue, f.Description)
		}
	}
}

func categoryLabel(cat analyze.RuleCategory) string {
	switch cat {
	case analyze.CategoryInnoDB:
		return "InnoDB"
	case analyze.CategoryReplication:
		return "Replication"
	case analyze.CategoryConnections:
		return "Connections"
	case analyze.CategoryMemory:
		return "Memory"
	case analyze.CategorySecurity:
		return "Security"
	case analyze.CategoryPerfSchema:
		return "Performance Schema"
	case analyze.CategoryGeneral:
		return "General"
	default:
		return string(cat)
	}
}

func printDuplicateKeyFindings(findings []analyze.DuplicateKeyFinding) {
	fmt.Printf("\n=== Duplicate Key Checker (%d findings) ===\n", len(findings))
	if len(findings) == 0 {
		fmt.Println("  No duplicate indexes found.")
		return
	}
	for _, f := range findings {
		fmt.Printf("  %s.%s: %s is a left-prefix of %s\n",
			f.Database, f.Table, f.DuplicateIndex, f.OverlapsWith)
		fmt.Printf("    DROP: %s\n", f.DropStatement)
	}
}

func printMySQLSummary(s *analyze.MySQLSummary) {
	fmt.Println("\n=== MySQL Summary ===")

	uptimeDays := s.Uptime / 86400
	fmt.Printf("  Version: %s  |  Uptime: %dd  |  QPS: %.0f\n",
		s.Version, uptimeDays, s.QPS)

	fmt.Printf("  Buffer Pool: %s (%.2f%% hit ratio, %d free pages)\n",
		humanBytes(s.BufferPool.SizeBytes), s.BufferPool.HitRatio, s.BufferPool.FreePages)

	fmt.Printf("  InnoDB: %s reads, %s inserts, %s updates, %s deletes\n",
		humanCount(s.InnoDB.RowsRead),
		humanCount(s.InnoDB.RowsInserted),
		humanCount(s.InnoDB.RowsUpdated),
		humanCount(s.InnoDB.RowsDeleted))

	fmt.Printf("  Connections: %d active / %d max\n",
		s.Connections.CurrentActive, s.Connections.MaxConnections)

	if len(s.SchemaStats) > 0 {
		schemas := make([]string, 0, len(s.SchemaStats))
		for _, ss := range s.SchemaStats {
			schemas = append(schemas, fmt.Sprintf("%s %s", ss.Schema, humanBytes(ss.SizeBytes)))
		}
		fmt.Printf("  Schemas: %d (%s)\n", len(s.SchemaStats), strings.Join(schemas, ", "))
	}

	if s.Replication.IsReplica {
		lag := "NULL"
		if s.Replication.SecondsBehind != nil {
			lag = fmt.Sprintf("%ds", *s.Replication.SecondsBehind)
		}
		fmt.Printf("  Replication: IO=%s SQL=%s Lag=%s\n",
			s.Replication.SlaveIORunning, s.Replication.SlaveSQLRunning, lag)
	}
}

// ---------------------------------------------------------------------------
// Output formatting — json mode
// ---------------------------------------------------------------------------

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// Human-readable formatting helpers
// ---------------------------------------------------------------------------

func humanBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
	)
	switch {
	case b >= tb:
		return fmt.Sprintf("%.1fTB", float64(b)/float64(tb))
	case b >= gb:
		return fmt.Sprintf("%.1fGB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1fKB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

func humanCount(n int64) string {
	const (
		thousand = 1000
		million  = 1000 * thousand
		billion  = 1000 * million
	)
	switch {
	case n >= billion:
		return fmt.Sprintf("%.1fB", float64(n)/float64(billion))
	case n >= million:
		return fmt.Sprintf("%.1fM", float64(n)/float64(million))
	case n >= thousand:
		return fmt.Sprintf("%.1fK", float64(n)/float64(thousand))
	default:
		return fmt.Sprintf("%d", n)
	}
}
