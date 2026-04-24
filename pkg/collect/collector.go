package collect

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/scaledb-io/scaledb/pkg/output"
)

const (
	discoveryInterval = 5 * time.Minute
	schemaInterval    = 1 * time.Hour
	flushInterval     = 5 * time.Minute
	shutdownTimeout   = 15 * time.Second
)

// Run is the top-level entry point for `scaledb collect`.
// It loads config, sets up the collector, and enters the main loop.
func Run(ctx context.Context, configPath string, daemonMode bool) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}

	// Daemon mode: re-exec with marker env var.
	if daemonMode && os.Getenv("SCALEDB_DAEMON") != "1" {
		return daemonize(cfg)
	}

	// Set up logger.
	var logger *slog.Logger
	if os.Getenv("SCALEDB_DAEMON") == "1" {
		f, err := os.OpenFile(cfg.Daemon.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("opening log file %s: %w", cfg.Daemon.LogFile, err)
		}
		defer f.Close()
		logger = slog.New(slog.NewJSONHandler(f, nil))
	} else {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	return run(ctx, cfg, logger)
}

// run is the core collector loop.
func run(ctx context.Context, cfg *Config, logger *slog.Logger) error {
	// Resolve password.
	password, err := cfg.ResolvePassword()
	if err != nil {
		return err
	}

	// Parse interval.
	interval, err := cfg.ParseInterval()
	if err != nil {
		return err
	}

	// PII warning for query samples.
	if cfg.Collect.QuerySamples {
		fmt.Fprintln(os.Stderr, "WARNING: query_samples is enabled. Collected data may contain "+
			"sensitive information (user IDs, emails, passwords) embedded in SQL queries. "+
			"Ensure your data handling policies permit this.")
	}

	// Connect to cluster/host endpoint.
	var host string
	port := cfg.Port
	if cfg.IsAurora() {
		host = cfg.Cluster
		port = 3306
	} else {
		host = cfg.Host
	}

	clusterDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s&readTimeout=30s&parseTime=true",
		cfg.User, password, host, port)

	clusterDB, err := sql.Open("mysql", clusterDSN)
	if err != nil {
		return fmt.Errorf("opening cluster connection: %w", err)
	}
	defer clusterDB.Close()
	clusterDB.SetMaxOpenConns(2)
	clusterDB.SetConnMaxLifetime(10 * time.Minute)

	if err := clusterDB.PingContext(ctx); err != nil {
		return fmt.Errorf("connecting to %s:%d: %w", host, port, err)
	}

	// Discover topology.
	endpoint := fmt.Sprintf("%s:%d", host, port)
	var instances []DiscoveredInstance
	var clusterID string

	if cfg.IsAurora() {
		// Aurora: auto-discover writer + readers from cluster endpoint.
		discovered, err := DiscoverTopology(ctx, clusterDB, host)
		if err != nil {
			return fmt.Errorf("discovering topology: %w", err)
		}
		instances = discovered
		clusterID = host
		if idx := strings.Index(host, "."); idx > 0 {
			clusterID = host[:idx]
		}
	} else {
		// Single host (direct or tunnel): skip discovery, use as-is.
		instances = []DiscoveredInstance{{
			ServerID: host,
			Endpoint: host,
			IsWriter: true,
		}}
		clusterID = host
	}

	logger.Info("topology discovered",
		"cluster", clusterID,
		"instances", len(instances),
		"endpoint", endpoint,
	)

	// Open per-instance connections.
	conns := make(map[string]*sql.DB)
	for _, inst := range instances {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s&readTimeout=30s&parseTime=true",
			cfg.User, password, inst.Endpoint, port)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			logger.Warn("failed to open connection", "instance", inst.ServerID, "error", err)
			continue
		}
		db.SetMaxOpenConns(3)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(10 * time.Minute)

		if err := db.PingContext(ctx); err != nil {
			logger.Warn("failed to ping instance", "instance", inst.ServerID, "error", err)
			db.Close()
			continue
		}

		conns[inst.ServerID] = db
		logger.Info("connected to instance",
			"instance", inst.ServerID,
			"endpoint", inst.Endpoint,
			"is_writer", inst.IsWriter,
		)
	}
	defer func() {
		for _, db := range conns {
			db.Close()
		}
	}()

	if len(conns) == 0 {
		return fmt.Errorf("no instances reachable")
	}

	// Set up output writer.
	var writer output.DataWriter
	switch cfg.Output.Type {
	case "s3":
		s3w, err := output.NewS3Writer(ctx, output.S3Config{
			Bucket:   cfg.Output.Bucket,
			Region:   cfg.Output.Region,
			Endpoint: cfg.Output.Endpoint,
		})
		if err != nil {
			return fmt.Errorf("creating S3 writer: %w", err)
		}
		writer = output.WrapS3(s3w)
		logger.Info("output: S3", "bucket", cfg.Output.Bucket, "region", cfg.Output.Region)
	default:
		lw, err := output.NewLocalWriter(cfg.Output.Path)
		if err != nil {
			return err
		}
		writer = output.WrapLocal(lw)
		logger.Info("output: local", "path", cfg.Output.Path)
	}
	defer writer.Close()

	// Set up signal handling.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// Tickers.
	pollTicker := time.NewTicker(interval)
	defer pollTicker.Stop()

	discoveryTicker := time.NewTicker(discoveryInterval)
	defer discoveryTicker.Stop()

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	var schemaTicker *time.Ticker
	if cfg.Collect.Schemas {
		schemaTicker = time.NewTicker(schemaInterval)
		defer schemaTicker.Stop()
	}

	var wg sync.WaitGroup

	// Run one immediate poll cycle.
	logger.Info("starting collection",
		"interval", interval.String(),
		"schemas", cfg.Collect.Schemas,
		"query_samples", cfg.Collect.QuerySamples,
	)
	pollAll(ctx, conns, clusterID, writer, logger, &wg)

	for {
		select {
		case sig := <-sigCh:
			logger.Info("received signal, shutting down", "signal", sig.String())
			cancel()
			return shutdown(logger, &wg, writer, conns)

		case <-ctx.Done():
			return shutdown(logger, &wg, writer, conns)

		case <-pollTicker.C:
			pollAll(ctx, conns, clusterID, writer, logger, &wg)

		case <-discoveryTicker.C:
			newInstances, err := DiscoverTopology(ctx, clusterDB, host)
			if err != nil {
				logger.Warn("topology rediscovery failed", "error", err)
				continue
			}
			updateConnections(ctx, conns, newInstances, cfg.User, password, port, logger)

		case <-flushTicker.C:
			if err := writer.Flush(); err != nil {
				logger.Warn("periodic flush failed", "error", err)
			}
		}

		// Schema ticker is optional.
		if schemaTicker != nil {
			select {
			case <-schemaTicker.C:
				// Schema collection will be added when we integrate with pkg/analyze.
				logger.Debug("schema collection tick (not yet implemented in OSS)")
			default:
			}
		}
	}
}

// pollAll fans out a poll for each connected instance.
func pollAll(
	ctx context.Context,
	conns map[string]*sql.DB,
	clusterID string,
	writer output.DataWriter,
	logger *slog.Logger,
	wg *sync.WaitGroup,
) {
	timestamp := time.Now().UTC().Format("2006-01-02 15:04:05")

	for instanceID, db := range conns {
		wg.Add(1)
		go func(instID string, instDB *sql.DB) {
			defer wg.Done()

			result, err := pollInstance(ctx, instDB, instID, clusterID, timestamp)
			if err != nil {
				logger.Warn("poll failed", "instance", instID, "error", err)
				return
			}

			// Write results.
			if len(result.Metrics) > 0 {
				if err := writer.WriteMetrics(instID, result.Metrics); err != nil {
					logger.Warn("write metrics failed", "instance", instID, "error", err)
				}
			}
			if len(result.Digests) > 0 {
				if err := writer.WriteDigests(instID, result.Digests); err != nil {
					logger.Warn("write digests failed", "instance", instID, "error", err)
				}
			}
			if len(result.IndexUsage) > 0 {
				if err := writer.WriteIndexUsage(instID, result.IndexUsage); err != nil {
					logger.Warn("write index usage failed", "instance", instID, "error", err)
				}
			}
			if len(result.Samples) > 0 {
				if err := writer.WriteQuerySamples(instID, result.Samples); err != nil {
					logger.Warn("write samples failed", "instance", instID, "error", err)
				}
			}
			if len(result.WaitEvents) > 0 {
				if err := writer.WriteWaitEvents(instID, result.WaitEvents); err != nil {
					logger.Warn("write wait events failed", "instance", instID, "error", err)
				}
			}
		}(instanceID, db)
	}
}

// updateConnections reconciles the current connections with a new topology.
// Only adds instances it can actually connect to. Never removes existing
// working connections unless the new set has replacements that work.
func updateConnections(
	ctx context.Context,
	conns map[string]*sql.DB,
	newInstances []DiscoveredInstance,
	user, password string,
	port int,
	logger *slog.Logger,
) {
	// Try to connect to new instances first.
	reachable := make(map[string]*sql.DB)
	for _, inst := range newInstances {
		if _, ok := conns[inst.ServerID]; ok {
			// Already connected — keep it.
			reachable[inst.ServerID] = conns[inst.ServerID]
			continue
		}
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s&readTimeout=30s&parseTime=true",
			user, password, inst.Endpoint, port)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			logger.Warn("failed to connect to new instance", "instance", inst.ServerID, "error", err)
			continue
		}
		db.SetMaxOpenConns(3)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(10 * time.Minute)

		if err := db.PingContext(ctx); err != nil {
			logger.Warn("failed to ping new instance", "instance", inst.ServerID, "error", err)
			db.Close()
			continue
		}

		reachable[inst.ServerID] = db
		logger.Info("new instance discovered", "instance", inst.ServerID, "endpoint", inst.Endpoint)
	}

	// Only update the connection map if the new topology has reachable instances.
	// This prevents killing working connections when discovery returns endpoints
	// we can't reach (e.g., Aurora instance names from behind an SSH tunnel).
	if len(reachable) == 0 {
		logger.Warn("topology rediscovery found no reachable instances, keeping existing connections")
		return
	}

	// Close connections that aren't in the new reachable set.
	for id, db := range conns {
		if _, ok := reachable[id]; !ok {
			logger.Info("instance removed from topology", "instance", id)
			db.Close()
			delete(conns, id)
		}
	}

	// Add new reachable connections.
	for id, db := range reachable {
		conns[id] = db
	}
}

// shutdown performs graceful shutdown.
func shutdown(
	logger *slog.Logger,
	wg *sync.WaitGroup,
	writer output.DataWriter,
	conns map[string]*sql.DB,
) error {
	// Drain in-flight polls.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("all polls drained")
	case <-time.After(shutdownTimeout):
		logger.Warn("drain timeout, some polls may still be running")
	}

	// Flush output.
	if err := writer.Close(); err != nil {
		logger.Warn("flush on shutdown failed", "error", err)
	}

	// Close connections.
	for id, db := range conns {
		db.Close()
		delete(conns, id)
	}

	logger.Info("collector stopped")
	return nil
}

// daemonize re-execs the current process with the SCALEDB_DAEMON env var set.
// The parent writes the child PID to the pidfile and exits.
func daemonize(cfg *Config) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("finding executable path: %w", err)
	}

	// Ensure log directory exists.
	logDir := cfg.Daemon.LogFile[:strings.LastIndex(cfg.Daemon.LogFile, "/")]
	if logDir != "" {
		os.MkdirAll(logDir, 0755)
	}

	env := append(os.Environ(), "SCALEDB_DAEMON=1")
	attr := &os.ProcAttr{
		Env: env,
		Files: []*os.File{
			os.Stdin,
			nil, // stdout will be redirected by child
			nil, // stderr will be redirected by child
		},
	}

	proc, err := os.StartProcess(exe, os.Args, attr)
	if err != nil {
		return fmt.Errorf("starting daemon process: %w", err)
	}

	// Write pidfile.
	if err := os.WriteFile(cfg.Daemon.PIDFile, []byte(strconv.Itoa(proc.Pid)), 0644); err != nil {
		return fmt.Errorf("writing pidfile %s: %w", cfg.Daemon.PIDFile, err)
	}

	fmt.Fprintf(os.Stderr, "scaledb collect daemon started (pid: %d)\n", proc.Pid)
	proc.Release()
	return nil
}

// StopDaemon reads the pidfile and sends SIGTERM to the running daemon.
func StopDaemon(pidfile string) error {
	if pidfile == "" {
		pidfile = "/var/run/scaledb.pid"
	}

	data, err := os.ReadFile(pidfile)
	if err != nil {
		return fmt.Errorf("reading pidfile %s: %w", pidfile, err)
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return fmt.Errorf("parsing pid from %s: %w", pidfile, err)
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("finding process %d: %w", pid, err)
	}

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("sending SIGTERM to pid %d: %w", pid, err)
	}

	// Remove pidfile after successful signal.
	os.Remove(pidfile)

	fmt.Fprintf(os.Stderr, "sent SIGTERM to pid %d\n", pid)
	return nil
}
