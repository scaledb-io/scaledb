package collect

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	maxBackoff     = 60 * time.Second
	initialBackoff = 1 * time.Second
)

// openFunc creates a new *sql.DB from a DSN. Replaceable in tests.
type openFunc func(dsn string) (*sql.DB, error)

var defaultOpen openFunc = func(dsn string) (*sql.DB, error) {
	return sql.Open("mysql", dsn)
}

// connState tracks per-instance connection health and handles reconnection.
type connState struct {
	db               *sql.DB
	serverID         string
	endpoint         string
	port             int
	user             string
	password         string
	maxOpenConns     int
	maxIdleConns     int // 0 = Go default
	consecutiveFails int
	lastAttempt      time.Time
	backoff          time.Duration
	open             openFunc
}

func newConnState(db *sql.DB, serverID, endpoint string, port int, user, password string, maxOpen, maxIdle int) *connState {
	return &connState{
		db:           db,
		serverID:     serverID,
		endpoint:     endpoint,
		port:         port,
		user:         user,
		password:     password,
		maxOpenConns: maxOpen,
		maxIdleConns: maxIdle,
		open:         defaultOpen,
	}
}

// RecordSuccess resets failure state after a successful poll.
func (cs *connState) RecordSuccess() {
	cs.consecutiveFails = 0
	cs.backoff = 0
}

// RecordFailure increments the consecutive failure counter.
func (cs *connState) RecordFailure() {
	cs.consecutiveFails++
}

// NeedsReconnect returns true if consecutive failures have reached the threshold.
func (cs *connState) NeedsReconnect(threshold int) bool {
	return cs.consecutiveFails >= threshold
}

// ReadyToReconnect returns true if enough backoff time has elapsed.
func (cs *connState) ReadyToReconnect() bool {
	if cs.backoff == 0 {
		return true
	}
	return time.Since(cs.lastAttempt) >= cs.backoff
}

// Reconnect closes the old connection pool and creates a fresh one.
// On failure, advances the backoff timer. On success, resets all failure state.
func (cs *connState) Reconnect(ctx context.Context, logger *slog.Logger) error {
	if !cs.ReadyToReconnect() {
		remaining := cs.backoff - time.Since(cs.lastAttempt)
		return fmt.Errorf("backoff not elapsed, next retry in %s", remaining.Round(time.Second))
	}

	cs.lastAttempt = time.Now()

	// Close old pool — dead sockets never recover when the tunnel listener
	// is a different process. The closed *sql.DB stays in cs.db so that
	// subsequent polls fail fast (returns "database is closed") rather than
	// panicking on a nil pointer. Only replaced on successful reconnect.
	if cs.db != nil {
		_ = cs.db.Close()
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s&readTimeout=30s&parseTime=true",
		cs.user, cs.password, cs.endpoint, cs.port)

	db, err := cs.open(dsn)
	if err != nil {
		cs.advanceBackoff()
		return fmt.Errorf("opening connection to %s: %w", cs.serverID, err)
	}

	db.SetMaxOpenConns(cs.maxOpenConns)
	if cs.maxIdleConns > 0 {
		db.SetMaxIdleConns(cs.maxIdleConns)
	}
	db.SetConnMaxLifetime(10 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		cs.advanceBackoff()
		return fmt.Errorf("pinging %s (%s:%d): %w", cs.serverID, cs.endpoint, cs.port, err)
	}

	cs.db = db
	cs.consecutiveFails = 0
	cs.backoff = 0
	return nil
}

func (cs *connState) advanceBackoff() {
	if cs.backoff == 0 {
		cs.backoff = initialBackoff
	} else {
		cs.backoff *= 2
		if cs.backoff > maxBackoff {
			cs.backoff = maxBackoff
		}
	}
}

// Close closes the underlying database connection.
func (cs *connState) Close() error {
	if cs.db != nil {
		return cs.db.Close()
	}
	return nil
}
