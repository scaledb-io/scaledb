package collect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

func TestConnState_FailureCounting(t *testing.T) {
	cs := &connState{}

	cs.RecordFailure()
	cs.RecordFailure()
	cs.RecordFailure()

	if cs.consecutiveFails != 3 {
		t.Errorf("consecutiveFails = %d, want 3", cs.consecutiveFails)
	}
	if !cs.NeedsReconnect(3) {
		t.Error("NeedsReconnect(3) = false, want true")
	}
	if cs.NeedsReconnect(4) {
		t.Error("NeedsReconnect(4) = true, want false")
	}
}

func TestConnState_RecordSuccessResets(t *testing.T) {
	cs := &connState{
		consecutiveFails: 5,
		backoff:          16 * time.Second,
	}

	cs.RecordSuccess()

	if cs.consecutiveFails != 0 {
		t.Errorf("consecutiveFails = %d, want 0", cs.consecutiveFails)
	}
	if cs.backoff != 0 {
		t.Errorf("backoff = %v, want 0", cs.backoff)
	}
}

func TestConnState_BackoffProgression(t *testing.T) {
	cs := &connState{}

	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		60 * time.Second, // capped
		60 * time.Second, // stays capped
	}

	for i, w := range want {
		cs.advanceBackoff()
		if cs.backoff != w {
			t.Errorf("step %d: backoff = %v, want %v", i+1, cs.backoff, w)
		}
	}
}

func TestConnState_ReconnectOpenFailure(t *testing.T) {
	cs := &connState{
		serverID: "test",
		endpoint: "localhost",
		port:     3306,
		user:     "u",
		password: "p",
		open: func(dsn string) (*sql.DB, error) {
			return nil, fmt.Errorf("injected open error")
		},
	}

	err := cs.Reconnect(context.Background(), slog.Default())
	if err == nil {
		t.Fatal("expected error from open")
	}
	if cs.backoff != initialBackoff {
		t.Errorf("backoff = %v, want %v", cs.backoff, initialBackoff)
	}
}

func TestConnState_ReconnectPingFailure(t *testing.T) {
	cs := &connState{
		serverID:     "test",
		endpoint:     "127.0.0.1",
		port:         1, // nothing listening
		user:         "u",
		password:     "p",
		maxOpenConns: 3,
		open:         defaultOpen,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := cs.Reconnect(ctx, slog.Default())
	if err == nil {
		t.Fatal("expected ping error")
	}
	if cs.backoff != initialBackoff {
		t.Errorf("backoff = %v, want %v", cs.backoff, initialBackoff)
	}
}

func TestConnState_ReconnectBackoffNotElapsed(t *testing.T) {
	cs := &connState{
		backoff:     10 * time.Second,
		lastAttempt: time.Now(),
	}

	err := cs.Reconnect(context.Background(), slog.Default())
	if err == nil {
		t.Fatal("expected backoff error")
	}
}

func TestConnState_ReconnectResetsOnSuccess(t *testing.T) {
	// Simulate a successful reconnect by injecting an openFunc that returns
	// a valid *sql.DB. We can't actually ping, so we test the open+backoff
	// logic indirectly: after two failures, backoff should be 2s. After a
	// successful Reconnect, everything resets.
	cs := &connState{
		serverID:         "test",
		endpoint:         "localhost",
		port:             3306,
		user:             "u",
		password:         "p",
		maxOpenConns:     3,
		consecutiveFails: 5,
		backoff:          4 * time.Second,
		lastAttempt:      time.Now().Add(-5 * time.Second), // backoff elapsed
		open:             defaultOpen,
	}

	// Reconnect will succeed at sql.Open but fail at Ping (no server).
	// This tests that backoff advances on ping failure.
	err := cs.Reconnect(context.Background(), slog.Default())
	if err == nil {
		t.Fatal("expected ping failure (no server)")
	}
	if cs.backoff != 8*time.Second {
		t.Errorf("backoff after ping failure = %v, want 8s", cs.backoff)
	}
	// consecutiveFails unchanged (only reset on full success).
	if cs.consecutiveFails != 5 {
		t.Errorf("consecutiveFails = %d, want 5 (unchanged)", cs.consecutiveFails)
	}
}

func TestHandlePollResults_SuccessResetsFailures(t *testing.T) {
	conns := map[string]*connState{
		"inst-1": {consecutiveFails: 2, backoff: 2 * time.Second},
		"inst-2": {consecutiveFails: 0},
	}
	errs := map[string]error{} // all succeeded

	handlePollResults(context.Background(), conns, errs, 3, slog.Default())

	if conns["inst-1"].consecutiveFails != 0 {
		t.Errorf("inst-1 consecutiveFails = %d, want 0", conns["inst-1"].consecutiveFails)
	}
	if conns["inst-1"].backoff != 0 {
		t.Errorf("inst-1 backoff = %v, want 0", conns["inst-1"].backoff)
	}
	if conns["inst-2"].consecutiveFails != 0 {
		t.Errorf("inst-2 consecutiveFails = %d, want 0", conns["inst-2"].consecutiveFails)
	}
}

func TestHandlePollResults_FailureIncrementsCounter(t *testing.T) {
	conns := map[string]*connState{
		"inst-1": {consecutiveFails: 0},
		"inst-2": {consecutiveFails: 0},
	}
	errs := map[string]error{
		"inst-1": errors.New("connection refused"),
	}

	handlePollResults(context.Background(), conns, errs, 3, slog.Default())

	if conns["inst-1"].consecutiveFails != 1 {
		t.Errorf("inst-1 consecutiveFails = %d, want 1", conns["inst-1"].consecutiveFails)
	}
	// inst-2 succeeded — should stay at 0.
	if conns["inst-2"].consecutiveFails != 0 {
		t.Errorf("inst-2 consecutiveFails = %d, want 0", conns["inst-2"].consecutiveFails)
	}
}

func TestHandlePollResults_ThresholdTriggersReconnect(t *testing.T) {
	reconnectCalled := false
	conns := map[string]*connState{
		"inst-1": {
			serverID:         "inst-1",
			endpoint:         "localhost",
			port:             3306,
			user:             "u",
			password:         "p",
			maxOpenConns:     3,
			consecutiveFails: 2, // one more failure hits threshold=3
			open: func(dsn string) (*sql.DB, error) {
				reconnectCalled = true
				return nil, fmt.Errorf("still down")
			},
		},
	}
	errs := map[string]error{
		"inst-1": errors.New("connection refused"),
	}

	handlePollResults(context.Background(), conns, errs, 3, slog.Default())

	if conns["inst-1"].consecutiveFails != 3 {
		t.Errorf("consecutiveFails = %d, want 3", conns["inst-1"].consecutiveFails)
	}
	if !reconnectCalled {
		t.Error("Reconnect was not called at threshold")
	}
	if conns["inst-1"].backoff != initialBackoff {
		t.Errorf("backoff = %v, want %v (reconnect failed)", conns["inst-1"].backoff, initialBackoff)
	}
}

func TestHandlePollResults_BelowThresholdNoReconnect(t *testing.T) {
	reconnectCalled := false
	conns := map[string]*connState{
		"inst-1": {
			consecutiveFails: 0, // first failure, threshold=3
			open: func(dsn string) (*sql.DB, error) {
				reconnectCalled = true
				return nil, fmt.Errorf("should not be called")
			},
		},
	}
	errs := map[string]error{
		"inst-1": errors.New("connection refused"),
	}

	handlePollResults(context.Background(), conns, errs, 3, slog.Default())

	if reconnectCalled {
		t.Error("Reconnect should not be called below threshold")
	}
	if conns["inst-1"].consecutiveFails != 1 {
		t.Errorf("consecutiveFails = %d, want 1", conns["inst-1"].consecutiveFails)
	}
}

func TestHandlePollResults_BackoffPreventsReconnect(t *testing.T) {
	reconnectCalled := false
	conns := map[string]*connState{
		"inst-1": {
			serverID:         "inst-1",
			consecutiveFails: 4,
			backoff:          30 * time.Second,
			lastAttempt:      time.Now(), // backoff not elapsed
			open: func(dsn string) (*sql.DB, error) {
				reconnectCalled = true
				return nil, fmt.Errorf("should not be called")
			},
		},
	}
	errs := map[string]error{
		"inst-1": errors.New("connection refused"),
	}

	handlePollResults(context.Background(), conns, errs, 3, slog.Default())

	if reconnectCalled {
		t.Error("Reconnect should not be called while in backoff")
	}
	// Failure still counted.
	if conns["inst-1"].consecutiveFails != 5 {
		t.Errorf("consecutiveFails = %d, want 5", conns["inst-1"].consecutiveFails)
	}
}

func TestHandlePollResults_MixedResults(t *testing.T) {
	reconnectCalled := false
	conns := map[string]*connState{
		"healthy": {consecutiveFails: 0},
		"failing": {
			serverID:         "failing",
			endpoint:         "localhost",
			port:             3306,
			user:             "u",
			password:         "p",
			maxOpenConns:     3,
			consecutiveFails: 2, // hits threshold=3
			open: func(dsn string) (*sql.DB, error) {
				reconnectCalled = true
				return nil, fmt.Errorf("still down")
			},
		},
		"recovering": {consecutiveFails: 5}, // was failing, now succeeds
	}
	errs := map[string]error{
		"failing": errors.New("connection refused"),
		// "healthy" and "recovering" not in errs → success
	}

	handlePollResults(context.Background(), conns, errs, 3, slog.Default())

	// Healthy stays healthy.
	if conns["healthy"].consecutiveFails != 0 {
		t.Errorf("healthy consecutiveFails = %d, want 0", conns["healthy"].consecutiveFails)
	}
	// Failing triggers reconnect.
	if !reconnectCalled {
		t.Error("reconnect not called for failing instance")
	}
	// Recovering resets.
	if conns["recovering"].consecutiveFails != 0 {
		t.Errorf("recovering consecutiveFails = %d, want 0", conns["recovering"].consecutiveFails)
	}
}

func TestConnState_ReadyToReconnect(t *testing.T) {
	cs := &connState{}
	if !cs.ReadyToReconnect() {
		t.Error("should be ready with zero backoff")
	}

	cs.backoff = 5 * time.Second
	cs.lastAttempt = time.Now()
	if cs.ReadyToReconnect() {
		t.Error("should not be ready, backoff not elapsed")
	}

	cs.lastAttempt = time.Now().Add(-6 * time.Second)
	if !cs.ReadyToReconnect() {
		t.Error("should be ready, backoff elapsed")
	}
}
