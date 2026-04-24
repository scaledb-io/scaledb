package collect

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// rawWaitEvent holds wait event data before correlation with parent statements.
type rawWaitEvent struct {
	EventID        uint64
	ThreadID       uint64
	NestingEventID uint64
	EventName      string
	TimerWait      uint64
	TimerStart     uint64
	Operation      string
	Source         string
}

// bufferBounds holds the ring buffer's time span and row count,
// used by the adaptive interval algorithm.
type bufferBounds struct {
	OldestTimer uint64 // picoseconds since MySQL start
	NewestTimer uint64
	RowCount    uint64
}

// SamplerState tracks per-instance state for the adaptive sampler.
type SamplerState struct {
	LastStatementEventID uint64
	LastWaitEventID      uint64
	Interval             time.Duration
	WaitAccum            map[waitKey]*waitAccum
	LastFlush            time.Time
}

// waitKey is the grouping key for wait event aggregation.
type waitKey struct {
	ParentDigest string
	EventName    string
	Bucket       string
}

// waitAccum holds running totals for a single (digest, event, bucket).
type waitAccum struct {
	Count     uint64
	TotalWait uint64
}

// NewSamplerState creates a SamplerState with the given initial interval.
func NewSamplerState(interval time.Duration) *SamplerState {
	return &SamplerState{
		Interval:  interval,
		WaitAccum: make(map[waitKey]*waitAccum),
		LastFlush: time.Now(),
	}
}

// queryStatementSamples fetches new statement events from
// events_statements_history_long since the given high-water mark EVENT_ID.
func queryStatementSamples(ctx context.Context, db *sql.DB, lastEventID uint64) ([]QuerySample, uint64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			EVENT_ID,
			THREAD_ID,
			IFNULL(DIGEST, ''),
			IFNULL(SQL_TEXT, ''),
			IFNULL(TIMER_WAIT, 0),
			IFNULL(ROWS_EXAMINED, 0),
			IFNULL(ROWS_SENT, 0),
			IFNULL(ROWS_AFFECTED, 0),
			IFNULL(CREATED_TMP_TABLES, 0),
			IFNULL(CREATED_TMP_DISK_TABLES, 0),
			IFNULL(NO_INDEX_USED, 0),
			IFNULL(CURRENT_SCHEMA, '')
		FROM performance_schema.events_statements_history_long
		WHERE EVENT_ID > ?
		ORDER BY EVENT_ID
	`, lastEventID)
	if err != nil {
		return nil, lastEventID, fmt.Errorf("querying statement samples: %w", err)
	}
	defer rows.Close()

	var (
		samples    []QuerySample
		maxEventID = lastEventID
		threadID   uint64
	)
	for rows.Next() {
		var s QuerySample
		if err := rows.Scan(
			&s.EventID, &threadID, &s.Digest, &s.SQLText,
			&s.TimerWait, &s.RowsExamined, &s.RowsSent, &s.RowsAffected,
			&s.CreatedTmpTables, &s.CreatedTmpDiskTables, &s.NoIndexUsed,
			&s.CurrentSchema,
		); err != nil {
			return nil, lastEventID, fmt.Errorf("scanning statement sample: %w", err)
		}
		samples = append(samples, s)
		if s.EventID > maxEventID {
			maxEventID = s.EventID
		}
	}
	if err := rows.Err(); err != nil {
		return nil, lastEventID, fmt.Errorf("iterating statement samples: %w", err)
	}

	return samples, maxEventID, nil
}

// queryWaitEvents fetches new wait events from events_waits_history_long
// since the given high-water mark EVENT_ID.
func queryWaitEvents(ctx context.Context, db *sql.DB, lastEventID uint64) ([]rawWaitEvent, uint64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			EVENT_ID,
			THREAD_ID,
			IFNULL(NESTING_EVENT_ID, 0),
			EVENT_NAME,
			IFNULL(TIMER_WAIT, 0),
			IFNULL(TIMER_START, 0),
			IFNULL(OPERATION, ''),
			IFNULL(SOURCE, '')
		FROM performance_schema.events_waits_history_long
		WHERE EVENT_ID > ?
		ORDER BY EVENT_ID
	`, lastEventID)
	if err != nil {
		return nil, lastEventID, fmt.Errorf("querying wait events: %w", err)
	}
	defer rows.Close()

	var (
		events     []rawWaitEvent
		maxEventID = lastEventID
	)
	for rows.Next() {
		var w rawWaitEvent
		if err := rows.Scan(
			&w.EventID, &w.ThreadID, &w.NestingEventID,
			&w.EventName, &w.TimerWait, &w.TimerStart,
			&w.Operation, &w.Source,
		); err != nil {
			return nil, lastEventID, fmt.Errorf("scanning wait event: %w", err)
		}
		events = append(events, w)
		if w.EventID > maxEventID {
			maxEventID = w.EventID
		}
	}
	if err := rows.Err(); err != nil {
		return nil, lastEventID, fmt.Errorf("iterating wait events: %w", err)
	}

	return events, maxEventID, nil
}

// queryBufferBounds measures the ring buffer's time span by reading
// the min/max TIMER_START and row count from events_statements_history_long.
func queryBufferBounds(ctx context.Context, db *sql.DB) (bufferBounds, error) {
	var b bufferBounds
	err := db.QueryRowContext(ctx, `
		SELECT
			IFNULL(MIN(TIMER_START), 0),
			IFNULL(MAX(TIMER_START), 0),
			COUNT(*)
		FROM performance_schema.events_statements_history_long
	`).Scan(&b.OldestTimer, &b.NewestTimer, &b.RowCount)
	if err != nil {
		return b, fmt.Errorf("querying buffer bounds: %w", err)
	}
	return b, nil
}

// bufferLifespan returns the time span of the ring buffer in seconds.
// TIMER_START values are in picoseconds (1e12 per second).
func bufferLifespan(b bufferBounds) time.Duration {
	if b.OldestTimer == 0 || b.NewestTimer == 0 || b.NewestTimer <= b.OldestTimer {
		return 0
	}
	picos := b.NewestTimer - b.OldestTimer
	// TIMER_START is in picoseconds. 1 nanosecond = 1000 picoseconds.
	nanos := picos / 1000
	return time.Duration(nanos) * time.Nanosecond
}

// adjustInterval computes a new sampling interval based on the buffer lifespan
// and current interval. If the buffer lifespan is shorter than the interval,
// we're missing events and need to poll faster. If longer, we have headroom
// and can poll slower to reduce load.
//
// Bounds: min 1s, max 60s.
func adjustInterval(current time.Duration, lifespan time.Duration) time.Duration {
	if lifespan == 0 {
		return current
	}

	ratio := float64(lifespan) / float64(current)

	var next time.Duration
	switch {
	case ratio < 1.5:
		// Buffer lifespan is tight — poll faster.
		next = time.Duration(float64(current) * 0.5)
	case ratio > 4.0:
		// Lots of headroom — poll slower to reduce load.
		next = time.Duration(float64(current) * 1.5)
	default:
		return current
	}

	// Clamp to bounds.
	if next < 1*time.Second {
		next = 1 * time.Second
	}
	if next > 60*time.Second {
		next = 60 * time.Second
	}
	return next
}
