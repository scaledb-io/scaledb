package collect

import (
	"testing"
	"time"
)

func TestBucketTime(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"2026-04-24T10:00:00Z", "2026-04-24 10:00:00"},
		{"2026-04-24T10:00:03Z", "2026-04-24 10:00:00"},
		{"2026-04-24T10:00:04Z", "2026-04-24 10:00:00"},
		{"2026-04-24T10:00:05Z", "2026-04-24 10:00:05"},
		{"2026-04-24T10:00:07Z", "2026-04-24 10:00:05"},
		{"2026-04-24T10:00:59Z", "2026-04-24 10:00:55"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			ts, _ := time.Parse(time.RFC3339, tt.input)
			got := BucketTime(ts).Format("2006-01-02 15:04:05")
			if got != tt.want {
				t.Errorf("BucketTime(%s) = %s, want %s", tt.input, got, tt.want)
			}
		})
	}
}

func TestCorrelateWaits(t *testing.T) {
	samples := []QuerySample{
		{EventID: 100, Digest: "digest_A"},
		{EventID: 200, Digest: "digest_B"},
	}

	waits := []RawWaitEvent{
		{EventID: 1001, NestingEventID: 100, EventName: "wait/io/table", TimerWait: 500},
		{EventID: 1002, NestingEventID: 200, EventName: "wait/io/table", TimerWait: 300},
		{EventID: 1003, NestingEventID: 999, EventName: "wait/io/table", TimerWait: 100}, // orphan
	}

	result := CorrelateWaits(samples, waits)

	if len(result) != 3 {
		t.Fatalf("got %d correlated waits, want 3", len(result))
	}
	if result[0].ParentDigest != "digest_A" {
		t.Errorf("result[0].ParentDigest = %q, want digest_A", result[0].ParentDigest)
	}
	if result[1].ParentDigest != "digest_B" {
		t.Errorf("result[1].ParentDigest = %q, want digest_B", result[1].ParentDigest)
	}
	if result[2].ParentDigest != "" {
		t.Errorf("result[2].ParentDigest = %q, want empty (orphan)", result[2].ParentDigest)
	}
}

func TestAggregateWaits(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2026-04-24T10:00:03Z")

	waits := []CorrelatedWait{
		{ParentDigest: "d1", EventName: "wait/io/table", TimerWait: 100},
		{ParentDigest: "d1", EventName: "wait/io/table", TimerWait: 200},
		{ParentDigest: "d1", EventName: "wait/lock/row", TimerWait: 50},
		{ParentDigest: "d2", EventName: "wait/io/table", TimerWait: 300},
	}

	result := AggregateWaits("inst-1", "cluster-1", waits, now)

	// 3 unique (digest, event_name) combinations
	if len(result) != 3 {
		t.Fatalf("got %d summaries, want 3", len(result))
	}

	// Find the d1 + wait/io/table entry
	var found bool
	for _, s := range result {
		if s.ParentDigest == "d1" && s.EventName == "wait/io/table" {
			found = true
			if s.Count != 2 {
				t.Errorf("d1 io/table count = %d, want 2", s.Count)
			}
			if s.TotalWait != 300 {
				t.Errorf("d1 io/table total_wait = %d, want 300", s.TotalWait)
			}
			if s.Timestamp != "2026-04-24 10:00:00" {
				t.Errorf("timestamp = %q, want bucketed to :00", s.Timestamp)
			}
		}
	}
	if !found {
		t.Error("missing d1 + wait/io/table summary")
	}
}

func TestAdjustInterval(t *testing.T) {
	tests := []struct {
		name     string
		current  time.Duration
		lifespan time.Duration
		want     time.Duration
	}{
		{"tight buffer - speed up", 10 * time.Second, 10 * time.Second, 5 * time.Second},
		{"lots of headroom - slow down", 10 * time.Second, 50 * time.Second, 15 * time.Second},
		{"comfortable - no change", 10 * time.Second, 25 * time.Second, 10 * time.Second},
		{"zero lifespan - no change", 10 * time.Second, 0, 10 * time.Second},
		{"floor at 1s", 2 * time.Second, 2 * time.Second, 1 * time.Second},
		{"cap at 60s", 50 * time.Second, 300 * time.Second, 60 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AdjustInterval(tt.current, tt.lifespan)
			if got != tt.want {
				t.Errorf("AdjustInterval(%s, %s) = %s, want %s", tt.current, tt.lifespan, got, tt.want)
			}
		})
	}
}
