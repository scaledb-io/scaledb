//go:build integration

package analyze

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func TestRunAll_LocalMySQL(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	envelopes, err := RunAll(ctx, db, "test-instance-001", "test-cluster-001")
	if err != nil {
		t.Fatalf("RunAll returned error: %v", err)
	}

	if len(envelopes) != 3 {
		t.Fatalf("expected 3 envelopes, got %d", len(envelopes))
	}

	expectedTools := map[string]bool{
		"pt-variable-advisor":      false,
		"pt-duplicate-key-checker": false,
		"pt-mysql-summary":         false,
	}

	tsRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)

	for _, env := range envelopes {
		if env.RunID == "" {
			t.Error("envelope has empty RunID")
		}
		if env.InstanceID != "test-instance-001" {
			t.Errorf("expected InstanceID 'test-instance-001', got %q", env.InstanceID)
		}
		if env.ClusterID != "test-cluster-001" {
			t.Errorf("expected ClusterID 'test-cluster-001', got %q", env.ClusterID)
		}
		if env.Tool == "" {
			t.Error("envelope has empty Tool")
		}
		if env.CollectedAt == "" {
			t.Error("envelope has empty CollectedAt")
		}
		if !tsRegex.MatchString(env.CollectedAt) {
			t.Errorf("CollectedAt %q does not match YYYY-MM-DD HH:MM:SS format", env.CollectedAt)
		}

		// Verify JSONPayload is valid JSON.
		if !json.Valid([]byte(env.JSONPayload)) {
			t.Errorf("envelope for tool %q has invalid JSON payload", env.Tool)
		}

		// Track that each expected tool appears exactly once.
		if _, ok := expectedTools[env.Tool]; !ok {
			t.Errorf("unexpected tool %q in envelopes", env.Tool)
		} else {
			expectedTools[env.Tool] = true
		}

		// TopicForTool should return non-empty for all known tools.
		topic := TopicForTool(env.Tool)
		if topic == "" {
			t.Errorf("TopicForTool(%q) returned empty string", env.Tool)
		}
	}

	for tool, seen := range expectedTools {
		if !seen {
			t.Errorf("expected tool %q in envelopes but it was missing", tool)
		}
	}
}
