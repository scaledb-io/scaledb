//go:build integration

package analyze

import (
	"context"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func TestCollectSummary_LocalMySQL(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	summary, err := CollectSummary(ctx, db)
	if err != nil {
		t.Fatalf("CollectSummary returned error: %v", err)
	}

	if summary.Version == "" {
		t.Error("Version is empty")
	}
	if !strings.HasPrefix(summary.Version, "8.0") {
		t.Errorf("expected Version to start with '8.0', got %q", summary.Version)
	}

	if summary.Uptime <= 0 {
		t.Errorf("expected Uptime > 0, got %d", summary.Uptime)
	}

	if summary.BufferPool.SizeBytes <= 0 {
		t.Errorf("expected BufferPool.SizeBytes > 0, got %d", summary.BufferPool.SizeBytes)
	}

	if summary.BufferPool.HitRatio <= 0 || summary.BufferPool.HitRatio > 100 {
		t.Errorf("expected BufferPool.HitRatio in (0, 100], got %f", summary.BufferPool.HitRatio)
	}

	if summary.Connections.MaxConnections <= 0 {
		t.Errorf("expected Connections.MaxConnections > 0, got %d", summary.Connections.MaxConnections)
	}

	if len(summary.SchemaStats) == 0 {
		t.Fatal("expected non-empty SchemaStats")
	}

	foundScout := false
	for _, ss := range summary.SchemaStats {
		if ss.Schema == "scout" {
			foundScout = true
			break
		}
	}
	if !foundScout {
		t.Error("expected 'scout' schema in SchemaStats but did not find it")
	}
}

func TestCollectSummary_SchemaStats(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	summary, err := CollectSummary(ctx, db)
	if err != nil {
		t.Fatalf("CollectSummary returned error: %v", err)
	}

	for _, ss := range summary.SchemaStats {
		if ss.Schema == "scout" {
			if ss.Tables <= 0 {
				t.Errorf("expected scout schema to have tables > 0, got %d", ss.Tables)
			}
			if ss.SizeBytes < 0 {
				t.Errorf("expected scout schema SizeBytes >= 0, got %d", ss.SizeBytes)
			}
			return
		}
	}

	t.Fatal("scout schema not found in SchemaStats")
}
