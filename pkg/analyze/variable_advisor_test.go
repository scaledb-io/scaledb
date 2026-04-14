//go:build integration

package analyze

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := "scout:scoutpassword@tcp(localhost:3306)/?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Skipf("local MySQL not available: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("local MySQL not reachable: %v", err)
	}
	return db
}

func TestCheckVariables_LocalMySQL(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := CheckVariables(ctx, db, CheckVariablesOptions{})
	if err != nil {
		t.Fatalf("CheckVariables returned error: %v", err)
	}

	if len(result.Findings) == 0 {
		t.Fatal("expected non-empty findings list, got 0")
	}

	if result.TotalRules < 70 {
		t.Errorf("expected at least 70 rules in registry, got %d", result.TotalRules)
	}

	if len(result.ByCategory) == 0 {
		t.Error("expected ByCategory to be populated")
	}

	validSeverities := map[string]bool{"warn": true, "note": true, "critical": true}
	seenRuleIDs := make(map[string]bool)

	for _, f := range result.Findings {
		if f.RuleID == "" {
			t.Error("finding has empty RuleID")
		}
		if f.Category == "" {
			t.Errorf("finding %q has empty Category", f.RuleID)
		}
		if f.Severity == "" {
			t.Error("finding has empty Severity")
		}
		if !validSeverities[f.Severity] {
			t.Errorf("finding %q has invalid severity %q", f.RuleID, f.Severity)
		}
		if f.Variable == "" {
			t.Errorf("finding %q has empty Variable", f.RuleID)
		}
		if f.Description == "" {
			t.Errorf("finding %q has empty Description", f.RuleID)
		}
		if seenRuleIDs[f.RuleID] {
			t.Errorf("duplicate rule_id %q", f.RuleID)
		}
		seenRuleIDs[f.RuleID] = true
	}
}

func TestCheckVariables_CategoryFilter(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := CheckVariables(ctx, db, CheckVariablesOptions{Category: "innodb"})
	if err != nil {
		t.Fatalf("CheckVariables returned error: %v", err)
	}

	for _, f := range result.Findings {
		if f.Category != CategoryInnoDB {
			t.Errorf("finding %q has category %q, expected innodb", f.RuleID, f.Category)
		}
	}

	// Should only have InnoDB category in ByCategory.
	for cat := range result.ByCategory {
		if cat != CategoryInnoDB {
			t.Errorf("ByCategory contains unexpected category %q", cat)
		}
	}
}

func TestCheckVariables_ExpectedRules(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := CheckVariables(ctx, db, CheckVariablesOptions{})
	if err != nil {
		t.Fatalf("CheckVariables returned error: %v", err)
	}

	// Build a map of rule_id -> finding for easy lookup.
	byRule := make(map[string]VariableAdvisorFinding, len(result.Findings))
	for _, f := range result.Findings {
		byRule[f.RuleID] = f
	}

	expected := []struct {
		ruleID   string
		severity string
	}{
		{"delay_key_write", "warn"},
		{"innodb_buffer_pool_size", "warn"},
		{"innodb_log_file_size", "warn"},
		{"key_buffer_size", "warn"},
		{"myisam_recover_options", "warn"},
	}

	for _, tc := range expected {
		t.Run(tc.ruleID, func(t *testing.T) {
			f, ok := byRule[tc.ruleID]
			if !ok {
				t.Fatalf("rule %q did not fire", tc.ruleID)
			}
			if f.Severity != tc.severity {
				t.Errorf("rule %q: expected severity %q, got %q", tc.ruleID, tc.severity, f.Severity)
			}
		})
	}
}

