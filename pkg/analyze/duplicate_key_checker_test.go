//go:build integration

package analyze

import (
	"context"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func TestCheckDuplicateKeys_NoDuplicates(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Clean up leftover test tables from previous runs.
	db.ExecContext(ctx, "DROP TABLE IF EXISTS scout._scaledb_test_dups")
	db.ExecContext(ctx, "DROP TABLE IF EXISTS scout._scaledb_test_pk")

	findings, err := CheckDuplicateKeys(ctx, db)
	if err != nil {
		t.Fatalf("CheckDuplicateKeys returned error: %v", err)
	}

	// Filter to only scout database test tables (ignore application tables).
	var testFindings []DuplicateKeyFinding
	for _, f := range findings {
		if f.Database == "scout" && strings.HasPrefix(f.Table, "_scaledb_test") {
			testFindings = append(testFindings, f)
		}
	}

	if len(testFindings) != 0 {
		t.Errorf("expected 0 duplicate key findings for test tables, got %d", len(testFindings))
		for _, f := range testFindings {
			t.Logf("  %s.%s: %s overlaps %s", f.Database, f.Table, f.DuplicateIndex, f.OverlapsWith)
		}
	}
}

func TestCheckDuplicateKeys_WithDuplicates(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a test table with an intentional left-prefix duplicate index.
	_, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS scout")
	if err != nil {
		t.Fatalf("creating scout database: %v", err)
	}

	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS scout._scaledb_test_dups")
	if err != nil {
		t.Fatalf("dropping test table: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		CREATE TABLE scout._scaledb_test_dups (
			id INT AUTO_INCREMENT PRIMARY KEY,
			email VARCHAR(255),
			name VARCHAR(255),
			INDEX idx_email (email),
			INDEX idx_email_name (email, name)
		) ENGINE=InnoDB
	`)
	if err != nil {
		t.Fatalf("creating test table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS scout._scaledb_test_dups")
	})

	findings, err := CheckDuplicateKeys(ctx, db)
	if err != nil {
		t.Fatalf("CheckDuplicateKeys returned error: %v", err)
	}

	// Filter to our test table.
	var testFindings []DuplicateKeyFinding
	for _, f := range findings {
		if f.Database == "scout" && f.Table == "_scaledb_test_dups" {
			testFindings = append(testFindings, f)
		}
	}

	if len(testFindings) == 0 {
		t.Fatal("expected at least 1 duplicate key finding for _scaledb_test_dups, got 0")
	}

	f := testFindings[0]
	if f.Database != "scout" {
		t.Errorf("expected database 'scout', got %q", f.Database)
	}
	if f.Table != "_scaledb_test_dups" {
		t.Errorf("expected table '_scaledb_test_dups', got %q", f.Table)
	}
	if f.DuplicateIndex != "idx_email" {
		t.Errorf("expected duplicate_index 'idx_email', got %q", f.DuplicateIndex)
	}
	if f.OverlapsWith != "idx_email_name" {
		t.Errorf("expected overlaps_with 'idx_email_name', got %q", f.OverlapsWith)
	}
	if !strings.Contains(f.DropStatement, "DROP INDEX") {
		t.Errorf("expected DropStatement to contain 'DROP INDEX', got %q", f.DropStatement)
	}
}

func TestCheckDuplicateKeys_PrimaryKeyNotReported(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a table where PRIMARY KEY (id) is a prefix of a secondary index (id, email).
	// The PRIMARY key should never appear as the duplicate_index.
	_, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS scout")
	if err != nil {
		t.Fatalf("creating scout database: %v", err)
	}

	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS scout._scaledb_test_pk_prefix")
	if err != nil {
		t.Fatalf("dropping test table: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		CREATE TABLE scout._scaledb_test_pk_prefix (
			id INT AUTO_INCREMENT PRIMARY KEY,
			email VARCHAR(255),
			INDEX idx_id_email (id, email)
		) ENGINE=InnoDB
	`)
	if err != nil {
		t.Fatalf("creating test table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS scout._scaledb_test_pk_prefix")
	})

	findings, err := CheckDuplicateKeys(ctx, db)
	if err != nil {
		t.Fatalf("CheckDuplicateKeys returned error: %v", err)
	}

	for _, f := range findings {
		if f.Database == "scout" && f.Table == "_scaledb_test_pk_prefix" {
			if strings.EqualFold(f.DuplicateIndex, "PRIMARY") {
				t.Errorf("PRIMARY key should never be reported as a duplicate, but got finding: %+v", f)
			}
		}
	}
}
