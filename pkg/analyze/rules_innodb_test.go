package analyze

import "testing"

func TestInnoDBRules(t *testing.T) {
	tests := []struct {
		name         string
		ruleID       string
		vars         map[string]string
		wantCount    int
		wantSeverity string
	}{
		// --- innodb_flush_log_at_trx_commit ---
		{
			name:         "flush_log_at_trx_commit value 2 fires warn",
			ruleID:       "innodb_flush_log_at_trx_commit",
			vars:         map[string]string{"innodb_flush_log_at_trx_commit": "2"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:         "flush_log_at_trx_commit value 0 fires warn with extra note",
			ruleID:       "innodb_flush_log_at_trx_commit",
			vars:         map[string]string{"innodb_flush_log_at_trx_commit": "0"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "flush_log_at_trx_commit value 1 silent",
			ruleID: "innodb_flush_log_at_trx_commit",
			vars:   map[string]string{"innodb_flush_log_at_trx_commit": "1"},
		},

		// --- innodb_doublewrite ---
		{
			name:         "doublewrite OFF fires warn",
			ruleID:       "innodb_doublewrite",
			vars:         map[string]string{"innodb_doublewrite": "OFF"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "doublewrite ON silent",
			ruleID: "innodb_doublewrite",
			vars:   map[string]string{"innodb_doublewrite": "ON"},
		},

		// --- innodb_flush_neighbors ---
		{
			name:         "flush_neighbors 1 fires note",
			ruleID:       "innodb_flush_neighbors",
			vars:         map[string]string{"innodb_flush_neighbors": "1"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "flush_neighbors 0 silent",
			ruleID: "innodb_flush_neighbors",
			vars:   map[string]string{"innodb_flush_neighbors": "0"},
		},

		// --- innodb_io_capacity ---
		{
			name:         "io_capacity at default 200 fires note",
			ruleID:       "innodb_io_capacity",
			vars:         map[string]string{"innodb_io_capacity": "200"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "io_capacity tuned silent",
			ruleID: "innodb_io_capacity",
			vars:   map[string]string{"innodb_io_capacity": "2000"},
		},

		// --- innodb_io_capacity_max ---
		{
			name:         "io_capacity_max at default 2000 fires note",
			ruleID:       "innodb_io_capacity_max",
			vars:         map[string]string{"innodb_io_capacity_max": "2000"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "io_capacity_max tuned silent",
			ruleID: "innodb_io_capacity_max",
			vars:   map[string]string{"innodb_io_capacity_max": "10000"},
		},

		// --- innodb_log_buffer_size ---
		{
			name:         "log_buffer_size over 16MB fires warn",
			ruleID:       "innodb_log_buffer_size",
			vars:         map[string]string{"innodb_log_buffer_size": "33554432"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "log_buffer_size at 16MB silent",
			ruleID: "innodb_log_buffer_size",
			vars:   map[string]string{"innodb_log_buffer_size": "16777216"},
		},

		// --- innodb_force_recovery ---
		{
			name:         "force_recovery nonzero fires warn",
			ruleID:       "innodb_force_recovery",
			vars:         map[string]string{"innodb_force_recovery": "1"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "force_recovery zero silent",
			ruleID: "innodb_force_recovery",
			vars:   map[string]string{"innodb_force_recovery": "0"},
		},

		// --- innodb_fast_shutdown ---
		{
			name:         "fast_shutdown value 0 fires warn",
			ruleID:       "innodb_fast_shutdown",
			vars:         map[string]string{"innodb_fast_shutdown": "0"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:         "fast_shutdown value 2 fires warn",
			ruleID:       "innodb_fast_shutdown",
			vars:         map[string]string{"innodb_fast_shutdown": "2"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "fast_shutdown value 1 silent",
			ruleID: "innodb_fast_shutdown",
			vars:   map[string]string{"innodb_fast_shutdown": "1"},
		},

		// --- innodb_lock_wait_timeout ---
		{
			name:         "lock_wait_timeout over 120 fires warn",
			ruleID:       "innodb_lock_wait_timeout",
			vars:         map[string]string{"innodb_lock_wait_timeout": "300"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "lock_wait_timeout at 50 silent",
			ruleID: "innodb_lock_wait_timeout",
			vars:   map[string]string{"innodb_lock_wait_timeout": "50"},
		},

		// --- innodb_dedicated_server ---
		{
			name:         "dedicated_server ON fires note",
			ruleID:       "innodb_dedicated_server",
			vars:         map[string]string{"innodb_dedicated_server": "ON"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "dedicated_server OFF silent",
			ruleID: "innodb_dedicated_server",
			vars:   map[string]string{"innodb_dedicated_server": "OFF"},
		},

		// --- innodb_print_all_deadlocks ---
		{
			name:         "print_all_deadlocks OFF fires note",
			ruleID:       "innodb_print_all_deadlocks",
			vars:         map[string]string{"innodb_print_all_deadlocks": "OFF"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "print_all_deadlocks ON silent",
			ruleID: "innodb_print_all_deadlocks",
			vars:   map[string]string{"innodb_print_all_deadlocks": "ON"},
		},

		// --- innodb_stats_persistent ---
		{
			name:         "stats_persistent OFF fires warn",
			ruleID:       "innodb_stats_persistent",
			vars:         map[string]string{"innodb_stats_persistent": "OFF"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "stats_persistent ON silent",
			ruleID: "innodb_stats_persistent",
			vars:   map[string]string{"innodb_stats_persistent": "ON"},
		},

		// --- innodb_checksum_algorithm ---
		{
			name:         "checksum_algorithm innodb fires note",
			ruleID:       "innodb_checksum_algorithm",
			vars:         map[string]string{"innodb_checksum_algorithm": "innodb"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "checksum_algorithm crc32 silent",
			ruleID: "innodb_checksum_algorithm",
			vars:   map[string]string{"innodb_checksum_algorithm": "crc32"},
		},

		// --- innodb_redo_log_capacity ---
		{
			name:         "redo_log_capacity at default on 8.0.35 fires warn",
			ruleID:       "innodb_redo_log_capacity",
			vars:         map[string]string{"version": "8.0.35", "innodb_redo_log_capacity": "104857600"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "redo_log_capacity at default on 8.0.28 silent (version too old)",
			ruleID: "innodb_redo_log_capacity",
			vars:   map[string]string{"version": "8.0.28", "innodb_redo_log_capacity": "104857600"},
		},
		{
			name:   "redo_log_capacity tuned on 8.0.35 silent",
			ruleID: "innodb_redo_log_capacity",
			vars:   map[string]string{"version": "8.0.35", "innodb_redo_log_capacity": "536870912"},
		},

		// --- innodb_buffer_pool_instances ---
		{
			name:         "buffer_pool_instances 1 with large pool fires note",
			ruleID:       "innodb_buffer_pool_instances",
			vars:         map[string]string{"innodb_buffer_pool_size": "8589934592", "innodb_buffer_pool_instances": "1"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "buffer_pool_instances 8 with large pool silent",
			ruleID: "innodb_buffer_pool_instances",
			vars:   map[string]string{"innodb_buffer_pool_size": "8589934592", "innodb_buffer_pool_instances": "8"},
		},
		{
			name:   "buffer_pool_instances 1 with small pool silent",
			ruleID: "innodb_buffer_pool_instances",
			vars:   map[string]string{"innodb_buffer_pool_size": "134217728", "innodb_buffer_pool_instances": "1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := runSingleRule(t, tc.ruleID, tc.vars)
			assertFindings(t, findings, tc.wantCount, tc.wantSeverity)
		})
	}
}

func TestInnoDBFlushLogValue0ExtraMessage(t *testing.T) {
	findings := runSingleRule(t, "innodb_flush_log_at_trx_commit", map[string]string{
		"innodb_flush_log_at_trx_commit": "0",
	})
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if got := findings[0].Description; got == "" {
		t.Fatal("expected non-empty description")
	}
	// Verify the extra note about value 0 is present.
	want := "no benefit over value 2"
	if !contains(findings[0].Description, want) {
		t.Errorf("description missing %q: %s", want, findings[0].Description)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
