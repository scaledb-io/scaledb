package analyze

import "testing"

func TestReplicationRules(t *testing.T) {
	tests := []struct {
		name         string
		ruleID       string
		vars         map[string]string
		wantCount    int
		wantSeverity string
	}{
		// --- log_bin ---
		{
			name:         "log_bin OFF fires",
			ruleID:       "log_bin",
			vars:         map[string]string{"log_bin": "OFF"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "log_bin ON silent",
			ruleID: "log_bin",
			vars:   map[string]string{"log_bin": "ON"},
		},

		// --- binlog_format ---
		{
			name:         "binlog_format STATEMENT fires",
			ruleID:       "binlog_format",
			vars:         map[string]string{"binlog_format": "STATEMENT"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:         "binlog_format MIXED fires",
			ruleID:       "binlog_format",
			vars:         map[string]string{"binlog_format": "MIXED"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "binlog_format ROW silent",
			ruleID: "binlog_format",
			vars:   map[string]string{"binlog_format": "ROW"},
		},

		// --- sync_binlog ---
		{
			name:         "sync_binlog 0 with log_bin ON fires",
			ruleID:       "sync_binlog",
			vars:         map[string]string{"sync_binlog": "0", "log_bin": "ON"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "sync_binlog 1 with log_bin ON silent",
			ruleID: "sync_binlog",
			vars:   map[string]string{"sync_binlog": "1", "log_bin": "ON"},
		},
		{
			name:   "sync_binlog 0 with log_bin OFF silent",
			ruleID: "sync_binlog",
			vars:   map[string]string{"sync_binlog": "0", "log_bin": "OFF"},
		},

		// --- gtid_mode ---
		{
			name:         "gtid_mode OFF fires",
			ruleID:       "gtid_mode",
			vars:         map[string]string{"gtid_mode": "OFF"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "gtid_mode ON silent",
			ruleID: "gtid_mode",
			vars:   map[string]string{"gtid_mode": "ON"},
		},

		// --- enforce_gtid_consistency ---
		{
			name:         "enforce_gtid_consistency OFF with gtid ON fires",
			ruleID:       "enforce_gtid_consistency",
			vars:         map[string]string{"enforce_gtid_consistency": "OFF", "gtid_mode": "ON"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "enforce_gtid_consistency ON with gtid ON silent",
			ruleID: "enforce_gtid_consistency",
			vars:   map[string]string{"enforce_gtid_consistency": "ON", "gtid_mode": "ON"},
		},
		{
			name:   "enforce_gtid_consistency OFF with gtid OFF silent",
			ruleID: "enforce_gtid_consistency",
			vars:   map[string]string{"enforce_gtid_consistency": "OFF", "gtid_mode": "OFF"},
		},

		// --- binlog_row_image ---
		{
			name:         "binlog_row_image FULL with ROW format fires",
			ruleID:       "binlog_row_image",
			vars:         map[string]string{"binlog_row_image": "FULL", "binlog_format": "ROW"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "binlog_row_image MINIMAL with ROW format silent",
			ruleID: "binlog_row_image",
			vars:   map[string]string{"binlog_row_image": "MINIMAL", "binlog_format": "ROW"},
		},
		{
			name:   "binlog_row_image FULL with STATEMENT format silent",
			ruleID: "binlog_row_image",
			vars:   map[string]string{"binlog_row_image": "FULL", "binlog_format": "STATEMENT"},
		},

		// --- replica_parallel_workers ---
		{
			name:         "replica_parallel_workers 0 fires",
			ruleID:       "replica_parallel_workers",
			vars:         map[string]string{"replica_parallel_workers": "0"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:         "replica_parallel_workers 1 fires",
			ruleID:       "replica_parallel_workers",
			vars:         map[string]string{"replica_parallel_workers": "1"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "replica_parallel_workers 4 silent",
			ruleID: "replica_parallel_workers",
			vars:   map[string]string{"replica_parallel_workers": "4"},
		},
		{
			name:         "slave_parallel_workers legacy fallback fires",
			ruleID:       "replica_parallel_workers",
			vars:         map[string]string{"slave_parallel_workers": "0"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "replica_parallel_workers preferred over legacy",
			ruleID: "replica_parallel_workers",
			vars:   map[string]string{"replica_parallel_workers": "4", "slave_parallel_workers": "0"},
		},

		// --- replica_preserve_commit_order ---
		{
			name:         "preserve_commit_order OFF with workers > 1 fires",
			ruleID:       "replica_preserve_commit_order",
			vars:         map[string]string{"replica_preserve_commit_order": "OFF", "replica_parallel_workers": "4"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "preserve_commit_order ON with workers > 1 silent",
			ruleID: "replica_preserve_commit_order",
			vars:   map[string]string{"replica_preserve_commit_order": "ON", "replica_parallel_workers": "4"},
		},
		{
			name:   "preserve_commit_order OFF with workers 1 silent",
			ruleID: "replica_preserve_commit_order",
			vars:   map[string]string{"replica_preserve_commit_order": "OFF", "replica_parallel_workers": "1"},
		},
		{
			name:         "slave_preserve_commit_order legacy fallback fires",
			ruleID:       "replica_preserve_commit_order",
			vars:         map[string]string{"slave_preserve_commit_order": "OFF", "slave_parallel_workers": "4"},
			wantCount:    1,
			wantSeverity: "warn",
		},

		// --- binlog_transaction_dependency_tracking ---
		{
			name:         "dependency_tracking COMMIT_ORDER fires",
			ruleID:       "binlog_transaction_dependency_tracking",
			vars:         map[string]string{"binlog_transaction_dependency_tracking": "COMMIT_ORDER"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "dependency_tracking WRITESET silent",
			ruleID: "binlog_transaction_dependency_tracking",
			vars:   map[string]string{"binlog_transaction_dependency_tracking": "WRITESET"},
		},

		// --- binlog_expire_logs_seconds ---
		{
			name:         "binlog_expire_logs_seconds 0 with log_bin ON fires",
			ruleID:       "binlog_expire_logs_seconds",
			vars:         map[string]string{"binlog_expire_logs_seconds": "0", "log_bin": "ON"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "binlog_expire_logs_seconds nonzero silent",
			ruleID: "binlog_expire_logs_seconds",
			vars:   map[string]string{"binlog_expire_logs_seconds": "604800", "log_bin": "ON"},
		},
		{
			name:   "binlog_expire_logs_seconds 0 with log_bin OFF silent",
			ruleID: "binlog_expire_logs_seconds",
			vars:   map[string]string{"binlog_expire_logs_seconds": "0", "log_bin": "OFF"},
		},

		// --- log_replica_updates ---
		{
			name:         "log_replica_updates OFF fires",
			ruleID:       "log_replica_updates",
			vars:         map[string]string{"log_replica_updates": "OFF"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "log_replica_updates ON silent",
			ruleID: "log_replica_updates",
			vars:   map[string]string{"log_replica_updates": "ON"},
		},
		{
			name:         "log_slave_updates legacy fallback fires",
			ruleID:       "log_replica_updates",
			vars:         map[string]string{"log_slave_updates": "OFF"},
			wantCount:    1,
			wantSeverity: "warn",
		},

		// --- innodb_autoinc_lock_mode ---
		{
			name:         "innodb_autoinc_lock_mode 1 with ROW fires",
			ruleID:       "innodb_autoinc_lock_mode",
			vars:         map[string]string{"innodb_autoinc_lock_mode": "1", "binlog_format": "ROW"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "innodb_autoinc_lock_mode 2 with ROW silent",
			ruleID: "innodb_autoinc_lock_mode",
			vars:   map[string]string{"innodb_autoinc_lock_mode": "2", "binlog_format": "ROW"},
		},
		{
			name:   "innodb_autoinc_lock_mode 1 with STATEMENT silent",
			ruleID: "innodb_autoinc_lock_mode",
			vars:   map[string]string{"innodb_autoinc_lock_mode": "1", "binlog_format": "STATEMENT"},
		},

		// --- replica_skip_errors ---
		{
			name:         "replica_skip_errors set fires critical",
			ruleID:       "replica_skip_errors",
			vars:         map[string]string{"replica_skip_errors": "1062,1053"},
			wantCount:    1,
			wantSeverity: "critical",
		},
		{
			name:   "replica_skip_errors OFF silent",
			ruleID: "replica_skip_errors",
			vars:   map[string]string{"replica_skip_errors": "OFF"},
		},
		{
			name:         "slave_skip_errors legacy fallback fires critical",
			ruleID:       "replica_skip_errors",
			vars:         map[string]string{"slave_skip_errors": "all"},
			wantCount:    1,
			wantSeverity: "critical",
		},

		// --- binlog_cache_size ---
		{
			name:         "binlog_cache_size at default 32K fires",
			ruleID:       "binlog_cache_size",
			vars:         map[string]string{"binlog_cache_size": "32768"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "binlog_cache_size 524288 silent",
			ruleID: "binlog_cache_size",
			vars:   map[string]string{"binlog_cache_size": "524288"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := runSingleRule(t, tc.ruleID, tc.vars)
			assertFindings(t, findings, tc.wantCount, tc.wantSeverity)
		})
	}
}

func TestReplicationSyncBinlogAuroraSkip(t *testing.T) {
	// sync_binlog should be tagged aurora-skip.
	for i := range ruleRegistry {
		if ruleRegistry[i].ID == "sync_binlog" {
			if !ruleRegistry[i].HasTag("aurora-skip") {
				t.Error("sync_binlog rule missing aurora-skip tag")
			}
			return
		}
	}
	t.Fatal("sync_binlog rule not found in registry")
}
