package analyze

import "testing"

func TestExistingRules(t *testing.T) {
	tests := []struct {
		name         string
		ruleID       string
		vars         map[string]string
		wantCount    int
		wantSeverity string
	}{
		// --- delay_key_write ---
		{
			name:         "delay_key_write ON fires",
			ruleID:       "delay_key_write",
			vars:         map[string]string{"delay_key_write": "ON"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "delay_key_write OFF silent",
			ruleID: "delay_key_write",
			vars:   map[string]string{"delay_key_write": "OFF"},
		},

		// --- innodb_buffer_pool_size ---
		{
			name:         "innodb_buffer_pool_size at default fires",
			ruleID:       "innodb_buffer_pool_size",
			vars:         map[string]string{"innodb_buffer_pool_size": "134217728"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "innodb_buffer_pool_size configured silent",
			ruleID: "innodb_buffer_pool_size",
			vars:   map[string]string{"innodb_buffer_pool_size": "8589934592"},
		},

		// --- innodb_log_file_size ---
		{
			name:         "innodb_log_file_size at default fires",
			ruleID:       "innodb_log_file_size",
			vars:         map[string]string{"innodb_log_file_size": "50331648"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "innodb_log_file_size configured silent",
			ruleID: "innodb_log_file_size",
			vars:   map[string]string{"innodb_log_file_size": "268435456"},
		},

		// --- key_buffer_size ---
		{
			name:         "key_buffer_size at default fires",
			ruleID:       "key_buffer_size",
			vars:         map[string]string{"key_buffer_size": "8388608"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "key_buffer_size configured silent",
			ruleID: "key_buffer_size",
			vars:   map[string]string{"key_buffer_size": "33554432"},
		},

		// --- sort_buffer_size ---
		{
			name:         "sort_buffer_size non-default fires",
			ruleID:       "sort_buffer_size",
			vars:         map[string]string{"sort_buffer_size": "524288"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "sort_buffer_size at default silent",
			ruleID: "sort_buffer_size",
			vars:   map[string]string{"sort_buffer_size": "262144"},
		},

		// --- expire_logs_days ---
		{
			name:         "expire_logs_days 0 with log_bin ON fires",
			ruleID:       "expire_logs_days",
			vars:         map[string]string{"expire_logs_days": "0", "log_bin": "ON"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "expire_logs_days 0 with log_bin OFF silent",
			ruleID: "expire_logs_days",
			vars:   map[string]string{"expire_logs_days": "0", "log_bin": "OFF"},
		},
		{
			name:   "expire_logs_days nonzero silent",
			ruleID: "expire_logs_days",
			vars:   map[string]string{"expire_logs_days": "7", "log_bin": "ON"},
		},

		// --- innodb_data_file_path ---
		{
			name:         "innodb_data_file_path autoextend fires",
			ruleID:       "innodb_data_file_path",
			vars:         map[string]string{"innodb_data_file_path": "ibdata1:12M:autoextend"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "innodb_data_file_path fixed size silent",
			ruleID: "innodb_data_file_path",
			vars:   map[string]string{"innodb_data_file_path": "ibdata1:12M"},
		},

		// --- innodb_flush_method ---
		{
			name:         "innodb_flush_method not O_DIRECT fires",
			ruleID:       "innodb_flush_method",
			vars:         map[string]string{"innodb_flush_method": "fsync"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "innodb_flush_method O_DIRECT silent",
			ruleID: "innodb_flush_method",
			vars:   map[string]string{"innodb_flush_method": "O_DIRECT"},
		},

		// --- myisam_recover_options ---
		{
			name:         "myisam_recover_options OFF fires",
			ruleID:       "myisam_recover_options",
			vars:         map[string]string{"myisam_recover_options": "OFF"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "myisam_recover_options BACKUP,FORCE silent",
			ruleID: "myisam_recover_options",
			vars:   map[string]string{"myisam_recover_options": "BACKUP,FORCE"},
		},

		// --- max_connections ---
		{
			name:         "max_connections over 1000 fires",
			ruleID:       "max_connections",
			vars:         map[string]string{"max_connections": "2000"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "max_connections at 500 silent",
			ruleID: "max_connections",
			vars:   map[string]string{"max_connections": "500"},
		},

		// --- innodb_file_per_table ---
		{
			name:         "innodb_file_per_table OFF fires",
			ruleID:       "innodb_file_per_table",
			vars:         map[string]string{"innodb_file_per_table": "OFF"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "innodb_file_per_table ON silent",
			ruleID: "innodb_file_per_table",
			vars:   map[string]string{"innodb_file_per_table": "ON"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := runSingleRule(t, tc.ruleID, tc.vars)
			assertFindings(t, findings, tc.wantCount, tc.wantSeverity)
		})
	}
}

func TestAuroraSkipsTaggedRules(t *testing.T) {
	auroraSkipRules := []struct {
		ruleID string
		vars   map[string]string
	}{
		{"innodb_log_file_size", map[string]string{"innodb_log_file_size": "50331648"}},
		{"innodb_flush_method", map[string]string{"innodb_flush_method": "fsync"}},
		{"innodb_file_per_table", map[string]string{"innodb_file_per_table": "OFF"}},
		{"innodb_data_file_path", map[string]string{"innodb_data_file_path": "ibdata1:12M:autoextend"}},
	}

	for _, tc := range auroraSkipRules {
		t.Run(tc.ruleID+"_on_aurora", func(t *testing.T) {
			// Should fire on vanilla MySQL.
			findings := runSingleRule(t, tc.ruleID, tc.vars)
			if len(findings) == 0 {
				t.Fatal("expected rule to fire on vanilla MySQL")
			}

			// The rule itself still fires (it doesn't know about Aurora),
			// but CheckVariables skips it. Verify the tag is set.
			for i := range ruleRegistry {
				if ruleRegistry[i].ID == tc.ruleID {
					if !ruleRegistry[i].HasTag("aurora-skip") {
						t.Errorf("rule %q missing aurora-skip tag", tc.ruleID)
					}
					break
				}
			}
		})
	}
}

func TestAuroraDowngradeTaggedRules(t *testing.T) {
	downgradeRules := []struct {
		ruleID string
		vars   map[string]string
	}{
		{"innodb_flush_log_at_trx_commit", map[string]string{"innodb_flush_log_at_trx_commit": "0"}},
		{"log_bin", map[string]string{"log_bin": "OFF"}},
		{"gtid_mode", map[string]string{"gtid_mode": "OFF"}},
		{"log_replica_updates", map[string]string{"log_replica_updates": "OFF"}},
	}

	for _, tc := range downgradeRules {
		t.Run(tc.ruleID+"_vanilla", func(t *testing.T) {
			findings := runSingleRule(t, tc.ruleID, tc.vars)
			if len(findings) == 0 {
				t.Fatal("expected rule to fire")
			}
			if findings[0].Severity != "warn" {
				t.Errorf("expected warn on vanilla MySQL, got %q", findings[0].Severity)
			}
		})

		t.Run(tc.ruleID+"_has_tag", func(t *testing.T) {
			for i := range ruleRegistry {
				if ruleRegistry[i].ID == tc.ruleID {
					if !ruleRegistry[i].HasTag("aurora-downgrade") {
						t.Errorf("rule %q missing aurora-downgrade tag", tc.ruleID)
					}
					return
				}
			}
			t.Fatalf("rule %q not found", tc.ruleID)
		})
	}
}

func TestAuroraSkipsMyISAMRules(t *testing.T) {
	myisamRules := []string{"delay_key_write", "myisam_recover_options", "key_buffer_size"}
	for _, ruleID := range myisamRules {
		t.Run(ruleID, func(t *testing.T) {
			for i := range ruleRegistry {
				if ruleRegistry[i].ID == ruleID {
					if !ruleRegistry[i].HasTag("aurora-skip") {
						t.Errorf("rule %q missing aurora-skip tag", ruleID)
					}
					return
				}
			}
			t.Fatalf("rule %q not found", ruleID)
		})
	}
}

func TestRuleRegistryNoDuplicateIDs(t *testing.T) {
	seen := make(map[string]bool)
	for _, rule := range ruleRegistry {
		if seen[rule.ID] {
			t.Errorf("duplicate rule ID: %q", rule.ID)
		}
		seen[rule.ID] = true
	}
}

func TestRuleRegistryAllHaveCategory(t *testing.T) {
	for _, rule := range ruleRegistry {
		if rule.Category == "" {
			t.Errorf("rule %q has empty category", rule.ID)
		}
	}
}
