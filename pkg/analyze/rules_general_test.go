package analyze

import "testing"

func TestGeneralRules(t *testing.T) {
	tests := []struct {
		name         string
		ruleID       string
		vars         map[string]string
		wantCount    int
		wantSeverity string
	}{
		// --- table_open_cache ---
		{
			name:         "table_open_cache low fires",
			ruleID:       "table_open_cache",
			vars:         map[string]string{"table_open_cache": "2000"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "table_open_cache at 4000 silent",
			ruleID: "table_open_cache",
			vars:   map[string]string{"table_open_cache": "4000"},
		},

		// --- table_definition_cache ---
		{
			name:         "table_definition_cache low fires",
			ruleID:       "table_definition_cache",
			vars:         map[string]string{"table_definition_cache": "1000"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "table_definition_cache at 2000 silent",
			ruleID: "table_definition_cache",
			vars:   map[string]string{"table_definition_cache": "2000"},
		},

		// --- open_files_limit ---
		{
			name:         "open_files_limit low fires",
			ruleID:       "open_files_limit",
			vars:         map[string]string{"open_files_limit": "1024"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "open_files_limit high silent",
			ruleID: "open_files_limit",
			vars:   map[string]string{"open_files_limit": "65535"},
		},

		// --- log_output ---
		{
			name:         "log_output TABLE fires",
			ruleID:       "log_output",
			vars:         map[string]string{"log_output": "TABLE"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "log_output FILE silent",
			ruleID: "log_output",
			vars:   map[string]string{"log_output": "FILE"},
		},

		// --- character_set_server ---
		{
			name:         "character_set_server not utf8mb4 fires",
			ruleID:       "character_set_server",
			vars:         map[string]string{"character_set_server": "latin1"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "character_set_server utf8mb4 silent",
			ruleID: "character_set_server",
			vars:   map[string]string{"character_set_server": "utf8mb4"},
		},

		// --- default_storage_engine ---
		{
			name:         "default_storage_engine not InnoDB fires",
			ruleID:       "default_storage_engine",
			vars:         map[string]string{"default_storage_engine": "MyISAM"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "default_storage_engine InnoDB silent",
			ruleID: "default_storage_engine",
			vars:   map[string]string{"default_storage_engine": "InnoDB"},
		},
		{
			name:   "default_storage_engine innodb lowercase silent",
			ruleID: "default_storage_engine",
			vars:   map[string]string{"default_storage_engine": "innodb"},
		},

		// --- slow_query_log ---
		{
			name:         "slow_query_log OFF fires",
			ruleID:       "slow_query_log",
			vars:         map[string]string{"slow_query_log": "OFF"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "slow_query_log ON silent",
			ruleID: "slow_query_log",
			vars:   map[string]string{"slow_query_log": "ON"},
		},

		// --- long_query_time ---
		{
			name:         "long_query_time high fires",
			ruleID:       "long_query_time",
			vars:         map[string]string{"long_query_time": "30"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "long_query_time at 10 silent",
			ruleID: "long_query_time",
			vars:   map[string]string{"long_query_time": "10"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := runSingleRule(t, tc.ruleID, tc.vars)
			assertFindings(t, findings, tc.wantCount, tc.wantSeverity)
		})
	}
}
