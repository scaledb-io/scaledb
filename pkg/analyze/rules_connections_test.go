package analyze

import "testing"

func TestConnectionRules(t *testing.T) {
	tests := []struct {
		name         string
		ruleID       string
		vars         map[string]string
		wantCount    int
		wantSeverity string
	}{
		// --- max_connections_critical ---
		{
			name:         "max_connections_critical fires above 5000",
			ruleID:       "max_connections_critical",
			vars:         map[string]string{"max_connections": "10000"},
			wantCount:    1,
			wantSeverity: "critical",
		},
		{
			name:   "max_connections_critical silent at 5000",
			ruleID: "max_connections_critical",
			vars:   map[string]string{"max_connections": "5000"},
		},
		{
			name:   "max_connections_critical silent at 1500",
			ruleID: "max_connections_critical",
			vars:   map[string]string{"max_connections": "1500"},
		},

		// --- thread_cache_size ---
		{
			name:         "thread_cache_size zero fires",
			ruleID:       "thread_cache_size",
			vars:         map[string]string{"thread_cache_size": "0"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "thread_cache_size nonzero silent",
			ruleID: "thread_cache_size",
			vars:   map[string]string{"thread_cache_size": "16"},
		},

		// --- wait_timeout_high ---
		{
			name:         "wait_timeout_high fires above 28800",
			ruleID:       "wait_timeout_high",
			vars:         map[string]string{"wait_timeout": "86400"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "wait_timeout_high silent at 28800",
			ruleID: "wait_timeout_high",
			vars:   map[string]string{"wait_timeout": "28800"},
		},

		// --- wait_timeout_low ---
		{
			name:         "wait_timeout_low fires below 60",
			ruleID:       "wait_timeout_low",
			vars:         map[string]string{"wait_timeout": "30"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "wait_timeout_low silent at 60",
			ruleID: "wait_timeout_low",
			vars:   map[string]string{"wait_timeout": "60"},
		},

		// --- interactive_timeout ---
		{
			name:         "interactive_timeout mismatch fires",
			ruleID:       "interactive_timeout",
			vars:         map[string]string{"interactive_timeout": "3600", "wait_timeout": "28800"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "interactive_timeout matching silent",
			ruleID: "interactive_timeout",
			vars:   map[string]string{"interactive_timeout": "28800", "wait_timeout": "28800"},
		},

		// --- max_connect_errors ---
		{
			name:         "max_connect_errors low fires",
			ruleID:       "max_connect_errors",
			vars:         map[string]string{"max_connect_errors": "100"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "max_connect_errors high silent",
			ruleID: "max_connect_errors",
			vars:   map[string]string{"max_connect_errors": "100000"},
		},

		// --- connect_timeout ---
		{
			name:         "connect_timeout high fires",
			ruleID:       "connect_timeout",
			vars:         map[string]string{"connect_timeout": "60"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "connect_timeout normal silent",
			ruleID: "connect_timeout",
			vars:   map[string]string{"connect_timeout": "10"},
		},

		// --- max_allowed_packet ---
		{
			name:         "max_allowed_packet low fires",
			ruleID:       "max_allowed_packet",
			vars:         map[string]string{"max_allowed_packet": "4194304"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "max_allowed_packet at 64MB silent",
			ruleID: "max_allowed_packet",
			vars:   map[string]string{"max_allowed_packet": "67108864"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := runSingleRule(t, tc.ruleID, tc.vars)
			assertFindings(t, findings, tc.wantCount, tc.wantSeverity)
		})
	}
}
