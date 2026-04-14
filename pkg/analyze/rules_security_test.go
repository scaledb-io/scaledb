package analyze

import "testing"

func TestSecurityRules(t *testing.T) {
	tests := []struct {
		name         string
		ruleID       string
		vars         map[string]string
		wantCount    int
		wantSeverity string
	}{
		// --- local_infile ---
		{
			name:         "local_infile ON fires",
			ruleID:       "local_infile",
			vars:         map[string]string{"local_infile": "ON"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "local_infile OFF silent",
			ruleID: "local_infile",
			vars:   map[string]string{"local_infile": "OFF"},
		},

		// --- skip_name_resolve ---
		{
			name:         "skip_name_resolve OFF fires",
			ruleID:       "skip_name_resolve",
			vars:         map[string]string{"skip_name_resolve": "OFF"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "skip_name_resolve ON silent",
			ruleID: "skip_name_resolve",
			vars:   map[string]string{"skip_name_resolve": "ON"},
		},

		// --- require_secure_transport ---
		{
			name:         "require_secure_transport OFF fires",
			ruleID:       "require_secure_transport",
			vars:         map[string]string{"require_secure_transport": "OFF"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "require_secure_transport ON silent",
			ruleID: "require_secure_transport",
			vars:   map[string]string{"require_secure_transport": "ON"},
		},

		// --- sql_mode_strict ---
		{
			name:         "sql_mode missing STRICT_TRANS_TABLES fires",
			ruleID:       "sql_mode_strict",
			vars:         map[string]string{"sql_mode": "NO_ENGINE_SUBSTITUTION"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "sql_mode with STRICT_TRANS_TABLES silent",
			ruleID: "sql_mode_strict",
			vars:   map[string]string{"sql_mode": "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"},
		},

		// --- sql_mode_no_engine_sub ---
		{
			name:         "sql_mode missing NO_ENGINE_SUBSTITUTION fires",
			ruleID:       "sql_mode_no_engine_sub",
			vars:         map[string]string{"sql_mode": "STRICT_TRANS_TABLES"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "sql_mode with NO_ENGINE_SUBSTITUTION silent",
			ruleID: "sql_mode_no_engine_sub",
			vars:   map[string]string{"sql_mode": "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"},
		},

		// --- default_authentication_plugin ---
		{
			name:         "default_authentication_plugin mysql_native fires",
			ruleID:       "default_authentication_plugin",
			vars:         map[string]string{"default_authentication_plugin": "mysql_native_password"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "default_authentication_plugin caching_sha2 silent",
			ruleID: "default_authentication_plugin",
			vars:   map[string]string{"default_authentication_plugin": "caching_sha2_password"},
		},
		{
			name:         "authentication_policy non-sha2 fires",
			ruleID:       "default_authentication_plugin",
			vars:         map[string]string{"authentication_policy": "mysql_native_password,,"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "authentication_policy wildcard silent",
			ruleID: "default_authentication_plugin",
			vars:   map[string]string{"authentication_policy": "*,,"},
		},
		{
			name:   "authentication_policy caching_sha2 silent",
			ruleID: "default_authentication_plugin",
			vars:   map[string]string{"authentication_policy": "caching_sha2_password,,"},
		},

		// --- tls_version ---
		{
			name:         "tls_version with TLSv1 fires",
			ruleID:       "tls_version",
			vars:         map[string]string{"tls_version": "TLSv1,TLSv1.1,TLSv1.2"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:         "tls_version with only TLSv1.1 fires",
			ruleID:       "tls_version",
			vars:         map[string]string{"tls_version": "TLSv1.1,TLSv1.2"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "tls_version TLSv1.2 only silent",
			ruleID: "tls_version",
			vars:   map[string]string{"tls_version": "TLSv1.2,TLSv1.3"},
		},
		{
			name:   "tls_version TLSv1.3 only silent",
			ruleID: "tls_version",
			vars:   map[string]string{"tls_version": "TLSv1.3"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := runSingleRule(t, tc.ruleID, tc.vars)
			assertFindings(t, findings, tc.wantCount, tc.wantSeverity)
		})
	}
}
