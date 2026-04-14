package analyze

import "testing"

func TestPerfSchemaRules(t *testing.T) {
	tests := []struct {
		name         string
		ruleID       string
		vars         map[string]string
		wantCount    int
		wantSeverity string
	}{
		// --- performance_schema ---
		{
			name:         "performance_schema OFF fires",
			ruleID:       "performance_schema",
			vars:         map[string]string{"performance_schema": "OFF"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "performance_schema ON silent",
			ruleID: "performance_schema",
			vars:   map[string]string{"performance_schema": "ON"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := runSingleRule(t, tc.ruleID, tc.vars)
			assertFindings(t, findings, tc.wantCount, tc.wantSeverity)
		})
	}
}
