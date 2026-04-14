package analyze

import "testing"

func TestMemoryRules(t *testing.T) {
	tests := []struct {
		name         string
		ruleID       string
		vars         map[string]string
		wantCount    int
		wantSeverity string
	}{
		// --- sort_buffer_size_large ---
		{
			name:         "sort_buffer_size_large fires above 2MB",
			ruleID:       "sort_buffer_size_large",
			vars:         map[string]string{"sort_buffer_size": "4194304"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "sort_buffer_size_large silent at 2MB",
			ruleID: "sort_buffer_size_large",
			vars:   map[string]string{"sort_buffer_size": "2097152"},
		},

		// --- join_buffer_size ---
		{
			name:         "join_buffer_size fires above 4MB",
			ruleID:       "join_buffer_size",
			vars:         map[string]string{"join_buffer_size": "8388608"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "join_buffer_size silent at 4MB",
			ruleID: "join_buffer_size",
			vars:   map[string]string{"join_buffer_size": "4194304"},
		},

		// --- read_buffer_size ---
		{
			name:         "read_buffer_size fires above 8MB",
			ruleID:       "read_buffer_size",
			vars:         map[string]string{"read_buffer_size": "16777216"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "read_buffer_size silent at 8MB",
			ruleID: "read_buffer_size",
			vars:   map[string]string{"read_buffer_size": "8388608"},
		},

		// --- read_rnd_buffer_size ---
		{
			name:         "read_rnd_buffer_size fires above 4MB",
			ruleID:       "read_rnd_buffer_size",
			vars:         map[string]string{"read_rnd_buffer_size": "8388608"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "read_rnd_buffer_size silent at 4MB",
			ruleID: "read_rnd_buffer_size",
			vars:   map[string]string{"read_rnd_buffer_size": "4194304"},
		},

		// --- tmp_table_size_mismatch ---
		{
			name:         "tmp_table_size_mismatch fires when tmp > heap",
			ruleID:       "tmp_table_size_mismatch",
			vars:         map[string]string{"tmp_table_size": "67108864", "max_heap_table_size": "16777216"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "tmp_table_size_mismatch silent when equal",
			ruleID: "tmp_table_size_mismatch",
			vars:   map[string]string{"tmp_table_size": "16777216", "max_heap_table_size": "16777216"},
		},

		// --- tmp_table_size_large ---
		{
			name:         "tmp_table_size_large fires above 256MB",
			ruleID:       "tmp_table_size_large",
			vars:         map[string]string{"tmp_table_size": "536870912"},
			wantCount:    1,
			wantSeverity: "warn",
		},
		{
			name:   "tmp_table_size_large silent at 256MB",
			ruleID: "tmp_table_size_large",
			vars:   map[string]string{"tmp_table_size": "268435456"},
		},

		// --- max_heap_table_size ---
		{
			name:         "max_heap_table_size fires when less than tmp",
			ruleID:       "max_heap_table_size",
			vars:         map[string]string{"max_heap_table_size": "16777216", "tmp_table_size": "67108864"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "max_heap_table_size silent when equal",
			ruleID: "max_heap_table_size",
			vars:   map[string]string{"max_heap_table_size": "67108864", "tmp_table_size": "67108864"},
		},

		// --- bulk_insert_buffer_size ---
		{
			name:         "bulk_insert_buffer_size fires above 64MB",
			ruleID:       "bulk_insert_buffer_size",
			vars:         map[string]string{"bulk_insert_buffer_size": "134217728"},
			wantCount:    1,
			wantSeverity: "note",
		},
		{
			name:   "bulk_insert_buffer_size silent at 64MB",
			ruleID: "bulk_insert_buffer_size",
			vars:   map[string]string{"bulk_insert_buffer_size": "67108864"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := runSingleRule(t, tc.ruleID, tc.vars)
			assertFindings(t, findings, tc.wantCount, tc.wantSeverity)
		})
	}
}
