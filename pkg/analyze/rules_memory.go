package analyze

// MemoryRules returns advisory rules for memory and per-session buffer variables.
func MemoryRules() []RuleDefinition {
	return []RuleDefinition{
		{
			ID:       "sort_buffer_size",
			Category: CategoryMemory,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "sort_buffer_size")
				if !ok {
					return nil
				}
				if n != 262144 { // 256KB default
					return []VariableAdvisorFinding{{
						RuleID:       "sort_buffer_size",
						Category:     CategoryMemory,
						Severity:     "note",
						Variable:     "sort_buffer_size",
						CurrentValue: ctx.Vars["sort_buffer_size"],
						Description:  "The sort_buffer_size variable should generally be left at its default unless an expert determines it is necessary to change it.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "sort_buffer_size_large",
			Category: CategoryMemory,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "sort_buffer_size")
				if !ok {
					return nil
				}
				if n > 2097152 { // 2MB
					return []VariableAdvisorFinding{{
						RuleID:       "sort_buffer_size_large",
						Category:     CategoryMemory,
						Severity:     "warn",
						Variable:     "sort_buffer_size",
						CurrentValue: ctx.Vars["sort_buffer_size"],
						Description:  "sort_buffer_size exceeds 2MB. Large values actually hurt performance due to memory allocation overhead. Keep at or below 2MB.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "key_buffer_size",
			Category: CategoryMemory,
			Tags:     []string{"myisam-only", "aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "key_buffer_size")
				if !ok {
					return nil
				}
				if n == 8388608 { // 8MB default
					return []VariableAdvisorFinding{{
						RuleID:       "key_buffer_size",
						Category:     CategoryMemory,
						Severity:     "warn",
						Variable:     "key_buffer_size",
						CurrentValue: ctx.Vars["key_buffer_size"],
						Description:  "The key buffer size is set to its default value, which is not good for most production systems.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "join_buffer_size",
			Category: CategoryMemory,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "join_buffer_size")
				if !ok {
					return nil
				}
				if n > 4194304 { // 4MB
					return []VariableAdvisorFinding{{
						RuleID:       "join_buffer_size",
						Category:     CategoryMemory,
						Severity:     "warn",
						Variable:     "join_buffer_size",
						CurrentValue: ctx.Vars["join_buffer_size"],
						Description:  "join_buffer_size exceeds 4MB. This is allocated per join operation per session, which can cause significant memory pressure under concurrent workloads.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "read_buffer_size",
			Category: CategoryMemory,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "read_buffer_size")
				if !ok {
					return nil
				}
				if n > 8388608 { // 8MB
					return []VariableAdvisorFinding{{
						RuleID:       "read_buffer_size",
						Category:     CategoryMemory,
						Severity:     "warn",
						Variable:     "read_buffer_size",
						CurrentValue: ctx.Vars["read_buffer_size"],
						Description:  "read_buffer_size exceeds 8MB. Oversized buffers waste memory per session and can cause swapping or OOM under load.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "read_rnd_buffer_size",
			Category: CategoryMemory,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "read_rnd_buffer_size")
				if !ok {
					return nil
				}
				if n > 4194304 { // 4MB
					return []VariableAdvisorFinding{{
						RuleID:       "read_rnd_buffer_size",
						Category:     CategoryMemory,
						Severity:     "warn",
						Variable:     "read_rnd_buffer_size",
						CurrentValue: ctx.Vars["read_rnd_buffer_size"],
						Description:  "read_rnd_buffer_size exceeds 4MB. Oversized random read buffers destabilize the server under concurrent workloads.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "tmp_table_size_mismatch",
			Category: CategoryMemory,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				tmp, okTmp := parseIntVar(ctx.Vars, "tmp_table_size")
				heap, okHeap := parseIntVar(ctx.Vars, "max_heap_table_size")
				if !okTmp || !okHeap {
					return nil
				}
				if tmp > heap {
					return []VariableAdvisorFinding{{
						RuleID:       "tmp_table_size_mismatch",
						Category:     CategoryMemory,
						Severity:     "note",
						Variable:     "tmp_table_size",
						CurrentValue: ctx.Vars["tmp_table_size"],
						Description:  "tmp_table_size is larger than max_heap_table_size. The effective limit for in-memory temp tables is the minimum of the two, so the extra tmp_table_size is ignored.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "tmp_table_size_large",
			Category: CategoryMemory,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "tmp_table_size")
				if !ok {
					return nil
				}
				if n > 268435456 { // 256MB
					return []VariableAdvisorFinding{{
						RuleID:       "tmp_table_size_large",
						Category:     CategoryMemory,
						Severity:     "warn",
						Variable:     "tmp_table_size",
						CurrentValue: ctx.Vars["tmp_table_size"],
						Description:  "tmp_table_size exceeds 256MB. Very large in-memory temp tables can cause severe memory pressure. Consider optimizing queries instead.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "max_heap_table_size",
			Category: CategoryMemory,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				heap, okHeap := parseIntVar(ctx.Vars, "max_heap_table_size")
				tmp, okTmp := parseIntVar(ctx.Vars, "tmp_table_size")
				if !okHeap || !okTmp {
					return nil
				}
				if heap < tmp {
					return []VariableAdvisorFinding{{
						RuleID:       "max_heap_table_size",
						Category:     CategoryMemory,
						Severity:     "note",
						Variable:     "max_heap_table_size",
						CurrentValue: ctx.Vars["max_heap_table_size"],
						Description:  "max_heap_table_size is less than tmp_table_size. It should be >= tmp_table_size to avoid silently limiting in-memory temp tables.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "bulk_insert_buffer_size",
			Category: CategoryMemory,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "bulk_insert_buffer_size")
				if !ok {
					return nil
				}
				if n > 67108864 { // 64MB
					return []VariableAdvisorFinding{{
						RuleID:       "bulk_insert_buffer_size",
						Category:     CategoryMemory,
						Severity:     "note",
						Variable:     "bulk_insert_buffer_size",
						CurrentValue: ctx.Vars["bulk_insert_buffer_size"],
						Description:  "bulk_insert_buffer_size exceeds 64MB. This is a per-session allocation during bulk inserts (LOAD DATA, INSERT...SELECT) and can cause memory pressure.",
					}}
				}
				return nil
			},
		},
	}
}
