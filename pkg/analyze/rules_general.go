package analyze

import "strings"

// GeneralRules returns advisory rules for general MySQL configuration variables.
func GeneralRules() []RuleDefinition {
	return []RuleDefinition{
		{
			ID:       "delay_key_write",
			Category: CategoryGeneral,
			Tags:     []string{"myisam-only", "aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["delay_key_write"]
				if strings.EqualFold(v, "ON") {
					return []VariableAdvisorFinding{{
						RuleID:       "delay_key_write",
						Category:     CategoryGeneral,
						Severity:     "warn",
						Variable:     "delay_key_write",
						CurrentValue: v,
						Description:  "MyISAM index blocks are never flushed until necessary.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "myisam_recover_options",
			Category: CategoryGeneral,
			Tags:     []string{"myisam-only", "aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["myisam_recover_options"]
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "myisam_recover_options",
						Category:     CategoryGeneral,
						Severity:     "warn",
						Variable:     "myisam_recover_options",
						CurrentValue: v,
						Description:  "myisam_recover_options should be set to some value such as BACKUP,FORCE to ensure that table corruption is noticed.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "table_open_cache",
			Category: CategoryGeneral,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "table_open_cache")
				if !ok {
					return nil
				}
				if n < 4000 {
					return []VariableAdvisorFinding{{
						RuleID:       "table_open_cache",
						Category:     CategoryGeneral,
						Severity:     "note",
						Variable:     "table_open_cache",
						CurrentValue: ctx.Vars["table_open_cache"],
						Description:  "table_open_cache is below 4000 (the 8.0 default). A low value causes frequent table opens under concurrent workloads.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "table_definition_cache",
			Category: CategoryGeneral,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "table_definition_cache")
				if !ok {
					return nil
				}
				if n < 2000 {
					return []VariableAdvisorFinding{{
						RuleID:       "table_definition_cache",
						Category:     CategoryGeneral,
						Severity:     "note",
						Variable:     "table_definition_cache",
						CurrentValue: ctx.Vars["table_definition_cache"],
						Description:  "table_definition_cache is below 2000. A low value may cause frequent table definition reopens on schemas with many tables.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "open_files_limit",
			Category: CategoryGeneral,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "open_files_limit")
				if !ok {
					return nil
				}
				if n < 65535 {
					return []VariableAdvisorFinding{{
						RuleID:       "open_files_limit",
						Category:     CategoryGeneral,
						Severity:     "note",
						Variable:     "open_files_limit",
						CurrentValue: ctx.Vars["open_files_limit"],
						Description:  "open_files_limit is below 65535. Production servers should have a high limit to avoid running out of file descriptors.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "log_output",
			Category: CategoryGeneral,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["log_output"]
				if strings.EqualFold(v, "TABLE") {
					return []VariableAdvisorFinding{{
						RuleID:       "log_output",
						Category:     CategoryGeneral,
						Severity:     "warn",
						Variable:     "log_output",
						CurrentValue: v,
						Description:  "log_output is set to TABLE. Writing slow query logs and general logs to tables has high performance impact. Use FILE instead.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "character_set_server",
			Category: CategoryGeneral,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["character_set_server"]
				if v == "" {
					return nil
				}
				if !strings.EqualFold(v, "utf8mb4") {
					return []VariableAdvisorFinding{{
						RuleID:       "character_set_server",
						Category:     CategoryGeneral,
						Severity:     "note",
						Variable:     "character_set_server",
						CurrentValue: v,
						Description:  "character_set_server is not utf8mb4 (the 8.0 default). utf8mb4 is the modern standard supporting full Unicode including emojis.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "default_storage_engine",
			Category: CategoryGeneral,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["default_storage_engine"]
				if v == "" {
					return nil
				}
				if !strings.EqualFold(v, "InnoDB") {
					return []VariableAdvisorFinding{{
						RuleID:       "default_storage_engine",
						Category:     CategoryGeneral,
						Severity:     "note",
						Variable:     "default_storage_engine",
						CurrentValue: v,
						Description:  "default_storage_engine is not InnoDB. InnoDB is the standard transactional engine for MySQL 8.0+.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "slow_query_log",
			Category: CategoryGeneral,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["slow_query_log"]
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "slow_query_log",
						Category:     CategoryGeneral,
						Severity:     "note",
						Variable:     "slow_query_log",
						CurrentValue: v,
						Description:  "slow_query_log is OFF. Enable it for performance monitoring and slow query analysis.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "long_query_time",
			Category: CategoryGeneral,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "long_query_time")
				if !ok {
					return nil
				}
				if n > 10 {
					return []VariableAdvisorFinding{{
						RuleID:       "long_query_time",
						Category:     CategoryGeneral,
						Severity:     "note",
						Variable:     "long_query_time",
						CurrentValue: ctx.Vars["long_query_time"],
						Description:  "long_query_time exceeds 10 seconds. The default of 10s is already high; recommend 1-2s for production to catch more slow queries.",
					}}
				}
				return nil
			},
		},
	}
}
