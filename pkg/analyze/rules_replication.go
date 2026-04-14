package analyze

import "strings"

// ReplicationRules returns advisory rules for replication configuration variables.
func ReplicationRules() []RuleDefinition {
	return []RuleDefinition{
		{
			ID:       "expire_logs_days",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				expire := ctx.Vars["expire_logs_days"]
				logBin := ctx.Vars["log_bin"]
				if expire == "0" && strings.EqualFold(logBin, "ON") {
					return []VariableAdvisorFinding{{
						RuleID:       "expire_logs_days",
						Category:     CategoryReplication,
						Severity:     "warn",
						Variable:     "expire_logs_days",
						CurrentValue: expire,
						Description:  "Binary logs are enabled, but automatic purging is not enabled.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "log_bin",
			Category: CategoryReplication,
			Tags:     []string{"aurora-downgrade"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["log_bin"]
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "log_bin",
						Category:     CategoryReplication,
						Severity:     "warn",
						Variable:     "log_bin",
						CurrentValue: v,
						Description:  "Binary logging is disabled. Point-in-time recovery and replication are impossible.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "binlog_format",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["binlog_format"]
				if v != "" && !strings.EqualFold(v, "ROW") {
					return []VariableAdvisorFinding{{
						RuleID:       "binlog_format",
						Category:     CategoryReplication,
						Severity:     "warn",
						Variable:     "binlog_format",
						CurrentValue: v,
						Description:  "binlog_format is not ROW. ROW-based replication is the proven standard; STATEMENT is unreliable.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "sync_binlog",
			Category: CategoryReplication,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["sync_binlog"]
				logBin := ctx.Vars["log_bin"]
				if v != "" && v != "1" && strings.EqualFold(logBin, "ON") {
					return []VariableAdvisorFinding{{
						RuleID:       "sync_binlog",
						Category:     CategoryReplication,
						Severity:     "warn",
						Variable:     "sync_binlog",
						CurrentValue: v,
						Description:  "sync_binlog is not 1. Not flushing binlog every transaction risks data loss on crash.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "gtid_mode",
			Category: CategoryReplication,
			Tags:     []string{"aurora-downgrade"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["gtid_mode"]
				if v != "" && !strings.EqualFold(v, "ON") {
					return []VariableAdvisorFinding{{
						RuleID:       "gtid_mode",
						Category:     CategoryReplication,
						Severity:     "warn",
						Variable:     "gtid_mode",
						CurrentValue: v,
						Description:  "GTID mode is not enabled. GTID-based replication is the operational standard.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "enforce_gtid_consistency",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["enforce_gtid_consistency"]
				gtid := ctx.Vars["gtid_mode"]
				if !strings.EqualFold(v, "ON") && strings.EqualFold(gtid, "ON") {
					return []VariableAdvisorFinding{{
						RuleID:       "enforce_gtid_consistency",
						Category:     CategoryReplication,
						Severity:     "warn",
						Variable:     "enforce_gtid_consistency",
						CurrentValue: v,
						Description:  "enforce_gtid_consistency is not ON while gtid_mode is ON. This is required for GTID-based replication.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "binlog_row_image",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["binlog_row_image"]
				format := ctx.Vars["binlog_format"]
				if v != "" && !strings.EqualFold(v, "MINIMAL") && strings.EqualFold(format, "ROW") {
					return []VariableAdvisorFinding{{
						RuleID:       "binlog_row_image",
						Category:     CategoryReplication,
						Severity:     "note",
						Variable:     "binlog_row_image",
						CurrentValue: v,
						Description:  "binlog_row_image is not MINIMAL with ROW format. MINIMAL reduces replication bandwidth.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "replica_parallel_workers",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				// Check new name first, fall back to legacy.
				varName := "replica_parallel_workers"
				v := ctx.Vars[varName]
				if v == "" {
					varName = "slave_parallel_workers"
					v = ctx.Vars[varName]
				}
				if v == "0" || v == "1" {
					return []VariableAdvisorFinding{{
						RuleID:       "replica_parallel_workers",
						Category:     CategoryReplication,
						Severity:     "note",
						Variable:     varName,
						CurrentValue: v,
						Description:  "Parallel replication workers are not configured. Recommend 4-8 workers for parallel apply to cut replica lag.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "replica_preserve_commit_order",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				// Check parallel workers first (new name, then legacy).
				w, ok := parseIntVar(ctx.Vars, "replica_parallel_workers")
				if !ok {
					w, ok = parseIntVar(ctx.Vars, "slave_parallel_workers")
				}
				if !ok || w <= 1 {
					return nil
				}

				// Check preserve commit order (new name, then legacy).
				varName := "replica_preserve_commit_order"
				v := ctx.Vars[varName]
				if v == "" {
					varName = "slave_preserve_commit_order"
					v = ctx.Vars[varName]
				}
				if strings.EqualFold(v, "OFF") || v == "0" {
					return []VariableAdvisorFinding{{
						RuleID:       "replica_preserve_commit_order",
						Category:     CategoryReplication,
						Severity:     "warn",
						Variable:     varName,
						CurrentValue: v,
						Description:  "Parallel workers > 1 but commit order preservation is OFF. Replica can enter states the source was never in.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "binlog_transaction_dependency_tracking",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["binlog_transaction_dependency_tracking"]
				if v != "" && !strings.EqualFold(v, "WRITESET") {
					return []VariableAdvisorFinding{{
						RuleID:       "binlog_transaction_dependency_tracking",
						Category:     CategoryReplication,
						Severity:     "note",
						Variable:     "binlog_transaction_dependency_tracking",
						CurrentValue: v,
						Description:  "binlog_transaction_dependency_tracking is not WRITESET. WRITESET improves parallelization on replicas.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "binlog_expire_logs_seconds",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["binlog_expire_logs_seconds"]
				logBin := ctx.Vars["log_bin"]
				if v == "0" && strings.EqualFold(logBin, "ON") {
					return []VariableAdvisorFinding{{
						RuleID:       "binlog_expire_logs_seconds",
						Category:     CategoryReplication,
						Severity:     "warn",
						Variable:     "binlog_expire_logs_seconds",
						CurrentValue: v,
						Description:  "Binary logs are enabled, but binlog_expire_logs_seconds is 0 (no automatic purge). This is the preferred variable in MySQL 8.0+.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "log_replica_updates",
			Category: CategoryReplication,
			Tags:     []string{"aurora-downgrade"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				// Check new name first, fall back to legacy.
				varName := "log_replica_updates"
				v := ctx.Vars[varName]
				if v == "" {
					varName = "log_slave_updates"
					v = ctx.Vars[varName]
				}
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "log_replica_updates",
						Category:     CategoryReplication,
						Severity:     "warn",
						Variable:     varName,
						CurrentValue: v,
						Description:  "log_replica_updates is OFF. Required for replication chains and Group Replication.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_autoinc_lock_mode",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_autoinc_lock_mode"]
				format := ctx.Vars["binlog_format"]
				if v != "" && v != "2" && strings.EqualFold(format, "ROW") {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_autoinc_lock_mode",
						Category:     CategoryReplication,
						Severity:     "note",
						Variable:     "innodb_autoinc_lock_mode",
						CurrentValue: v,
						Description:  "innodb_autoinc_lock_mode is not 2 (interleaved) with ROW-based replication. Interleaved mode is optimal for ROW format.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "replica_skip_errors",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				// Check new name first, fall back to legacy.
				varName := "replica_skip_errors"
				v := ctx.Vars[varName]
				if v == "" {
					varName = "slave_skip_errors"
					v = ctx.Vars[varName]
				}
				if v != "" && !strings.EqualFold(v, "OFF") && v != "0" && v != "" {
					return []VariableAdvisorFinding{{
						RuleID:       "replica_skip_errors",
						Category:     CategoryReplication,
						Severity:     "critical",
						Variable:     varName,
						CurrentValue: v,
						Description:  "replica_skip_errors is configured. Never skip replication errors; resolve them directly to avoid silent data drift.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "binlog_cache_size",
			Category: CategoryReplication,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v, ok := parseIntVar(ctx.Vars, "binlog_cache_size")
				if ok && v == 32768 {
					return []VariableAdvisorFinding{{
						RuleID:       "binlog_cache_size",
						Category:     CategoryReplication,
						Severity:     "note",
						Variable:     "binlog_cache_size",
						CurrentValue: ctx.Vars["binlog_cache_size"],
						Description:  "binlog_cache_size is at the 32K default. Recommend 512K+ for ROW-based replication.",
					}}
				}
				return nil
			},
		},
	}
}
