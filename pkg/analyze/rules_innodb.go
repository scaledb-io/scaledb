package analyze

import "strings"

// InnoDBRules returns advisory rules for InnoDB configuration variables.
func InnoDBRules() []RuleDefinition {
	return []RuleDefinition{
		{
			ID:       "innodb_buffer_pool_size",
			Category: CategoryInnoDB,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "innodb_buffer_pool_size")
				if !ok {
					return nil
				}
				if n == 134217728 { // 128MB default
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_buffer_pool_size",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_buffer_pool_size",
						CurrentValue: ctx.Vars["innodb_buffer_pool_size"],
						Description:  "The InnoDB buffer pool size is unconfigured.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_log_file_size",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "innodb_log_file_size")
				if !ok {
					return nil
				}
				if n == 50331648 { // 48MB default
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_log_file_size",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_log_file_size",
						CurrentValue: ctx.Vars["innodb_log_file_size"],
						Description:  "The InnoDB log file size is set to its default value, which is not usable on production systems.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_data_file_path",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_data_file_path"]
				if strings.Contains(strings.ToLower(v), "autoextend") {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_data_file_path",
						Category:     CategoryInnoDB,
						Severity:     "note",
						Variable:     "innodb_data_file_path",
						CurrentValue: v,
						Description:  "Auto-extending InnoDB files can consume a lot of disk space that is very difficult to reclaim later.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_flush_method",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_flush_method"]
				if !strings.EqualFold(v, "O_DIRECT") {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_flush_method",
						Category:     CategoryInnoDB,
						Severity:     "note",
						Variable:     "innodb_flush_method",
						CurrentValue: v,
						Description:  "Most production database servers that use InnoDB should set innodb_flush_method to O_DIRECT to avoid double-buffering, unless the I/O system is very low performance.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_file_per_table",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_file_per_table"]
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_file_per_table",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_file_per_table",
						CurrentValue: v,
						Description:  "innodb_file_per_table should be ON for better space management and tablespace recovery.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_flush_log_at_trx_commit",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-downgrade"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_flush_log_at_trx_commit"]
				if v == "" {
					return nil
				}
				if v != "1" {
					desc := "innodb_flush_log_at_trx_commit is not set to 1; the server is not strictly ACID-compliant and a crash can lose transactions."
					if v == "0" {
						desc += " Value 0 provides no benefit over value 2, with more data loss risk."
					}
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_flush_log_at_trx_commit",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_flush_log_at_trx_commit",
						CurrentValue: v,
						Description:  desc,
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_doublewrite",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_doublewrite"]
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_doublewrite",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_doublewrite",
						CurrentValue: v,
						Description:  "innodb_doublewrite is disabled; data is unsafe without partial-page write protection.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_flush_neighbors",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_flush_neighbors"]
				if v != "" && v != "0" {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_flush_neighbors",
						Category:     CategoryInnoDB,
						Severity:     "note",
						Variable:     "innodb_flush_neighbors",
						CurrentValue: v,
						Description:  "innodb_flush_neighbors should be 0 for SSD storage, which is the MySQL 8.0 default.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_io_capacity",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "innodb_io_capacity")
				if !ok {
					return nil
				}
				if n == 200 {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_io_capacity",
						Category:     CategoryInnoDB,
						Severity:     "note",
						Variable:     "innodb_io_capacity",
						CurrentValue: ctx.Vars["innodb_io_capacity"],
						Description:  "innodb_io_capacity is at the conservative default of 200; tune for actual storage IOPS.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_io_capacity_max",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "innodb_io_capacity_max")
				if !ok {
					return nil
				}
				if n == 2000 {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_io_capacity_max",
						Category:     CategoryInnoDB,
						Severity:     "note",
						Variable:     "innodb_io_capacity_max",
						CurrentValue: ctx.Vars["innodb_io_capacity_max"],
						Description:  "innodb_io_capacity_max is at the default of 2000; should match actual storage burst IOPS.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_log_buffer_size",
			Category: CategoryInnoDB,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "innodb_log_buffer_size")
				if !ok {
					return nil
				}
				if n > 16777216 { // 16MB
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_log_buffer_size",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_log_buffer_size",
						CurrentValue: ctx.Vars["innodb_log_buffer_size"],
						Description:  "innodb_log_buffer_size exceeds 16MB; generally should not exceed 16MB.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_force_recovery",
			Category: CategoryInnoDB,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "innodb_force_recovery")
				if !ok {
					return nil
				}
				if n > 0 {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_force_recovery",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_force_recovery",
						CurrentValue: ctx.Vars["innodb_force_recovery"],
						Description:  "innodb_force_recovery is set to a non-zero value; this is forced recovery mode intended only for temporary corruption recovery.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_fast_shutdown",
			Category: CategoryInnoDB,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_fast_shutdown"]
				if v != "" && v != "1" {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_fast_shutdown",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_fast_shutdown",
						CurrentValue: v,
						Description:  "innodb_fast_shutdown is not set to 1; non-default shutdown can cause performance issues or require crash recovery on restart.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_lock_wait_timeout",
			Category: CategoryInnoDB,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "innodb_lock_wait_timeout")
				if !ok {
					return nil
				}
				if n > 120 {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_lock_wait_timeout",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_lock_wait_timeout",
						CurrentValue: ctx.Vars["innodb_lock_wait_timeout"],
						Description:  "innodb_lock_wait_timeout exceeds 120 seconds; can cause system overload if locks are not released.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_dedicated_server",
			Category: CategoryInnoDB,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_dedicated_server"]
				if strings.EqualFold(v, "ON") {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_dedicated_server",
						Category:     CategoryInnoDB,
						Severity:     "note",
						Variable:     "innodb_dedicated_server",
						CurrentValue: v,
						Description:  "innodb_dedicated_server is enabled; MySQL auto-configures buffer pool size, log file size, and flush method based on detected memory.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_print_all_deadlocks",
			Category: CategoryInnoDB,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_print_all_deadlocks"]
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_print_all_deadlocks",
						Category:     CategoryInnoDB,
						Severity:     "note",
						Variable:     "innodb_print_all_deadlocks",
						CurrentValue: v,
						Description:  "innodb_print_all_deadlocks is disabled; enable to monitor deadlock frequency in the error log.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_stats_persistent",
			Category: CategoryInnoDB,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_stats_persistent"]
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_stats_persistent",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_stats_persistent",
						CurrentValue: v,
						Description:  "innodb_stats_persistent is disabled; non-persistent statistics are recalculated on restart, causing query plan instability.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_checksum_algorithm",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["innodb_checksum_algorithm"]
				if v != "" && !strings.EqualFold(v, "crc32") {
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_checksum_algorithm",
						Category:     CategoryInnoDB,
						Severity:     "note",
						Variable:     "innodb_checksum_algorithm",
						CurrentValue: v,
						Description:  "innodb_checksum_algorithm is not set to crc32; crc32 is the fastest and recommended algorithm for MySQL 8.0+.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_redo_log_capacity",
			Category: CategoryInnoDB,
			Tags:     []string{"aurora-skip"},
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				if !ctx.Version.AtLeast(8, 0, 30) {
					return nil
				}
				n, ok := parseIntVar(ctx.Vars, "innodb_redo_log_capacity")
				if !ok {
					return nil
				}
				if n == 104857600 { // 100MB default
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_redo_log_capacity",
						Category:     CategoryInnoDB,
						Severity:     "warn",
						Variable:     "innodb_redo_log_capacity",
						CurrentValue: ctx.Vars["innodb_redo_log_capacity"],
						Description:  "innodb_redo_log_capacity is at the 100MB default (replaces innodb_log_file_size in 8.0.30+); increase for production workloads.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "innodb_buffer_pool_instances",
			Category: CategoryInnoDB,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				poolSize, ok := parseIntVar(ctx.Vars, "innodb_buffer_pool_size")
				if !ok {
					return nil
				}
				instances := ctx.Vars["innodb_buffer_pool_instances"]
				if poolSize > 1073741824 && instances == "1" { // > 1GB
					return []VariableAdvisorFinding{{
						RuleID:       "innodb_buffer_pool_instances",
						Category:     CategoryInnoDB,
						Severity:     "note",
						Variable:     "innodb_buffer_pool_instances",
						CurrentValue: instances,
						Description:  "innodb_buffer_pool_size exceeds 1GB but innodb_buffer_pool_instances is 1; recommend 1 instance per GB, up to a maximum of 64.",
					}}
				}
				return nil
			},
		},
	}
}
