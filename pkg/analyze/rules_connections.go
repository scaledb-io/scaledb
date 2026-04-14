package analyze

// ConnectionRules returns advisory rules for connection management variables.
func ConnectionRules() []RuleDefinition {
	return []RuleDefinition{
		{
			ID:       "max_connections",
			Category: CategoryConnections,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "max_connections")
				if !ok {
					return nil
				}
				if n > 1000 {
					return []VariableAdvisorFinding{{
						RuleID:       "max_connections",
						Category:     CategoryConnections,
						Severity:     "note",
						Variable:     "max_connections",
						CurrentValue: ctx.Vars["max_connections"],
						Description:  "max_connections is set very high. Consider whether this is truly necessary.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "max_connections_critical",
			Category: CategoryConnections,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "max_connections")
				if !ok {
					return nil
				}
				if n > 5000 {
					return []VariableAdvisorFinding{{
						RuleID:       "max_connections_critical",
						Category:     CategoryConnections,
						Severity:     "critical",
						Variable:     "max_connections",
						CurrentValue: ctx.Vars["max_connections"],
						Description:  "max_connections is above 5000, which is almost certainly misconfigured. This can exhaust memory and destabilize the server.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "thread_cache_size",
			Category: CategoryConnections,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v, ok := ctx.Vars["thread_cache_size"]
				if !ok {
					return nil
				}
				if v == "0" {
					return []VariableAdvisorFinding{{
						RuleID:       "thread_cache_size",
						Category:     CategoryConnections,
						Severity:     "warn",
						Variable:     "thread_cache_size",
						CurrentValue: v,
						Description:  "thread_cache_size is 0. A new thread is created for every connection, adding overhead. Set to at least 8-16.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "wait_timeout_high",
			Category: CategoryConnections,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "wait_timeout")
				if !ok {
					return nil
				}
				if n > 28800 {
					return []VariableAdvisorFinding{{
						RuleID:       "wait_timeout_high",
						Category:     CategoryConnections,
						Severity:     "note",
						Variable:     "wait_timeout",
						CurrentValue: ctx.Vars["wait_timeout"],
						Description:  "wait_timeout exceeds the 8-hour default. Idle connections waste resources and may hold locks.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "wait_timeout_low",
			Category: CategoryConnections,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "wait_timeout")
				if !ok {
					return nil
				}
				if n < 60 {
					return []VariableAdvisorFinding{{
						RuleID:       "wait_timeout_low",
						Category:     CategoryConnections,
						Severity:     "warn",
						Variable:     "wait_timeout",
						CurrentValue: ctx.Vars["wait_timeout"],
						Description:  "wait_timeout is below 60 seconds. This may kill legitimate connections that are briefly idle.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "interactive_timeout",
			Category: CategoryConnections,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				it, okIT := ctx.Vars["interactive_timeout"]
				wt, okWT := ctx.Vars["wait_timeout"]
				if !okIT || !okWT {
					return nil
				}
				if it != wt {
					return []VariableAdvisorFinding{{
						RuleID:       "interactive_timeout",
						Category:     CategoryConnections,
						Severity:     "note",
						Variable:     "interactive_timeout",
						CurrentValue: it,
						Description:  "interactive_timeout differs from wait_timeout. These should usually match to avoid confusing timeout behavior.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "max_connect_errors",
			Category: CategoryConnections,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "max_connect_errors")
				if !ok {
					return nil
				}
				if n < 100000 {
					return []VariableAdvisorFinding{{
						RuleID:       "max_connect_errors",
						Category:     CategoryConnections,
						Severity:     "note",
						Variable:     "max_connect_errors",
						CurrentValue: ctx.Vars["max_connect_errors"],
						Description:  "max_connect_errors is below 100000. A low value may block legitimate hosts after transient failures. Set very large or use FLUSH HOSTS.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "connect_timeout",
			Category: CategoryConnections,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "connect_timeout")
				if !ok {
					return nil
				}
				if n > 30 {
					return []VariableAdvisorFinding{{
						RuleID:       "connect_timeout",
						Category:     CategoryConnections,
						Severity:     "note",
						Variable:     "connect_timeout",
						CurrentValue: ctx.Vars["connect_timeout"],
						Description:  "connect_timeout exceeds 30 seconds. A large value creates DoS vulnerability by allowing slow connection attempts to tie up resources.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "max_allowed_packet",
			Category: CategoryConnections,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				n, ok := parseIntVar(ctx.Vars, "max_allowed_packet")
				if !ok {
					return nil
				}
				if n < 67108864 {
					return []VariableAdvisorFinding{{
						RuleID:       "max_allowed_packet",
						Category:     CategoryConnections,
						Severity:     "note",
						Variable:     "max_allowed_packet",
						CurrentValue: ctx.Vars["max_allowed_packet"],
						Description:  "max_allowed_packet is below 64MB (the 8.0 default). This may cause errors for large inserts or BLOB operations.",
					}}
				}
				return nil
			},
		},
	}
}
