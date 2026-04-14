package analyze

import "strings"

// SecurityRules returns advisory rules for security-related variables.
func SecurityRules() []RuleDefinition {
	return []RuleDefinition{
		{
			ID:       "local_infile",
			Category: CategorySecurity,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["local_infile"]
				if strings.EqualFold(v, "ON") {
					return []VariableAdvisorFinding{{
						RuleID:       "local_infile",
						Category:     CategorySecurity,
						Severity:     "warn",
						Variable:     "local_infile",
						CurrentValue: v,
						Description:  "local_infile is enabled. This is a potential security exploit vector that allows reading local files via LOAD DATA LOCAL INFILE.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "skip_name_resolve",
			Category: CategorySecurity,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["skip_name_resolve"]
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "skip_name_resolve",
						Category:     CategorySecurity,
						Severity:     "note",
						Variable:     "skip_name_resolve",
						CurrentValue: v,
						Description:  "skip_name_resolve is OFF. DNS lookups on every connection add latency and can cause connectivity issues if DNS is unreliable.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "require_secure_transport",
			Category: CategorySecurity,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["require_secure_transport"]
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "require_secure_transport",
						Category:     CategorySecurity,
						Severity:     "note",
						Variable:     "require_secure_transport",
						CurrentValue: v,
						Description:  "require_secure_transport is OFF. Clients can connect without TLS, exposing data in transit.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "sql_mode_strict",
			Category: CategorySecurity,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["sql_mode"]
				if v == "" {
					return nil
				}
				if !strings.Contains(strings.ToUpper(v), "STRICT_TRANS_TABLES") {
					return []VariableAdvisorFinding{{
						RuleID:       "sql_mode_strict",
						Category:     CategorySecurity,
						Severity:     "warn",
						Variable:     "sql_mode",
						CurrentValue: v,
						Description:  "sql_mode does not include STRICT_TRANS_TABLES. Without strict mode, MySQL silently truncates data and inserts default values instead of raising errors.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "sql_mode_no_engine_sub",
			Category: CategorySecurity,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["sql_mode"]
				if v == "" {
					return nil
				}
				if !strings.Contains(strings.ToUpper(v), "NO_ENGINE_SUBSTITUTION") {
					return []VariableAdvisorFinding{{
						RuleID:       "sql_mode_no_engine_sub",
						Category:     CategorySecurity,
						Severity:     "note",
						Variable:     "sql_mode",
						CurrentValue: v,
						Description:  "sql_mode does not include NO_ENGINE_SUBSTITUTION. CREATE TABLE may silently use a different storage engine if the specified one is unavailable.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "default_authentication_plugin",
			Category: CategorySecurity,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				// MySQL 8.0.27+ uses authentication_policy instead of default_authentication_plugin.
				if ap, ok := ctx.Vars["authentication_policy"]; ok && ap != "" {
					// authentication_policy is a comma-separated list; first element is the default.
					first := strings.Split(ap, ",")[0]
					first = strings.TrimSpace(first)
					if first != "*" && !strings.EqualFold(first, "caching_sha2_password") {
						return []VariableAdvisorFinding{{
							RuleID:       "default_authentication_plugin",
							Category:     CategorySecurity,
							Severity:     "note",
							Variable:     "authentication_policy",
							CurrentValue: ap,
							Description:  "The default authentication plugin (via authentication_policy) is not caching_sha2_password. Older authentication plugins are less secure.",
						}}
					}
					return nil
				}
				v := ctx.Vars["default_authentication_plugin"]
				if v == "" {
					return nil
				}
				if !strings.EqualFold(v, "caching_sha2_password") {
					return []VariableAdvisorFinding{{
						RuleID:       "default_authentication_plugin",
						Category:     CategorySecurity,
						Severity:     "note",
						Variable:     "default_authentication_plugin",
						CurrentValue: v,
						Description:  "default_authentication_plugin is not caching_sha2_password. Older authentication plugins like mysql_native_password are less secure.",
					}}
				}
				return nil
			},
		},
		{
			ID:       "tls_version",
			Category: CategorySecurity,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["tls_version"]
				if v == "" {
					return nil
				}
				// Parse the comma-separated list of TLS versions.
				versions := strings.Split(v, ",")
				hasOld := false
				hasModern := false
				for _, ver := range versions {
					ver = strings.TrimSpace(ver)
					if ver == "TLSv1" || ver == "TLSv1.1" {
						hasOld = true
					}
					if ver == "TLSv1.2" || ver == "TLSv1.3" {
						hasModern = true
					}
				}
				if hasOld {
					_ = hasModern // old TLS versions are a problem regardless
					return []VariableAdvisorFinding{{
						RuleID:       "tls_version",
						Category:     CategorySecurity,
						Severity:     "warn",
						Variable:     "tls_version",
						CurrentValue: v,
						Description:  "tls_version includes TLSv1 or TLSv1.1, which are deprecated and insecure. Only TLSv1.2 and TLSv1.3 should be allowed.",
					}}
				}
				return nil
			},
		},
	}
}
