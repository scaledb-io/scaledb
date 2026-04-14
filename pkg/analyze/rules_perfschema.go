package analyze

import "strings"

// PerfSchemaRules returns advisory rules for Performance Schema configuration.
func PerfSchemaRules() []RuleDefinition {
	return []RuleDefinition{
		{
			ID:       "performance_schema",
			Category: CategoryPerfSchema,
			Check: func(ctx *RuleContext) []VariableAdvisorFinding {
				v := ctx.Vars["performance_schema"]
				if strings.EqualFold(v, "OFF") {
					return []VariableAdvisorFinding{{
						RuleID:       "performance_schema",
						Category:     CategoryPerfSchema,
						Severity:     "warn",
						Variable:     "performance_schema",
						CurrentValue: v,
						Description:  "performance_schema is OFF. This disables all Performance Schema instrumentation, which is essential for monitoring and diagnostics in production.",
					}}
				}
				return nil
			},
		},
	}
}
