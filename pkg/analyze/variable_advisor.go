package analyze

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// VariableAdvisorFinding represents a single advisory finding for a MySQL global variable.
type VariableAdvisorFinding struct {
	RuleID       string       `json:"rule_id"`
	Category     RuleCategory `json:"category"`
	Severity     string       `json:"severity"` // "critical", "warn", "note"
	Variable     string       `json:"variable"`
	CurrentValue string       `json:"current_value"`
	Description  string       `json:"description"`
}

// CheckVariablesOptions controls filtering behavior for CheckVariables.
type CheckVariablesOptions struct {
	Category string // filter to a single category; empty means all
}

// CheckVariablesResult holds the findings from a variable advisor run.
type CheckVariablesResult struct {
	Findings     []VariableAdvisorFinding                    `json:"findings"`
	ByCategory   map[RuleCategory][]VariableAdvisorFinding   `json:"by_category"`
	IsAurora     bool                                        `json:"is_aurora"`
	Version      string                                      `json:"version"`
	TotalRules   int                                         `json:"total_rules"`
	RulesSkipped int                                         `json:"rules_skipped"`
}

// CheckVariables queries MySQL global variables and checks them against the
// rule registry. Findings are returned sorted by severity (critical first,
// then warn, then note). Aurora-managed rules are automatically skipped
// when running against Amazon Aurora MySQL.
func CheckVariables(ctx context.Context, db *sql.DB, opts CheckVariablesOptions) (*CheckVariablesResult, error) {
	vars, err := fetchGlobalVariables(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("fetching global variables: %w", err)
	}

	rctx := BuildRuleContext(vars)

	result := &CheckVariablesResult{
		ByCategory: make(map[RuleCategory][]VariableAdvisorFinding),
		IsAurora:   rctx.IsAurora,
		Version:    rctx.Version.Raw,
		TotalRules: len(ruleRegistry),
	}

	filterCategory := RuleCategory(opts.Category)

	for i := range ruleRegistry {
		rule := &ruleRegistry[i]

		if filterCategory != "" && rule.Category != filterCategory {
			continue
		}

		if rctx.IsAurora && rule.HasTag("aurora-skip") {
			result.RulesSkipped++
			continue
		}

		findings := rule.Check(&rctx)

		// On Aurora, downgrade tagged rules from warn to note.
		if rctx.IsAurora && rule.HasTag("aurora-downgrade") {
			for j := range findings {
				if findings[j].Severity == "warn" {
					findings[j].Severity = "note"
				}
			}
		}

		result.Findings = append(result.Findings, findings...)
		for _, f := range findings {
			result.ByCategory[f.Category] = append(result.ByCategory[f.Category], f)
		}
	}

	sort.Slice(result.Findings, func(i, j int) bool {
		return severityRank(result.Findings[i].Severity) < severityRank(result.Findings[j].Severity)
	})

	return result, nil
}

func fetchGlobalVariables(ctx context.Context, db *sql.DB) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT variable_name, variable_value FROM performance_schema.global_variables")
	if err != nil {
		return nil, fmt.Errorf("querying global_variables: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	vars := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, fmt.Errorf("scanning variable row: %w", err)
		}
		vars[strings.ToLower(name)] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating variable rows: %w", err)
	}

	return vars, nil
}

func severityRank(severity string) int {
	switch severity {
	case "critical":
		return 0
	case "warn":
		return 1
	case "note":
		return 2
	default:
		return 3
	}
}
