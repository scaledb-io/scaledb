package analyze

import "testing"

// runSingleRule runs a specific rule by ID against the given variables and returns findings.
func runSingleRule(t *testing.T, ruleID string, vars map[string]string) []VariableAdvisorFinding {
	t.Helper()
	ctx := BuildRuleContext(vars)
	for i := range ruleRegistry {
		if ruleRegistry[i].ID == ruleID {
			return ruleRegistry[i].Check(&ctx)
		}
	}
	t.Fatalf("rule %q not found in registry", ruleID)
	return nil
}

// assertFindings checks that the expected number of findings were returned with the expected severity.
func assertFindings(t *testing.T, findings []VariableAdvisorFinding, wantCount int, wantSeverity string) {
	t.Helper()
	if len(findings) != wantCount {
		t.Errorf("got %d findings, want %d", len(findings), wantCount)
		for _, f := range findings {
			t.Logf("  finding: %s (%s)", f.RuleID, f.Severity)
		}
		return
	}
	if wantCount > 0 && wantSeverity != "" {
		if findings[0].Severity != wantSeverity {
			t.Errorf("severity = %q, want %q", findings[0].Severity, wantSeverity)
		}
	}
}
