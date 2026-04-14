package analyze

import (
	"testing"
)

func TestParseMySQLVersion(t *testing.T) {
	tests := []struct {
		raw          string
		wantMajor    int
		wantMinor    int
		wantPatch    int
		wantAtLeast8 bool
	}{
		{"8.0.35", 8, 0, 35, true},
		{"8.0.35-26+aurora2.10.3.0", 8, 0, 35, true},
		{"5.7.44-log", 5, 7, 44, false},
		{"8.4.0", 8, 4, 0, true},
		{"", 0, 0, 0, false},
		{"garbage", 0, 0, 0, false},
		{"8.0.30-commercial", 8, 0, 30, true},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			v := ParseMySQLVersion(tc.raw)
			if v.Major != tc.wantMajor || v.Minor != tc.wantMinor || v.Patch != tc.wantPatch {
				t.Errorf("ParseMySQLVersion(%q) = %d.%d.%d, want %d.%d.%d",
					tc.raw, v.Major, v.Minor, v.Patch, tc.wantMajor, tc.wantMinor, tc.wantPatch)
			}
			if v.Raw != tc.raw {
				t.Errorf("Raw = %q, want %q", v.Raw, tc.raw)
			}
			got := v.AtLeast(8, 0, 0)
			if got != tc.wantAtLeast8 {
				t.Errorf("AtLeast(8,0,0) = %v, want %v", got, tc.wantAtLeast8)
			}
		})
	}
}

func TestMySQLVersionAtLeast(t *testing.T) {
	tests := []struct {
		version string
		major   int
		minor   int
		patch   int
		want    bool
	}{
		{"8.0.35", 8, 0, 35, true},  // exact match
		{"8.0.35", 8, 0, 30, true},  // higher patch
		{"8.0.35", 8, 0, 40, false}, // lower patch
		{"8.0.35", 8, 1, 0, false},  // lower minor
		{"8.4.0", 8, 0, 35, true},   // higher minor
		{"8.0.35", 9, 0, 0, false},  // lower major
		{"9.0.0", 8, 0, 35, true},   // higher major
		{"8.0.30", 8, 0, 30, true},  // exact match
	}
	for _, tc := range tests {
		t.Run(tc.version, func(t *testing.T) {
			v := ParseMySQLVersion(tc.version)
			got := v.AtLeast(tc.major, tc.minor, tc.patch)
			if got != tc.want {
				t.Errorf("%s.AtLeast(%d,%d,%d) = %v, want %v",
					tc.version, tc.major, tc.minor, tc.patch, got, tc.want)
			}
		})
	}
}

func TestDetectAurora(t *testing.T) {
	tests := []struct {
		name string
		vars map[string]string
		want bool
	}{
		{
			name: "aurora present",
			vars: map[string]string{"aurora_version": "2.10.3.0", "version": "8.0.35"},
			want: true,
		},
		{
			name: "aurora absent",
			vars: map[string]string{"version": "8.0.35"},
			want: false,
		},
		{
			name: "empty aurora value still detected",
			vars: map[string]string{"aurora_version": "", "version": "8.0.35"},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DetectAurora(tc.vars)
			if got != tc.want {
				t.Errorf("DetectAurora() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildRuleContext(t *testing.T) {
	vars := map[string]string{
		"version":        "8.0.35-26+aurora2.10.3.0",
		"aurora_version": "2.10.3.0",
		"max_connections": "500",
	}
	ctx := BuildRuleContext(vars)

	if !ctx.IsAurora {
		t.Error("expected IsAurora=true")
	}
	if ctx.Version.Major != 8 || ctx.Version.Minor != 0 || ctx.Version.Patch != 35 {
		t.Errorf("version = %d.%d.%d, want 8.0.35", ctx.Version.Major, ctx.Version.Minor, ctx.Version.Patch)
	}
	if ctx.Vars["max_connections"] != "500" {
		t.Error("Vars not passed through correctly")
	}
}

func TestParseIntVar(t *testing.T) {
	vars := map[string]string{
		"max_connections":          "500",
		"innodb_buffer_pool_size":  "134217728",
		"innodb_flush_method":      "O_DIRECT",
		"empty":                    "",
	}

	tests := []struct {
		name    string
		varName string
		wantVal int64
		wantOK  bool
	}{
		{"valid int", "max_connections", 500, true},
		{"large int", "innodb_buffer_pool_size", 134217728, true},
		{"non-numeric", "innodb_flush_method", 0, false},
		{"empty value", "empty", 0, false},
		{"missing key", "nonexistent", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, ok := parseIntVar(vars, tc.varName)
			if ok != tc.wantOK {
				t.Errorf("parseIntVar(%q) ok = %v, want %v", tc.varName, ok, tc.wantOK)
			}
			if val != tc.wantVal {
				t.Errorf("parseIntVar(%q) = %d, want %d", tc.varName, val, tc.wantVal)
			}
		})
	}
}

func TestRuleDefinitionHasTag(t *testing.T) {
	r := RuleDefinition{
		ID:   "test",
		Tags: []string{"aurora-skip", "myisam-only"},
	}
	if !r.HasTag("aurora-skip") {
		t.Error("expected HasTag(aurora-skip) = true")
	}
	if !r.HasTag("myisam-only") {
		t.Error("expected HasTag(myisam-only) = true")
	}
	if r.HasTag("nonexistent") {
		t.Error("expected HasTag(nonexistent) = false")
	}
}

func TestRuleRegistryCount(t *testing.T) {
	if len(ruleRegistry) < 70 {
		t.Errorf("expected at least 70 rules in registry, got %d", len(ruleRegistry))
	}
	t.Logf("total rules in registry: %d", len(ruleRegistry))
}
