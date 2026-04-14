package analyze

import (
	"fmt"
	"regexp"
	"strconv"
)

// RuleCategory groups rules for filtering and display.
type RuleCategory string

const (
	CategoryInnoDB      RuleCategory = "innodb"
	CategoryReplication RuleCategory = "replication"
	CategoryConnections RuleCategory = "connections"
	CategoryMemory      RuleCategory = "memory"
	CategorySecurity    RuleCategory = "security"
	CategoryPerfSchema  RuleCategory = "performance_schema"
	CategoryGeneral     RuleCategory = "general"
)

// MySQLVersion holds parsed server version components.
type MySQLVersion struct {
	Major int
	Minor int
	Patch int
	Raw   string
}

// AtLeast returns true if the version is >= major.minor.patch.
func (v MySQLVersion) AtLeast(major, minor, patch int) bool {
	if v.Major != major {
		return v.Major > major
	}
	if v.Minor != minor {
		return v.Minor > minor
	}
	return v.Patch >= patch
}

func (v MySQLVersion) String() string {
	if v.Raw != "" {
		return v.Raw
	}
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// RuleContext provides all context a rule needs to evaluate.
type RuleContext struct {
	Vars     map[string]string // performance_schema.global_variables (lowercase keys)
	IsAurora bool
	Version  MySQLVersion
}

// RuleDefinition describes a single advisory rule.
type RuleDefinition struct {
	ID       string
	Category RuleCategory
	Tags     []string // e.g. "aurora-skip", "myisam-only"
	Check    func(ctx *RuleContext) []VariableAdvisorFinding
}

// HasTag returns true if the rule has the given tag.
func (r *RuleDefinition) HasTag(tag string) bool {
	for _, t := range r.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// versionRegexp matches MySQL version strings like "8.0.35", "5.7.44-log",
// "8.0.35-26+aurora2.10.3.0".
var versionRegexp = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)

// ParseMySQLVersion parses a MySQL version string into its components.
func ParseMySQLVersion(raw string) MySQLVersion {
	v := MySQLVersion{Raw: raw}
	m := versionRegexp.FindStringSubmatch(raw)
	if len(m) < 4 {
		return v
	}
	v.Major, _ = strconv.Atoi(m[1])
	v.Minor, _ = strconv.Atoi(m[2])
	v.Patch, _ = strconv.Atoi(m[3])
	return v
}

// DetectAurora returns true if the variable map contains aurora_version,
// indicating the server is Amazon Aurora MySQL.
func DetectAurora(vars map[string]string) bool {
	_, ok := vars["aurora_version"]
	return ok
}

// BuildRuleContext creates a RuleContext from a global variables map.
func BuildRuleContext(vars map[string]string) RuleContext {
	return RuleContext{
		Vars:     vars,
		IsAurora: DetectAurora(vars),
		Version:  ParseMySQLVersion(vars["version"]),
	}
}

// ruleRegistry holds all variable advisor rules, built at init time from category files.
var ruleRegistry = buildRegistry()

func buildRegistry() []RuleDefinition {
	var all []RuleDefinition
	all = append(all, InnoDBRules()...)
	all = append(all, ReplicationRules()...)
	all = append(all, ConnectionRules()...)
	all = append(all, MemoryRules()...)
	all = append(all, SecurityRules()...)
	all = append(all, PerfSchemaRules()...)
	all = append(all, GeneralRules()...)
	return all
}

// parseIntVar is a helper that parses a numeric variable from the map.
// Returns 0, false if the variable is missing or not a valid integer.
func parseIntVar(vars map[string]string, name string) (int64, bool) {
	v, ok := vars[name]
	if !ok || v == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
