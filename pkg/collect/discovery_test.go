package collect

import (
	"context"
	"fmt"
	"testing"
)

func TestClusterSuffix(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"my-cluster.abc123.us-east-1.rds.amazonaws.com", "abc123.us-east-1.rds.amazonaws.com"},
		{"simple.rds.amazonaws.com", "rds.amazonaws.com"},
		{"nodots", ""},
		{"trailing.", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := clusterSuffix(tt.input)
			if got != tt.want {
				t.Errorf("clusterSuffix(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestResolveHostIdentity_ConfigOverride(t *testing.T) {
	// When hostname is set in config, it takes precedence — no DB query needed.
	got := ResolveHostIdentity(context.Background(), nil, "prod-db-01", "127.0.0.1")
	if got != "prod-db-01" {
		t.Errorf("ResolveHostIdentity() = %q, want 'prod-db-01'", got)
	}
}

func TestResolveHostIdentity_Fallback(t *testing.T) {
	// When no config override and DB is nil, falls back to connection target.
	got := ResolveHostIdentity(context.Background(), nil, "", "127.0.0.1")
	if got != "127.0.0.1" {
		t.Errorf("ResolveHostIdentity() = %q, want '127.0.0.1'", got)
	}
}

func TestIsTableNotFoundError(t *testing.T) {
	tests := []struct {
		msg  string
		want bool
	}{
		{"Error 1109 (42S02): Unknown table 'replica_host_status'", true},
		{"Error 1146 (42S02): Table 'information_schema.replica_host_status' doesn't exist", true},
		{"Error 1142 (42000): SELECT command denied", false},
		{"connection refused", false},
	}

	for _, tt := range tests {
		name := tt.msg
		if len(name) > 30 {
			name = name[:30]
		}
		t.Run(name, func(t *testing.T) {
			err := fmt.Errorf("%s", tt.msg)
			if got := isTableNotFoundError(err); got != tt.want {
				t.Errorf("isTableNotFoundError(%q) = %v, want %v", tt.msg, got, tt.want)
			}
		})
	}
}
