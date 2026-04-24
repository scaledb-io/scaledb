package collect

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_MinimalAurora(t *testing.T) {
	yaml := `
cluster: my-cluster.abc.us-east-1.rds.amazonaws.com
user: scout
password_from: env:TEST_PW
`
	t.Setenv("TEST_PW", "secret")
	cfg := loadFromString(t, yaml)

	if cfg.Cluster != "my-cluster.abc.us-east-1.rds.amazonaws.com" {
		t.Errorf("cluster = %q, want Aurora endpoint", cfg.Cluster)
	}
	if !cfg.IsAurora() {
		t.Error("expected IsAurora() = true")
	}
	if cfg.Output.Type != "local" {
		t.Errorf("output.type = %q, want 'local' (default)", cfg.Output.Type)
	}
	if cfg.Output.Path != "./scaledb-data/" {
		t.Errorf("output.path = %q, want default", cfg.Output.Path)
	}
	if cfg.Collect.Interval != "60s" {
		t.Errorf("collect.interval = %q, want '60s' (default)", cfg.Collect.Interval)
	}
}

func TestLoadConfig_SingleHost(t *testing.T) {
	yaml := `
host: localhost
user: root
password_from: rootpw
`
	cfg := loadFromString(t, yaml)

	if cfg.IsAurora() {
		t.Error("expected IsAurora() = false for host config")
	}
	if cfg.Endpoint() != "localhost:3306" {
		t.Errorf("Endpoint() = %q, want localhost:3306", cfg.Endpoint())
	}
}

func TestLoadConfig_FullConfig(t *testing.T) {
	yaml := `
cluster: prod.abc.us-east-1.rds.amazonaws.com
user: scout
password_from: env:SCOUT_PW
port: 3307
output:
  type: local
  path: /data/scaledb/
collect:
  interval: 30s
  schemas: true
  query_samples: true
daemon:
  pidfile: /tmp/scaledb.pid
  logfile: /tmp/scaledb.log
`
	t.Setenv("SCOUT_PW", "secret")
	cfg := loadFromString(t, yaml)

	if cfg.Port != 3307 {
		t.Errorf("port = %d, want 3307", cfg.Port)
	}
	if cfg.Output.Path != "/data/scaledb/" {
		t.Errorf("output.path = %q", cfg.Output.Path)
	}
	if !cfg.Collect.Schemas {
		t.Error("expected schemas = true")
	}
	if !cfg.Collect.QuerySamples {
		t.Error("expected query_samples = true")
	}
	if cfg.Daemon.PIDFile != "/tmp/scaledb.pid" {
		t.Errorf("daemon.pidfile = %q", cfg.Daemon.PIDFile)
	}
}

func TestLoadConfig_MissingRequired(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{"no cluster or host", "user: root\npassword_from: pw\n"},
		{"no user", "cluster: c.rds.amazonaws.com\npassword_from: pw\n"},
		{"no password", "cluster: c.rds.amazonaws.com\nuser: root\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfigFromBytes([]byte(tt.yaml))
			if err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestLoadConfig_InvalidInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval string
	}{
		{"too short", "5s"},
		{"too long", "25h"},
		{"not a duration", "fast"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := "cluster: c.rds.amazonaws.com\nuser: root\npassword_from: pw\ncollect:\n  interval: " + tt.interval + "\n"
			_, err := loadConfigFromBytes([]byte(content))
			if err == nil {
				t.Errorf("expected error for interval %q", tt.interval)
			}
		})
	}
}

func TestLoadConfig_UnsupportedOutputType(t *testing.T) {
	yaml := `
cluster: c.rds.amazonaws.com
user: root
password_from: pw
output:
  type: s3
  path: s3://bucket/
`
	_, err := loadConfigFromBytes([]byte(yaml))
	if err == nil {
		t.Error("expected error for output type 's3'")
	}
}

func TestResolvePassword_Env(t *testing.T) {
	cfg := &Config{PasswordFrom: "env:MY_SECRET"}
	t.Setenv("MY_SECRET", "hunter2")

	pw, err := cfg.ResolvePassword()
	if err != nil {
		t.Fatal(err)
	}
	if pw != "hunter2" {
		t.Errorf("password = %q, want 'hunter2'", pw)
	}
}

func TestResolvePassword_EnvMissing(t *testing.T) {
	cfg := &Config{PasswordFrom: "env:NONEXISTENT_VAR_12345"}
	_, err := cfg.ResolvePassword()
	if err == nil {
		t.Error("expected error for missing env var")
	}
}

func TestResolvePassword_Literal(t *testing.T) {
	cfg := &Config{PasswordFrom: "plaintext-secret"}
	pw, err := cfg.ResolvePassword()
	if err != nil {
		t.Fatal(err)
	}
	if pw != "plaintext-secret" {
		t.Errorf("password = %q, want 'plaintext-secret'", pw)
	}
}

func TestParseInterval(t *testing.T) {
	tests := []struct {
		input string
		want  string
		err   bool
	}{
		{"10s", "10s", false},
		{"60s", "1m0s", false},
		{"5m", "5m0s", false},
		{"24h", "24h0m0s", false},
		{"9s", "", true},
		{"25h", "", true},
		{"bogus", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			cfg := &Config{Collect: CollectConfig{Interval: tt.input}}
			d, err := cfg.ParseInterval()
			if tt.err {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if d.String() != tt.want {
				t.Errorf("duration = %s, want %s", d, tt.want)
			}
		})
	}
}

// helpers

func loadFromString(t *testing.T, content string) *Config {
	t.Helper()
	cfg, err := loadConfigFromBytes([]byte(content))
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	return cfg
}

func loadConfigFromBytes(data []byte) (*Config, error) {
	dir, err := os.MkdirTemp("", "scaledb-test-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		return nil, err
	}
	return LoadConfig(path)
}
