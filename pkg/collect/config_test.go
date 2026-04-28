package collect

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestFlushConfig_Defaults(t *testing.T) {
	yaml := `
cluster: c.rds.amazonaws.com
user: scout
password_from: env:TEST_PW
`
	t.Setenv("TEST_PW", "secret")
	cfg := loadFromString(t, yaml)

	if cfg.Output.FlushInterval != "5m" {
		t.Errorf("flush_interval = %q, want '5m'", cfg.Output.FlushInterval)
	}
	if cfg.Output.FlushSize != "128MB" {
		t.Errorf("flush_size = %q, want '128MB'", cfg.Output.FlushSize)
	}
	if cfg.Output.FlushRows == nil || *cfg.Output.FlushRows != 1000000 {
		t.Errorf("flush_rows = %v, want 1000000", cfg.Output.FlushRows)
	}
}

func TestFlushConfig_Custom(t *testing.T) {
	yaml := `
cluster: c.rds.amazonaws.com
user: scout
password_from: env:TEST_PW
output:
  flush_interval: 2m
  flush_size: 64MB
  flush_rows: 500000
`
	t.Setenv("TEST_PW", "secret")
	cfg := loadFromString(t, yaml)

	if cfg.Output.FlushInterval != "2m" {
		t.Errorf("flush_interval = %q, want '2m'", cfg.Output.FlushInterval)
	}
	if cfg.Output.FlushSize != "64MB" {
		t.Errorf("flush_size = %q, want '64MB'", cfg.Output.FlushSize)
	}
	if *cfg.Output.FlushRows != 500000 {
		t.Errorf("flush_rows = %d, want 500000", *cfg.Output.FlushRows)
	}
}

func TestFlushConfig_Disabled(t *testing.T) {
	// At least one must remain active.
	yaml := `
cluster: c.rds.amazonaws.com
user: scout
password_from: env:TEST_PW
output:
  flush_interval: -1
  flush_size: -1
  flush_rows: -1
`
	t.Setenv("TEST_PW", "secret")
	_, err := loadConfigFromBytes([]byte(yaml))
	if err == nil {
		t.Error("expected error when all flush triggers disabled")
	}
}

func TestFlushConfig_PartialDisable(t *testing.T) {
	yaml := `
cluster: c.rds.amazonaws.com
user: scout
password_from: env:TEST_PW
output:
  flush_interval: -1
  flush_size: -1
  flush_rows: 100
`
	t.Setenv("TEST_PW", "secret")
	cfg := loadFromString(t, yaml)

	fi, err := cfg.Output.ParseFlushInterval()
	if err != nil {
		t.Fatal(err)
	}
	if fi != -1 {
		t.Errorf("ParseFlushInterval() = %v, want -1", fi)
	}

	fs, err := cfg.Output.ParseFlushSize()
	if err != nil {
		t.Fatal(err)
	}
	if fs != -1 {
		t.Errorf("ParseFlushSize() = %v, want -1", fs)
	}
}

func TestParseSizeBytes(t *testing.T) {
	tests := []struct {
		input string
		want  int64
		err   bool
	}{
		{"128MB", 128 * 1024 * 1024, false},
		{"1GB", 1024 * 1024 * 1024, false},
		{"512KB", 512 * 1024, false},
		{"1024B", 1024, false},
		{"64M", 64 * 1024 * 1024, false},
		{"0.5GB", 512 * 1024 * 1024, false},
		{"", 0, true},
		{"xyz", 0, true},
		{"-5MB", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseSizeBytes(tt.input)
			if tt.err {
				if err == nil {
					t.Errorf("expected error for %q", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("ParseSizeBytes(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestLoadConfig_ReconnectDefaults(t *testing.T) {
	yaml := `
cluster: c.rds.amazonaws.com
user: scout
password_from: env:TEST_PW
`
	t.Setenv("TEST_PW", "secret")
	cfg := loadFromString(t, yaml)

	if cfg.Collect.ReconnectAfter != 3 {
		t.Errorf("reconnect_after = %d, want 3", cfg.Collect.ReconnectAfter)
	}
	if cfg.Collect.GiveUpAfter != "-1" {
		t.Errorf("give_up_after = %q, want '-1'", cfg.Collect.GiveUpAfter)
	}
}

func TestLoadConfig_ReconnectCustom(t *testing.T) {
	yaml := `
cluster: c.rds.amazonaws.com
user: scout
password_from: env:TEST_PW
collect:
  reconnect_after: 5
  give_up_after: 30m
`
	t.Setenv("TEST_PW", "secret")
	cfg := loadFromString(t, yaml)

	if cfg.Collect.ReconnectAfter != 5 {
		t.Errorf("reconnect_after = %d, want 5", cfg.Collect.ReconnectAfter)
	}

	d, err := cfg.Collect.ParseGiveUpAfter()
	if err != nil {
		t.Fatal(err)
	}
	if d != 30*time.Minute {
		t.Errorf("ParseGiveUpAfter() = %v, want 30m", d)
	}
}

func TestLoadConfig_ReconnectAfterInvalid(t *testing.T) {
	// reconnect_after: -1 means the YAML parser gives us -1, which is < 1.
	yaml := `
cluster: c.rds.amazonaws.com
user: scout
password_from: pw
collect:
  reconnect_after: -1
`
	_, err := loadConfigFromBytes([]byte(yaml))
	if err == nil {
		t.Error("expected error for reconnect_after=-1")
	}
}

func TestLoadConfig_GiveUpAfterInvalid(t *testing.T) {
	yaml := `
cluster: c.rds.amazonaws.com
user: scout
password_from: pw
collect:
  give_up_after: notaduration
`
	_, err := loadConfigFromBytes([]byte(yaml))
	if err == nil {
		t.Error("expected error for invalid give_up_after")
	}
}

func TestParseGiveUpAfter(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
		err   bool
	}{
		{"-1", -1, false},
		{"30m", 30 * time.Minute, false},
		{"1h", time.Hour, false},
		{"0s", 0, true},
		{"bogus", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			cc := &CollectConfig{GiveUpAfter: tt.input}
			got, err := cc.ParseGiveUpAfter()
			if tt.err {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("ParseGiveUpAfter() = %v, want %v", got, tt.want)
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
	defer func() { _ = os.RemoveAll(dir) }()

	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		return nil, err
	}
	return LoadConfig(path)
}
