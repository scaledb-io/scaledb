package collect

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
)

// Config holds the YAML configuration for scaledb collect.
type Config struct {
	Cluster      string        `yaml:"cluster"`
	Host         string        `yaml:"host"`          // fallback for non-Aurora single instance
	Port         int           `yaml:"port"`
	User         string        `yaml:"user"`
	PasswordFrom string        `yaml:"password_from"` // "env:VAR_NAME" or literal string
	Output       OutputConfig  `yaml:"output"`
	Collect      CollectConfig `yaml:"collect"`
	Daemon       DaemonConfig  `yaml:"daemon"`
}

// OutputConfig defines where collected data is written.
type OutputConfig struct {
	Type          string `yaml:"type"`           // "local" or "s3"
	Path          string `yaml:"path"`           // local directory (when type=local)
	Bucket        string `yaml:"bucket"`         // S3 bucket URI (when type=s3), e.g. "s3://my-bucket/prefix/"
	Region        string `yaml:"region"`         // AWS region (when type=s3), empty = default
	Endpoint      string `yaml:"endpoint"`       // Custom S3 endpoint for MinIO/R2/etc., empty = AWS
	FlushInterval string `yaml:"flush_interval"` // time trigger: "5m", "-1" = disabled
	FlushSize     string `yaml:"flush_size"`     // size trigger: "128MB", "-1" = disabled
	FlushRows     *int   `yaml:"flush_rows"`     // row trigger: 1000000, -1 = disabled; nil = use default
}

// CollectConfig controls what data is collected and at what interval.
type CollectConfig struct {
	Interval     string `yaml:"interval"`
	Schemas      bool   `yaml:"schemas"`
	QuerySamples bool   `yaml:"query_samples"`
}

// DaemonConfig controls daemon mode behavior.
type DaemonConfig struct {
	PIDFile string `yaml:"pidfile"`
	LogFile string `yaml:"logfile"`
}

// LoadConfig reads and parses a YAML configuration file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %s: %w", path, err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}

	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, nil
}

const (
	defaultFlushInterval = "5m"
	defaultFlushSize     = "128MB"
	defaultFlushRows     = 1000000
)

func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = 3306
	}
	if c.Output.Type == "" {
		c.Output.Type = "local"
	}
	if c.Output.Path == "" {
		c.Output.Path = "./scaledb-data/"
	}
	if c.Output.FlushInterval == "" {
		c.Output.FlushInterval = defaultFlushInterval
	}
	if c.Output.FlushSize == "" {
		c.Output.FlushSize = defaultFlushSize
	}
	if c.Output.FlushRows == nil {
		rows := defaultFlushRows
		c.Output.FlushRows = &rows
	}
	if c.Collect.Interval == "" {
		c.Collect.Interval = "60s"
	}
	if c.Daemon.PIDFile == "" {
		c.Daemon.PIDFile = "/var/run/scaledb.pid"
	}
	if c.Daemon.LogFile == "" {
		c.Daemon.LogFile = "/var/log/scaledb/collect.log"
	}
}

// Validate checks that all required fields are present and valid.
func (c *Config) Validate() error {
	if c.Cluster == "" && c.Host == "" {
		return fmt.Errorf("either 'cluster' (Aurora) or 'host' (single instance) is required")
	}
	if c.User == "" {
		return fmt.Errorf("'user' is required")
	}
	if c.PasswordFrom == "" {
		return fmt.Errorf("'password_from' is required")
	}
	switch c.Output.Type {
	case "local":
		// ok
	case "s3":
		if c.Output.Bucket == "" {
			return fmt.Errorf("output.bucket is required when type is 's3'")
		}
	default:
		return fmt.Errorf("output type %q is not supported (must be 'local' or 's3')", c.Output.Type)
	}
	if _, err := c.ParseInterval(); err != nil {
		return fmt.Errorf("invalid collect interval %q: %w", c.Collect.Interval, err)
	}
	if err := c.Output.ValidateFlush(); err != nil {
		return err
	}
	return nil
}

// ResolvePassword reads the password from the configured source.
// Supports "env:VAR_NAME" to read from an environment variable,
// or returns the literal string value.
func (c *Config) ResolvePassword() (string, error) {
	if strings.HasPrefix(c.PasswordFrom, "env:") {
		varName := strings.TrimPrefix(c.PasswordFrom, "env:")
		val := os.Getenv(varName)
		if val == "" {
			return "", fmt.Errorf("environment variable %q is not set (from password_from: %q)", varName, c.PasswordFrom)
		}
		return val, nil
	}
	return c.PasswordFrom, nil
}

// ParseInterval parses the collect interval string into a time.Duration.
// Enforces minimum 10s and maximum 24h.
func (c *Config) ParseInterval() (time.Duration, error) {
	d, err := time.ParseDuration(c.Collect.Interval)
	if err != nil {
		return 0, err
	}
	if d < 10*time.Second {
		return 0, fmt.Errorf("interval must be at least 10s, got %s", d)
	}
	if d > 24*time.Hour {
		return 0, fmt.Errorf("interval must be at most 24h, got %s", d)
	}
	return d, nil
}

// Endpoint returns the connection target. For Aurora clusters, returns the
// cluster endpoint. For single instances, returns host:port.
func (c *Config) Endpoint() string {
	if c.Cluster != "" {
		return c.Cluster
	}
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// IsAurora returns true if the config specifies an Aurora cluster endpoint.
func (c *Config) IsAurora() bool {
	return c.Cluster != ""
}

// ValidateFlush checks that flush config is valid and at least one trigger is active.
func (o *OutputConfig) ValidateFlush() error {
	fi, err := o.ParseFlushInterval()
	if err != nil {
		return fmt.Errorf("invalid flush_interval %q: %w", o.FlushInterval, err)
	}
	fs, err := o.ParseFlushSize()
	if err != nil {
		return fmt.Errorf("invalid flush_size %q: %w", o.FlushSize, err)
	}
	fr := *o.FlushRows
	if fr < -1 {
		return fmt.Errorf("flush_rows must be -1 (disabled) or a positive integer, got %d", fr)
	}

	intervalDisabled := fi < 0
	sizeDisabled := fs < 0
	rowsDisabled := fr < 0

	if intervalDisabled && sizeDisabled && rowsDisabled {
		return fmt.Errorf("all flush triggers are disabled; at least one of flush_interval, flush_size, or flush_rows must be active")
	}
	return nil
}

// ParseFlushInterval returns the flush interval duration, or -1 if disabled.
func (o *OutputConfig) ParseFlushInterval() (time.Duration, error) {
	if o.FlushInterval == "-1" {
		return -1, nil
	}
	d, err := time.ParseDuration(o.FlushInterval)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("flush_interval must be positive, got %s", d)
	}
	return d, nil
}

// ParseFlushSize parses a human-readable size string (e.g. "128MB") into bytes.
// Returns -1 if disabled ("-1").
func (o *OutputConfig) ParseFlushSize() (int64, error) {
	if o.FlushSize == "-1" {
		return -1, nil
	}
	return ParseSizeBytes(o.FlushSize)
}

// ParseSizeBytes parses a human-readable size string like "128MB", "1GB", "512KB" into bytes.
func ParseSizeBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// Split into numeric and suffix parts.
	i := 0
	for i < len(s) && (unicode.IsDigit(rune(s[i])) || s[i] == '.') {
		i++
	}
	numStr := s[:i]
	suffix := strings.ToUpper(strings.TrimSpace(s[i:]))

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing size %q: %w", s, err)
	}
	if num <= 0 {
		return 0, fmt.Errorf("size must be positive, got %s", s)
	}

	var multiplier float64
	switch suffix {
	case "B", "":
		multiplier = 1
	case "KB", "K":
		multiplier = 1024
	case "MB", "M":
		multiplier = 1024 * 1024
	case "GB", "G":
		multiplier = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size suffix %q in %q (use B, KB, MB, or GB)", suffix, s)
	}

	return int64(num * multiplier), nil
}

// FormatSizeBytes formats bytes into a human-readable string.
func FormatSizeBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1fGB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1fKB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
