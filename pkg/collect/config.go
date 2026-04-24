package collect

import (
	"fmt"
	"os"
	"strings"
	"time"

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
	Type     string `yaml:"type"`     // "local" or "s3"
	Path     string `yaml:"path"`     // local directory (when type=local)
	Bucket   string `yaml:"bucket"`   // S3 bucket URI (when type=s3), e.g. "s3://my-bucket/prefix/"
	Region   string `yaml:"region"`   // AWS region (when type=s3), empty = default
	Endpoint string `yaml:"endpoint"` // Custom S3 endpoint for MinIO/R2/etc., empty = AWS
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
