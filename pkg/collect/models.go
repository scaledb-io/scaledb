package collect

import "github.com/scaledb-io/scaledb/pkg/model"

// Re-export model types for convenience within the collect package.
type (
	Metric           = model.Metric
	QueryDigest      = model.QueryDigest
	IndexUsage       = model.IndexUsage
	QuerySample      = model.QuerySample
	WaitEventSummary = model.WaitEventSummary
)

// DiscoveredInstance represents an Aurora instance found via topology discovery.
type DiscoveredInstance struct {
	ServerID string
	Endpoint string
	IsWriter bool
}

// PollResult aggregates the output from a single poll cycle for one instance.
type PollResult struct {
	Metrics    []Metric
	Digests    []QueryDigest
	IndexUsage []IndexUsage
	Samples    []QuerySample
	WaitEvents []WaitEventSummary
}
