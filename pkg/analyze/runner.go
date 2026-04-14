package analyze

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// Envelope is the Kafka message wrapper matching the ClickHouse pt_source schema.
// Every analysis result is wrapped in this envelope before publishing to Redpanda.
type Envelope struct {
	RunID       string `json:"run_id"`
	InstanceID  string `json:"instance_id"`
	ClusterID   string `json:"cluster_id"`
	Tool        string `json:"tool"`
	ToolVersion string `json:"tool_version"`
	CollectedAt string `json:"collected_at"`
	JSONPayload string `json:"json_payload"`
}

const toolVersion = "scaledb-1.0"

// RunAll executes all analysis tools against a single MySQL instance and returns
// the results as Kafka-ready envelopes. Tools that fail are logged and skipped
// — partial results are returned rather than aborting the entire run.
func RunAll(ctx context.Context, db *sql.DB, instanceID, clusterID string) ([]Envelope, error) {
	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	var envelopes []Envelope

	// --- pt-variable-advisor ---
	varResult, err := CheckVariables(ctx, db, CheckVariablesOptions{})
	if err != nil {
		slog.Error("variable advisor failed", "instance_id", instanceID, "error", err)
	} else {
		payload, err := marshalPayload(map[string]any{"findings": varResult.Findings})
		if err != nil {
			return nil, fmt.Errorf("marshaling variable advisor payload: %w", err)
		}
		envelopes = append(envelopes, Envelope{
			RunID:       newUUID(),
			InstanceID:  instanceID,
			ClusterID:   clusterID,
			Tool:        "pt-variable-advisor",
			ToolVersion: toolVersion,
			CollectedAt: now,
			JSONPayload: payload,
		})
	}

	// --- pt-duplicate-key-checker ---
	dups, err := CheckDuplicateKeys(ctx, db)
	if err != nil {
		slog.Error("duplicate key checker failed", "instance_id", instanceID, "error", err)
	} else {
		payload, err := marshalPayload(map[string]any{"findings": dups})
		if err != nil {
			return nil, fmt.Errorf("marshaling duplicate key checker payload: %w", err)
		}
		envelopes = append(envelopes, Envelope{
			RunID:       newUUID(),
			InstanceID:  instanceID,
			ClusterID:   clusterID,
			Tool:        "pt-duplicate-key-checker",
			ToolVersion: toolVersion,
			CollectedAt: now,
			JSONPayload: payload,
		})
	}

	// --- pt-mysql-summary ---
	summary, err := CollectSummary(ctx, db)
	if err != nil {
		slog.Error("mysql summary failed", "instance_id", instanceID, "error", err)
	} else {
		payload, err := marshalPayload(summary)
		if err != nil {
			return nil, fmt.Errorf("marshaling mysql summary payload: %w", err)
		}
		envelopes = append(envelopes, Envelope{
			RunID:       newUUID(),
			InstanceID:  instanceID,
			ClusterID:   clusterID,
			Tool:        "pt-mysql-summary",
			ToolVersion: toolVersion,
			CollectedAt: now,
			JSONPayload: payload,
		})
	}

	// --- table-schema (one envelope per table) ---
	schemas, err := CollectSchemas(ctx, db)
	if err != nil {
		slog.Error("schema collector failed", "instance_id", instanceID, "error", err)
	} else {
		runID := newUUID()
		for i := range schemas {
			payload, err := marshalPayload(schemas[i])
			if err != nil {
				slog.Error("marshaling table schema", "table", schemas[i].TableName, "error", err)
				continue
			}
			envelopes = append(envelopes, Envelope{
				RunID:       runID,
				InstanceID:  instanceID,
				ClusterID:   clusterID,
				Tool:        "table-schema",
				ToolVersion: toolVersion,
				CollectedAt: now,
				JSONPayload: payload,
			})
		}
	}

	if len(envelopes) == 0 {
		return nil, fmt.Errorf("all analysis tools failed for instance %s", instanceID)
	}

	return envelopes, nil
}

// TopicForTool maps an analysis tool name to its Redpanda topic.
func TopicForTool(tool string) string {
	switch tool {
	case "pt-variable-advisor":
		return "scout.pt-variable-advisor"
	case "pt-duplicate-key-checker":
		return "scout.pt-schema-checks"
	case "pt-mysql-summary":
		return "scout.pt-mysql-summary"
	case "table-schema":
		return "scout.table-schemas"
	case "schema-drift":
		return "scout.schema-drift"
	default:
		return ""
	}
}

// marshalPayload JSON-encodes a value and returns it as a string.
func marshalPayload(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// newUUID generates a random UUID v4 without external dependencies.
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
