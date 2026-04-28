package collect

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// DiscoverTopology connects to the Aurora cluster endpoint and queries
// information_schema.replica_host_status to find all instances.
// For non-Aurora MySQL (where the table doesn't exist), returns a single
// instance using the provided endpoint.
func DiscoverTopology(ctx context.Context, db *sql.DB, clusterEndpoint string) ([]DiscoveredInstance, error) {
	instances, err := queryReplicaHostStatus(ctx, db, clusterEndpoint)
	if err != nil {
		// If the table doesn't exist, fall back to single-instance mode.
		if isTableNotFoundError(err) {
			return []DiscoveredInstance{{
				ServerID: clusterEndpoint,
				Endpoint: clusterEndpoint,
				IsWriter: true,
			}}, nil
		}
		return nil, fmt.Errorf("discovering topology: %w", err)
	}

	if len(instances) == 0 {
		// Empty result — use the cluster endpoint as the only instance.
		return []DiscoveredInstance{{
			ServerID: clusterEndpoint,
			Endpoint: clusterEndpoint,
			IsWriter: true,
		}}, nil
	}

	return instances, nil
}

// queryReplicaHostStatus queries Aurora's replica_host_status table.
// The writer is identified by having SESSION_ID = 'MASTER_SESSION_ID'.
func queryReplicaHostStatus(ctx context.Context, db *sql.DB, clusterEndpoint string) ([]DiscoveredInstance, error) {
	const q = `
		SELECT
			SERVER_ID,
			IFNULL(SESSION_ID, '') AS session_id
		FROM information_schema.replica_host_status`

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	suffix := clusterSuffix(clusterEndpoint)

	var instances []DiscoveredInstance
	for rows.Next() {
		var serverID, sessionID string
		if err := rows.Scan(&serverID, &sessionID); err != nil {
			return nil, fmt.Errorf("scanning replica_host_status: %w", err)
		}

		endpoint := serverID
		if suffix != "" {
			endpoint = serverID + "." + suffix
		}

		instances = append(instances, DiscoveredInstance{
			ServerID: serverID,
			Endpoint: endpoint,
			IsWriter: sessionID == "MASTER_SESSION_ID",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating replica_host_status: %w", err)
	}

	return instances, nil
}

// ResolveHostIdentity determines a meaningful instance identifier by querying
// the remote MySQL server. This works through tunnels, proxies, and bastions
// because the query runs on the remote side.
//
// Priority:
//  1. If configHostname is set, use it (explicit override).
//  2. Try SELECT @@aurora_server_id (Aurora instances return their real name).
//  3. Fall back to SELECT @@hostname (works on all MySQL, may return an IP).
//  4. Fall back to the connection target (e.g. "127.0.0.1").
func ResolveHostIdentity(ctx context.Context, db *sql.DB, configHostname, fallback string) string {
	if configHostname != "" {
		return configHostname
	}

	if db == nil {
		return fallback
	}

	// Try Aurora-specific server ID first.
	var identity string
	if err := db.QueryRowContext(ctx, "SELECT @@aurora_server_id").Scan(&identity); err == nil && identity != "" {
		return identity
	}

	// Fall back to generic hostname.
	if err := db.QueryRowContext(ctx, "SELECT @@hostname").Scan(&identity); err == nil && identity != "" {
		return identity
	}

	return fallback
}

// clusterSuffix extracts the DNS suffix from a cluster endpoint.
// e.g., "my-cluster.abc123.us-east-1.rds.amazonaws.com" → "abc123.us-east-1.rds.amazonaws.com"
func clusterSuffix(endpoint string) string {
	idx := strings.Index(endpoint, ".")
	if idx < 0 || idx == len(endpoint)-1 {
		return ""
	}
	return endpoint[idx+1:]
}

// isTableNotFoundError checks if the error is a MySQL "table doesn't exist" error.
// Error 1109 = ER_UNKNOWN_TABLE, Error 1146 = ER_NO_SUCH_TABLE.
func isTableNotFoundError(err error) bool {
	s := err.Error()
	return strings.Contains(s, "1109") || strings.Contains(s, "1146")
}
