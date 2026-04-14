package analyze

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// DuplicateKeyFinding represents an index that is a left-prefix duplicate of
// another index on the same table and can safely be dropped.
type DuplicateKeyFinding struct {
	Database       string   `json:"database"`
	Table          string   `json:"table"`
	DuplicateIndex string   `json:"duplicate_index"`
	OverlapsWith   string   `json:"overlaps_with"`
	Columns        []string `json:"columns"`
	DropStatement  string   `json:"drop_statement"`
}

// indexInfo holds the metadata for a single index on a table.
type indexInfo struct {
	name     string
	columns  []string
	isUnique bool
}

// tableKey identifies a table within a schema.
type tableKey struct {
	schema string
	table  string
}

const duplicateKeyQuery = `
SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
`

// CheckDuplicateKeys queries index metadata from information_schema and detects
// indexes whose columns are a left-prefix of another index on the same table.
// PRIMARY keys are never reported as duplicates.
func CheckDuplicateKeys(ctx context.Context, db *sql.DB) ([]DuplicateKeyFinding, error) {
	rows, err := db.QueryContext(ctx, duplicateKeyQuery)
	if err != nil {
		return nil, fmt.Errorf("querying information_schema.STATISTICS: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	// Collect columns per index, grouped by (schema, table).
	type indexBuildKey struct {
		tk   tableKey
		name string
	}
	// Preserve insertion order of tables.
	tableOrder := make([]tableKey, 0)
	tableSeen := make(map[tableKey]bool)
	// Map from (schema, table, index_name) → columns in order and uniqueness.
	indexColumns := make(map[indexBuildKey]*indexInfo)
	// Map from table → list of index names in discovery order.
	tableIndexOrder := make(map[tableKey][]string)
	indexOrderSeen := make(map[indexBuildKey]bool)

	for rows.Next() {
		var (
			schema    string
			table     string
			indexName string
			seqInIdx int
			colName   string
			nonUnique int
		)
		if err := rows.Scan(&schema, &table, &indexName, &seqInIdx, &colName, &nonUnique); err != nil {
			return nil, fmt.Errorf("scanning STATISTICS row: %w", err)
		}

		tk := tableKey{schema: schema, table: table}
		if !tableSeen[tk] {
			tableSeen[tk] = true
			tableOrder = append(tableOrder, tk)
		}

		bk := indexBuildKey{tk: tk, name: indexName}
		info, exists := indexColumns[bk]
		if !exists {
			info = &indexInfo{
				name:     indexName,
				isUnique: nonUnique == 0,
			}
			indexColumns[bk] = info
		}
		info.columns = append(info.columns, colName)

		if !indexOrderSeen[bk] {
			indexOrderSeen[bk] = true
			tableIndexOrder[tk] = append(tableIndexOrder[tk], indexName)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating STATISTICS rows: %w", err)
	}

	// For each table, compare every pair of indexes for left-prefix overlap.
	var findings []DuplicateKeyFinding

	for _, tk := range tableOrder {
		idxNames := tableIndexOrder[tk]
		if len(idxNames) < 2 {
			continue
		}

		// Build slice of indexInfo for this table.
		indexes := make([]indexInfo, 0, len(idxNames))
		for _, name := range idxNames {
			bk := indexBuildKey{tk: tk, name: name}
			indexes = append(indexes, *indexColumns[bk])
		}

		// Track which indexes have already been reported as duplicates so we
		// don't report the same index twice.
		reported := make(map[string]bool)

		// Sort indexes by column count ascending so shorter (duplicate) indexes
		// are compared first, producing deterministic results.
		sort.Slice(indexes, func(i, j int) bool {
			if len(indexes[i].columns) != len(indexes[j].columns) {
				return len(indexes[i].columns) < len(indexes[j].columns)
			}
			return indexes[i].name < indexes[j].name
		})

		for i := 0; i < len(indexes); i++ {
			a := indexes[i]

			// Never report PRIMARY as a duplicate.
			if strings.EqualFold(a.name, "PRIMARY") {
				continue
			}
			if reported[a.name] {
				continue
			}

			for j := 0; j < len(indexes); j++ {
				if i == j {
					continue
				}
				b := indexes[j]

				if isLeftPrefix(a.columns, b.columns) {
					// a is a left-prefix of b (or identical columns).
					// For identical columns: report the non-unique one as
					// duplicate, keeping the unique one. If both have the same
					// uniqueness, report a (the one sorted first).
					if columnsEqual(a.columns, b.columns) {
						if a.isUnique && !b.isUnique {
							// a is unique, b is not — b is the duplicate, not a.
							continue
						}
						// If both unique or both non-unique, or a is non-unique
						// and b is unique, a is the duplicate.
					}

					findings = append(findings, DuplicateKeyFinding{
						Database:       tk.schema,
						Table:          tk.table,
						DuplicateIndex: a.name,
						OverlapsWith:   b.name,
						Columns:        a.columns,
						DropStatement: fmt.Sprintf(
							"ALTER TABLE `%s`.`%s` DROP INDEX `%s`;",
							tk.schema, tk.table, a.name,
						),
					})
					reported[a.name] = true
					break
				}
			}
		}
	}

	return findings, nil
}

// isLeftPrefix returns true if a's columns are a left-prefix of b's columns
// (or identical). a must have len <= b for a prefix relationship. Strict
// prefix requires len(a) < len(b); identical columns are also accepted since
// that case is handled by the caller's uniqueness logic.
func isLeftPrefix(a, b []string) bool {
	if len(a) > len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

// columnsEqual returns true if a and b contain the same columns in the same
// order (case-insensitive).
func columnsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}
