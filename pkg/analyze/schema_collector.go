package analyze

import (
	"context"
	"database/sql"
	"fmt"
)

// TableSchema holds the full schema definition for a single table,
// including columns and indexes. Published as one Kafka message per table.
type TableSchema struct {
	SchemaName string        `json:"schema_name"`
	TableName  string        `json:"table_name"`
	TableRows  int64         `json:"table_rows"`
	DataLength int64         `json:"data_length"`
	IndexLength int64        `json:"index_length"`
	Engine     string        `json:"engine"`
	Columns    []ColumnInfo  `json:"columns"`
	Indexes    []IndexInfo   `json:"indexes"`
}

// ColumnInfo describes a single column in a table.
type ColumnInfo struct {
	Name                 string  `json:"name"`
	Position             int     `json:"position"`
	DataType             string  `json:"data_type"`
	ColumnType           string  `json:"column_type"`            // full type with precision, e.g. varchar(255)
	IsNullable           bool    `json:"is_nullable"`
	ColumnKey            string  `json:"column_key"`             // PRI, UNI, MUL, or ""
	Extra                string  `json:"extra"`                  // auto_increment, etc.
	DefaultValue         *string `json:"default_value"`          // nil = no default
	CharacterSet         string  `json:"character_set"`          // "" for non-string types
	Collation            string  `json:"collation"`              // "" for non-string types
	ColumnComment        string  `json:"column_comment"`
	GenerationExpression string  `json:"generation_expression"`  // "" for non-generated columns
}

// IndexInfo describes a single index on a table.
type IndexInfo struct {
	IndexName  string   `json:"index_name"`
	NonUnique  bool     `json:"non_unique"`
	Columns    []string `json:"columns"` // ordered column list
	IndexType  string   `json:"index_type"` // BTREE, FULLTEXT, etc.
}

// CollectSchemas queries INFORMATION_SCHEMA for all user tables in the
// instance and returns one TableSchema per table. System schemas
// (information_schema, mysql, performance_schema, sys) are excluded.
func CollectSchemas(ctx context.Context, db *sql.DB) ([]TableSchema, error) {
	// 1. Get all user tables with row estimates and sizes.
	tables, err := queryTables(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}

	// 2. Get all columns for all user tables in one query.
	colsByTable, err := queryColumns(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("querying columns: %w", err)
	}

	// 3. Get all indexes for all user tables in one query.
	idxByTable, err := queryIndexes(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("querying indexes: %w", err)
	}

	// 4. Assemble.
	for i := range tables {
		key := tables[i].SchemaName + "." + tables[i].TableName
		tables[i].Columns = colsByTable[key]
		tables[i].Indexes = idxByTable[key]
	}

	return tables, nil
}

func queryTables(ctx context.Context, db *sql.DB) ([]TableSchema, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT TABLE_SCHEMA, TABLE_NAME,
		       IFNULL(TABLE_ROWS, 0),
		       IFNULL(DATA_LENGTH, 0),
		       IFNULL(INDEX_LENGTH, 0),
		       IFNULL(ENGINE, '')
		FROM information_schema.TABLES
		WHERE TABLE_TYPE = 'BASE TABLE'
		  AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		ORDER BY TABLE_SCHEMA, TABLE_NAME
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var tables []TableSchema
	for rows.Next() {
		var t TableSchema
		if err := rows.Scan(&t.SchemaName, &t.TableName, &t.TableRows,
			&t.DataLength, &t.IndexLength, &t.Engine); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func queryColumns(ctx context.Context, db *sql.DB) (map[string][]ColumnInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT TABLE_SCHEMA, TABLE_NAME,
		       COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE,
		       IS_NULLABLE, IFNULL(COLUMN_KEY, ''), IFNULL(EXTRA, ''),
		       COLUMN_DEFAULT,
		       IFNULL(CHARACTER_SET_NAME, ''),
		       IFNULL(COLLATION_NAME, ''),
		       IFNULL(COLUMN_COMMENT, ''),
		       IFNULL(GENERATION_EXPRESSION, '')
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	result := make(map[string][]ColumnInfo)
	for rows.Next() {
		var schema, table, nullable string
		var defaultVal sql.NullString
		var c ColumnInfo
		if err := rows.Scan(&schema, &table, &c.Name, &c.Position,
			&c.DataType, &c.ColumnType, &nullable, &c.ColumnKey, &c.Extra,
			&defaultVal, &c.CharacterSet, &c.Collation,
			&c.ColumnComment, &c.GenerationExpression); err != nil {
			return nil, err
		}
		c.IsNullable = nullable == "YES"
		if defaultVal.Valid {
			c.DefaultValue = &defaultVal.String
		}
		key := schema + "." + table
		result[key] = append(result[key], c)
	}
	return result, rows.Err()
}

func queryIndexes(ctx context.Context, db *sql.DB) (map[string][]IndexInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT TABLE_SCHEMA, TABLE_NAME,
		       INDEX_NAME, NON_UNIQUE, COLUMN_NAME, SEQ_IN_INDEX, INDEX_TYPE
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	// Collect columns per index, then assemble.
	type indexKey struct {
		schema, table, indexName string
	}
	type indexMeta struct {
		nonUnique int
		indexType string
		columns   []string
	}

	ordered := make([]indexKey, 0)
	seen := make(map[indexKey]*indexMeta)

	for rows.Next() {
		var schema, table, indexName, colName, indexType string
		var nonUnique, seqInIndex int
		if err := rows.Scan(&schema, &table, &indexName, &nonUnique,
			&colName, &seqInIndex, &indexType); err != nil {
			return nil, err
		}

		ik := indexKey{schema, table, indexName}
		m, ok := seen[ik]
		if !ok {
			m = &indexMeta{nonUnique: nonUnique, indexType: indexType}
			seen[ik] = m
			ordered = append(ordered, ik)
		}
		m.columns = append(m.columns, colName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make(map[string][]IndexInfo)
	for _, ik := range ordered {
		m := seen[ik]
		key := ik.schema + "." + ik.table
		result[key] = append(result[key], IndexInfo{
			IndexName: ik.indexName,
			NonUnique: m.nonUnique != 0,
			Columns:   m.columns,
			IndexType: m.indexType,
		})
	}
	return result, nil
}
