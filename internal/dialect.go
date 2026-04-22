package internal

import (
	"strings"

	"github.com/vantaboard/go-googlesql/types"
)

// Dialect selects SQL generation and catalog persistence behavior for a physical engine.
// The surface is intentionally small; extend with DDL helpers, placeholder style, etc. as needed.
type Dialect interface {
	// ID returns a stable backend identifier ("sqlite", "duckdb", ...).
	ID() string
	// WindowPartitionCollation is used for PARTITION BY / ORDER BY sort keys where GoogleSQL
	// relies on custom collation (SQLite: googlesqlite_collate). Empty means omit COLLATE.
	WindowPartitionCollation() string
	// RewriteEmittedFunctionName rewrites a resolved function name before SQL emission.
	// Returns (newName, true) when changed; otherwise (name, false).
	RewriteEmittedFunctionName(name string) (string, bool)
	// ArraySubqueryListAggregate is the aggregate applied to the inner query column for array subqueries.
	// SQLite uses googlesqlite_array; DuckDB uses list().
	ArraySubqueryListAggregate() string
	// WrapGroupByKey wraps GROUP BY expressions (SQLite uses googlesqlite_group_by for complex keys).
	WrapGroupByKey(expr *SQLExpression) *SQLExpression
	// MaybeEmitNativeCast returns a backend-native cast, or (nil, nil) to fall back to googlesqlite_cast.
	MaybeEmitNativeCast(inner *SQLExpression, cast *CastData) (*SQLExpression, error)
	// ArrayUnnestUseLateralCorrelation is true when correlated UNNEST should use JOIN LATERAL (DuckDB).
	ArrayUnnestUseLateralCorrelation() bool
	// MergeTempTableName is the scratch table used by the multi-statement MERGE simulation.
	MergeTempTableName() string
	// MergeScratchTableIsTemporary uses CREATE TEMP TABLE for that scratch table (recommended on DuckDB).
	MergeScratchTableIsTemporary() bool

	// PhysicalPrimaryKeyColumnListEntry formats one column reference inside PRIMARY KEY (...).
	PhysicalPrimaryKeyColumnListEntry(columnName string) string
	// PhysicalUseWithoutRowID is true when CREATE TABLE should append WITHOUT ROWID (SQLite PK tables).
	PhysicalUseWithoutRowID() bool
	// PhysicalColumnStorageType maps a GoogleSQL type kind to a CREATE TABLE column type name.
	PhysicalColumnStorageType(kind types.TypeKind) string
	// QuoteIdent returns a delimited SQL identifier for this engine (SQLite: backticks, DuckDB: double quotes).
	QuoteIdent(ident string) string
}

// ApplySortCollation sets expr.Collation when the dialect uses a sort collation (SQLite only today).
func ApplySortCollation(d Dialect, expr *SQLExpression) {
	if d == nil || expr == nil {
		return
	}
	if c := d.WindowPartitionCollation(); c != "" {
		expr.Collation = c
	}
}

// SQLiteDialect is the default GoogleSQL-to-SQLite codegen target.
type SQLiteDialect struct{}

func (SQLiteDialect) ID() string { return "sqlite" }

func (SQLiteDialect) WindowPartitionCollation() string { return "googlesqlite_collate" }

func (SQLiteDialect) RewriteEmittedFunctionName(name string) (string, bool) {
	return name, false
}

func (SQLiteDialect) ArraySubqueryListAggregate() string { return "googlesqlite_array" }

func (SQLiteDialect) WrapGroupByKey(expr *SQLExpression) *SQLExpression {
	return NewFunctionExpression("googlesqlite_group_by", expr)
}

func (SQLiteDialect) MaybeEmitNativeCast(_ *SQLExpression, _ *CastData) (*SQLExpression, error) {
	return nil, nil
}

func (SQLiteDialect) ArrayUnnestUseLateralCorrelation() bool { return false }

func (SQLiteDialect) MergeTempTableName() string { return "googlesqlite_merged_table" }

func (SQLiteDialect) MergeScratchTableIsTemporary() bool { return false }

func (d SQLiteDialect) PhysicalPrimaryKeyColumnListEntry(columnName string) string {
	return d.QuoteIdent(columnName) + " COLLATE googlesqlite_collate"
}

func (SQLiteDialect) QuoteIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

func (SQLiteDialect) PhysicalUseWithoutRowID() bool { return true }

func (SQLiteDialect) PhysicalColumnStorageType(kind types.TypeKind) string {
	switch kind {
	case types.INT32, types.INT64, types.UINT32, types.UINT64, types.ENUM:
		return "INT"
	case types.BOOL:
		return "BOOLEAN"
	case types.FLOAT:
		return "FLOAT"
	case types.BYTES:
		return "BLOB"
	case types.DOUBLE:
		return "DOUBLE"
	case types.JSON:
		return "JSON"
	case types.STRING:
		return "TEXT"
	case types.DATE, types.TIMESTAMP, types.ARRAY, types.STRUCT, types.PROTO, types.TIME,
		types.DATETIME, types.GEOGRAPHY, types.NUMERIC, types.BIG_NUMERIC, types.EXTENDED, types.INTERVAL:
		return "TEXT"
	default:
		return "UNKNOWN"
	}
}

// duckDBNativeFunctions maps googlesqlite-prefixed runtime names to DuckDB builtins where semantics align.
var duckDBNativeFunctions = map[string]string{
	"googlesqlite_length":      "length",
	"googlesqlite_char_length": "length",
	"googlesqlite_abs":         "abs",
	"googlesqlite_lower":       "lower",
	"googlesqlite_upper":       "upper",
	"googlesqlite_substr":      "substr",
	// String family (Phase 2 batch): DuckDB builtins with compatible arity for common cases.
	"googlesqlite_trim":    "trim",
	"googlesqlite_ltrim":   "ltrim",
	"googlesqlite_rtrim":   "rtrim",
	"googlesqlite_concat":  "concat",
	"googlesqlite_replace": "replace",
	"googlesqlite_reverse": "reverse",
	"googlesqlite_repeat":  "repeat",
	// INSTR with 3+ args has no single DuckDB builtin; keep SQLite UDF path until rewritten.
	"googlesqlite_strpos": "strpos",
	"googlesqlite_chr":    "chr",
	"googlesqlite_ascii":  "ascii",
	// String family (Phase 2 batch 2)
	"googlesqlite_starts_with": "starts_with",
	"googlesqlite_ends_with":   "ends_with",
	"googlesqlite_left":        "left",
	"googlesqlite_right":       "right",
	"googlesqlite_lpad":        "lpad",
	"googlesqlite_rpad":        "rpad",
	"googlesqlite_initcap":     "initcap",
	"googlesqlite_unicode":     "unicode",
	// Hash (often used near JSON pipelines)
	"googlesqlite_md5":    "md5",
	"googlesqlite_sha1":   "sha1",
	"googlesqlite_sha256": "sha256",
	"googlesqlite_sha512": "sha512",
	// JSON — path/json semantics still differ for some workloads; expand with tests as needed.
	"googlesqlite_json_extract": "json_extract",
	"googlesqlite_json_value":   "json_extract",
	// Variadic SQL builtin (SQLite registers googlesqlite_coalesce UDF; DuckDB has native coalesce)
	"googlesqlite_coalesce": "coalesce",
	// DuckDB utility: throws with message (matches googlesqlite_error / BigQuery ERROR)
	"googlesqlite_error": "error",
}

// DuckDBDialect is the GoogleSQL-to-DuckDB codegen target (incremental parity).
type DuckDBDialect struct{}

func (DuckDBDialect) ID() string { return "duckdb" }

func (DuckDBDialect) WindowPartitionCollation() string { return "" }

func (DuckDBDialect) RewriteEmittedFunctionName(name string) (string, bool) {
	if alt, ok := duckDBNativeFunctions[name]; ok {
		return alt, true
	}
	if alt, ok := duckDBWindowRenames[name]; ok {
		return alt, true
	}
	if alt, ok := duckDBAggregateRenames[name]; ok {
		return alt, true
	}
	return name, false
}

func (DuckDBDialect) ArraySubqueryListAggregate() string { return "list" }

func (DuckDBDialect) WrapGroupByKey(expr *SQLExpression) *SQLExpression { return expr }

func (DuckDBDialect) ArrayUnnestUseLateralCorrelation() bool { return true }

func (DuckDBDialect) MergeTempTableName() string { return "googlesqlite_merged_table" }

func (DuckDBDialect) MergeScratchTableIsTemporary() bool { return true }

func (d DuckDBDialect) PhysicalPrimaryKeyColumnListEntry(columnName string) string {
	return d.QuoteIdent(columnName)
}

func (DuckDBDialect) QuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func (DuckDBDialect) PhysicalUseWithoutRowID() bool { return false }

func (DuckDBDialect) PhysicalColumnStorageType(kind types.TypeKind) string {
	switch kind {
	case types.INT32, types.INT64, types.UINT32, types.UINT64, types.ENUM:
		return "BIGINT"
	case types.BOOL:
		return "BOOLEAN"
	case types.FLOAT:
		return "FLOAT"
	case types.BYTES:
		return "BLOB"
	case types.DOUBLE:
		return "DOUBLE"
	case types.JSON:
		return "JSON"
	case types.STRING:
		return "VARCHAR"
	case types.DATE, types.TIMESTAMP, types.TIME, types.DATETIME, types.ARRAY, types.STRUCT, types.PROTO,
		types.GEOGRAPHY, types.NUMERIC, types.BIG_NUMERIC, types.EXTENDED, types.INTERVAL:
		return "VARCHAR"
	default:
		return "UNKNOWN"
	}
}

// See docs/duckdb-parity-roadmap.md for remaining parity gaps.
