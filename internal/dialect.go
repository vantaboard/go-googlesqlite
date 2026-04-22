package internal

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

// duckDBNativeFunctions maps googlesqlite-prefixed runtime names to DuckDB builtins where semantics align.
var duckDBNativeFunctions = map[string]string{
	"googlesqlite_length":      "length",
	"googlesqlite_char_length": "length",
	"googlesqlite_abs":         "abs",
	"googlesqlite_lower":       "lower",
	"googlesqlite_upper":       "upper",
	"googlesqlite_substr":      "substr",
}

// DuckDBDialect is the GoogleSQL-to-DuckDB codegen target (incremental parity).
type DuckDBDialect struct{}

func (DuckDBDialect) ID() string { return "duckdb" }

func (DuckDBDialect) WindowPartitionCollation() string { return "" }

func (DuckDBDialect) RewriteEmittedFunctionName(name string) (string, bool) {
	if alt, ok := duckDBNativeFunctions[name]; ok {
		return alt, true
	}
	return name, false
}

// SQLite-only codegen not yet behind Dialect (future work): array subquery/cast/UNNEST/merge helpers
// in transformer_subquery.go, transformer_scan_array.go, transformer_cast.go, transformer_stmt_merge.go.
