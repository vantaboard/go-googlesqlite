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
	// ArraySubqueryListAggregate is the aggregate applied to the inner query column for array subqueries.
	// SQLite uses googlesqlite_array; DuckDB uses list().
	ArraySubqueryListAggregate() string
	// WrapGroupByKey wraps GROUP BY expressions (SQLite uses googlesqlite_group_by for complex keys).
	WrapGroupByKey(expr *SQLExpression) *SQLExpression
	// MaybeEmitNativeCast returns a backend-native cast, or (nil, nil) to fall back to googlesqlite_cast.
	MaybeEmitNativeCast(inner *SQLExpression, cast *CastData) (*SQLExpression, error)
	// ArrayUnnestUseLateralCorrelation is true when correlated UNNEST should use JOIN LATERAL (DuckDB).
	ArrayUnnestUseLateralCorrelation() bool
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

func (DuckDBDialect) ArraySubqueryListAggregate() string { return "list" }

func (DuckDBDialect) WrapGroupByKey(expr *SQLExpression) *SQLExpression { return expr }

func (DuckDBDialect) ArrayUnnestUseLateralCorrelation() bool { return true }

// MERGE and other SQLite-only helpers remain dialect follow-ups; see docs/duckdb-parity-roadmap.md.
