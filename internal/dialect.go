package internal

import (
	"github.com/vantaboard/go-googlesql-engine/internal/dialect/core"
	engduck "github.com/vantaboard/go-googlesql-engine/internal/dialect/duckdb"
	engsqlite "github.com/vantaboard/go-googlesql-engine/internal/dialect/sqlite"
)

// Dialect selects SQL generation and catalog persistence behavior for a physical engine.
type Dialect = core.Dialect

// SQLiteDialect is the default GoogleSQL-to-SQLite codegen target.
type SQLiteDialect = engsqlite.Dialect

// DuckDBDialect is the GoogleSQL-to-DuckDB codegen target.
type DuckDBDialect = engduck.Dialect

// ApplySortCollation sets expr.Collation when the dialect uses a sort collation (SQLite only today).
func ApplySortCollation(d Dialect, expr *SQLExpression) {
	core.ApplySortCollation(d, expr)
}
