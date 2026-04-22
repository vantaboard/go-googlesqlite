package internal

// Dialect selects SQL generation and catalog persistence behavior for a physical engine.
// The surface is intentionally small; extend with DDL helpers, placeholder style, etc. as needed.
type Dialect interface {
	// ID returns a stable backend identifier ("sqlite", "duckdb", ...).
	ID() string
}

// SQLiteDialect is the default GoogleSQL-to-SQLite codegen target.
type SQLiteDialect struct{}

func (SQLiteDialect) ID() string { return "sqlite" }

// DuckDBDialect is the GoogleSQL-to-DuckDB codegen target (incremental parity).
type DuckDBDialect struct{}

func (DuckDBDialect) ID() string { return "duckdb" }
