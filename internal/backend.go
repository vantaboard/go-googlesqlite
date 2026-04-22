package internal

import (
	"database/sql"
	"fmt"
)

// SQLBackend opens a database/sql pool and provides dialect + catalog persistence for one engine.
type SQLBackend interface {
	// DriverName is the database/sql driver name passed to sql.Open (e.g. "sqlite", "duckdb").
	DriverName() string
	Open(dsn string) (*sql.DB, error)
	// RegisterExtensions runs engine-specific setup on db (UDFs, pragmas). May be a no-op.
	RegisterExtensions(db *sql.DB) error
	Dialect() Dialect
	NewCatalogRepository() CatalogRepository
}

// SQLiteBackend uses modernc.org/sqlite. Custom functions are registered globally in package init.
type SQLiteBackend struct{}

func (SQLiteBackend) DriverName() string { return "sqlite" }

func (SQLiteBackend) Open(dsn string) (*sql.DB, error) {
	return sql.Open("sqlite", dsn)
}

func (SQLiteBackend) RegisterExtensions(_ *sql.DB) error { return nil }

func (SQLiteBackend) Dialect() Dialect { return SQLiteDialect{} }

func (SQLiteBackend) NewCatalogRepository() CatalogRepository {
	return NewSQLiteCatalogRepository()
}

// DuckDBBackend uses github.com/duckdb/duckdb-go/v2 (database/sql driver "duckdb").
type DuckDBBackend struct{}

func (DuckDBBackend) DriverName() string { return "duckdb" }

func (DuckDBBackend) Open(dsn string) (*sql.DB, error) {
	return sql.Open("duckdb", dsn)
}

func (DuckDBBackend) RegisterExtensions(_ *sql.DB) error { return nil }

func (DuckDBBackend) Dialect() Dialect { return DuckDBDialect{} }

func (DuckDBBackend) NewCatalogRepository() CatalogRepository {
	return NewDuckDBCatalogRepository()
}

// OpenSQLBackend opens db, applies RegisterExtensions, and pings once.
func OpenSQLBackend(b SQLBackend, dsn string) (*sql.DB, error) {
	db, err := b.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("sql open (%s): %w", b.DriverName(), err)
	}
	if err := b.RegisterExtensions(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sql ping (%s): %w", b.DriverName(), err)
	}
	if _, ok := b.(DuckDBBackend); ok {
		// duckdb-go: avoid retaining idle conns that keep DB state; see docs/duckdb-phase3-phase4-followon.md
		db.SetMaxIdleConns(0)
	}
	return db, nil
}
