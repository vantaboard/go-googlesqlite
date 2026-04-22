package internal

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// catalogBindTime returns a Time safe to pass to database/sql for TIMESTAMP columns.
// Go's time.Time may include a monotonic clock; when drivers stringify binds, that can
// produce values DuckDB rejects (e.g. "... m=+0.123").
func catalogBindTime(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return time.Unix(0, t.UnixNano()).In(t.Location())
}

// CatalogRepository persists googlesqlite metadata table specs in a dialect-specific way.
type CatalogRepository interface {
	EnsureSchema(ctx context.Context, conn *Conn) error
	QueryUpdatedSince(ctx context.Context, conn *Conn, lastSyncedAt time.Time) (*sql.Rows, error)
	Upsert(ctx context.Context, conn *Conn, name string, kind CatalogSpecKind, specJSON string, at time.Time) error
	Delete(ctx context.Context, conn *Conn, name string) error
}

type sqliteCatalogRepository struct {
	createTable string
	createIndex string
	upsert      string
	deleteRow   string
	selectSince string
}

// NewSQLiteCatalogRepository returns catalog persistence matching the historical SQLite DDL/DML.
func NewSQLiteCatalogRepository() CatalogRepository {
	return &sqliteCatalogRepository{
		createTable: `
CREATE TABLE IF NOT EXISTS googlesqlite_catalog(
  name STRING NOT NULL PRIMARY KEY,
  kind STRING NOT NULL,
  spec STRING NOT NULL,
  updatedAt TIMESTAMP NOT NULL,
  createdAt TIMESTAMP NOT NULL
)`,
		createIndex: `
CREATE INDEX IF NOT EXISTS catalog_last_updated_index ON googlesqlite_catalog(updatedAt DESC)`,
		upsert: `
INSERT INTO googlesqlite_catalog (
  name,
  kind,
  spec,
  updatedAt,
  createdAt
) VALUES (
  @name,
  @kind,
  @spec,
  @updatedAt,
  @createdAt
) ON CONFLICT(name) DO UPDATE SET
  spec = @spec,
  updatedAt = @updatedAt
`,
		deleteRow: `DELETE FROM googlesqlite_catalog WHERE name = @name`,
		selectSince: `
SELECT name, kind, spec FROM googlesqlite_catalog WHERE updatedAt >= @lastUpdatedAt`,
	}
}

func (r *sqliteCatalogRepository) EnsureSchema(ctx context.Context, conn *Conn) error {
	if _, err := conn.ExecContext(ctx, r.createTable); err != nil {
		return fmt.Errorf("create catalog table: %w", err)
	}
	if _, err := conn.ExecContext(ctx, r.createIndex); err != nil {
		return fmt.Errorf("create catalog index: %w", err)
	}
	return nil
}

func (r *sqliteCatalogRepository) QueryUpdatedSince(ctx context.Context, conn *Conn, lastSyncedAt time.Time) (*sql.Rows, error) {
	return conn.QueryContext(ctx, r.selectSince, sql.Named("lastUpdatedAt", catalogBindTime(lastSyncedAt)))
}

func (r *sqliteCatalogRepository) Upsert(ctx context.Context, conn *Conn, name string, kind CatalogSpecKind, specJSON string, at time.Time) error {
	at = catalogBindTime(at)
	_, err := conn.ExecContext(ctx, r.upsert,
		sql.Named("name", name),
		sql.Named("kind", string(kind)),
		sql.Named("spec", specJSON),
		sql.Named("updatedAt", at),
		sql.Named("createdAt", at),
	)
	return err
}

func (r *sqliteCatalogRepository) Delete(ctx context.Context, conn *Conn, name string) error {
	_, err := conn.ExecContext(ctx, r.deleteRow, sql.Named("name", name))
	return err
}

type duckdbCatalogRepository struct {
	createTable string
	createIndex string
	upsert      string
	deleteRow   string
	selectSince string
}

// NewDuckDBCatalogRepository uses DuckDB-friendly types (VARCHAR) and positional parameters.
// DuckDB's binder treats @ident as a column reference, not a database/sql named argument like SQLite.
func NewDuckDBCatalogRepository() CatalogRepository {
	return &duckdbCatalogRepository{
		createTable: `
CREATE TABLE IF NOT EXISTS googlesqlite_catalog(
  name VARCHAR NOT NULL PRIMARY KEY,
  kind VARCHAR NOT NULL,
  spec VARCHAR NOT NULL,
  updatedAt TIMESTAMP NOT NULL,
  createdAt TIMESTAMP NOT NULL
)`,
		createIndex: `
CREATE INDEX IF NOT EXISTS catalog_last_updated_index ON googlesqlite_catalog(updatedAt DESC)`,
		upsert: `
INSERT INTO googlesqlite_catalog (
  name,
  kind,
  spec,
  updatedAt,
  createdAt
) VALUES (
  ?,
  ?,
  ?,
  ?,
  ?
) ON CONFLICT(name) DO UPDATE SET
  spec = excluded.spec,
  updatedAt = excluded.updatedAt
`,
		deleteRow:   `DELETE FROM googlesqlite_catalog WHERE name = ?`,
		selectSince: `SELECT name, kind, spec FROM googlesqlite_catalog WHERE updatedAt >= ?`,
	}
}

func (r *duckdbCatalogRepository) EnsureSchema(ctx context.Context, conn *Conn) error {
	if _, err := conn.ExecContext(ctx, r.createTable); err != nil {
		return fmt.Errorf("create catalog table: %w", err)
	}
	if _, err := conn.ExecContext(ctx, r.createIndex); err != nil {
		return fmt.Errorf("create catalog index: %w", err)
	}
	return nil
}

func (r *duckdbCatalogRepository) QueryUpdatedSince(ctx context.Context, conn *Conn, lastSyncedAt time.Time) (*sql.Rows, error) {
	return conn.QueryContext(ctx, r.selectSince, catalogBindTime(lastSyncedAt))
}

func (r *duckdbCatalogRepository) Upsert(ctx context.Context, conn *Conn, name string, kind CatalogSpecKind, specJSON string, at time.Time) error {
	at = catalogBindTime(at)
	_, err := conn.ExecContext(ctx, r.upsert, name, string(kind), specJSON, at, at)
	return err
}

func (r *duckdbCatalogRepository) Delete(ctx context.Context, conn *Conn, name string) error {
	_, err := conn.ExecContext(ctx, r.deleteRow, name)
	return err
}
