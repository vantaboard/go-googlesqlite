//go:build duckdb

// Package googlesqlite registers driver name "googlesqlduck" when built with -tags duckdb.
// Linking pulls in duckdb-go (C++ static libs); ensure your CGO linker can resolve libstdc++
// (or libc++) alongside the stack's usual CGO flags.

package googlesqlite

import (
	"database/sql"
	"database/sql/driver"

	internal "github.com/vantaboard/go-googlesqlite/internal"

	_ "github.com/duckdb/duckdb-go/v2"
)

func init() {
	sql.Register("googlesqlduck", &GoogleSQLDuckDriver{})
}

// GoogleSQLDuckDriver runs GoogleSQL through the same pipeline as [GoogleSQLiteDriver] but uses
// DuckDB via [github.com/duckdb/duckdb-go/v2] and [internal.DuckDBDialect]. Register name: "googlesqlduck".
type GoogleSQLDuckDriver struct {
	ConnectHook func(*GoogleSQLiteConn) error
}

func (d *GoogleSQLDuckDriver) Open(name string) (driver.Conn, error) {
	db, catalog, err := newDBAndCatalogWithBackend(name, internal.DuckDBBackend{})
	if err != nil {
		return nil, err
	}
	conn, err := newGoogleSQLiteConn(db, catalog, internal.DuckDBDialect{})
	if err != nil {
		return nil, err
	}
	if d.ConnectHook != nil {
		if err := d.ConnectHook(conn); err != nil {
			return nil, err
		}
	}
	return conn, nil
}
