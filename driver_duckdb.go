//go:build duckdb

// Package googlesqlite registers driver name "googlesqlduck" when built with -tags duckdb.
//
// Linking (see https://github.com/duckdb/duckdb-go#linking-duckdb ):
//
//   - Default: duckdb-go statically links pre-built DuckDB; you need a CGO toolchain that can
//     link those C++ archives (libstdc++/libc++), consistent with your GOOGLESQL_* / mold setup.
//
//   - Dynamic library (recommended when static linking fails or for smaller binaries): download
//     libduckdb from https://github.com/duckdb/duckdb/releases , then build with BOTH tags
//     duckdb (this file) and duckdb_use_lib (upstream), set CGO_LDFLAGS, and at runtime set the
//     loader path. Example on Linux (from upstream README):
//
//	CGO_ENABLED=1 CGO_LDFLAGS="-lduckdb -L/path/to/libs" \
//	  go build -tags "duckdb,duckdb_use_lib,…" .
//	LD_LIBRARY_PATH=/path/to/libs ./yourbinary
//
//     On macOS use DYLD_LIBRARY_PATH instead of LD_LIBRARY_PATH. Use `task test:duckdb-lib`
//     in this repo when DUCKDB_LIB_DIR is set.
//
// Upstream details: https://github.com/duckdb/duckdb-go#linking-a-dynamic-library

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
