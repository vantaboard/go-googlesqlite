//go:build duckdb && duckdb_use_lib

// DuckDB catalog repository tests live in package googlesqlite_test (not internal) so
// `go test ./internal` does not build a second test binary that must load libduckdb.so.

package googlesqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	internal "github.com/vantaboard/go-googlesqlite/internal"
)

func TestDuckDBCatalogRepository_ensureUpsertQuery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.duckdb")
	db, err := internal.OpenSQLBackend(internal.DuckDBBackend{}, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	raw, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	conn := internal.NewConn(raw, nil)

	repo := internal.NewDuckDBCatalogRepository()
	if err := repo.EnsureSchema(ctx, conn); err != nil {
		t.Fatal(err)
	}

	at := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	if err := repo.Upsert(ctx, conn, "proj.ds.t1", internal.TableSpecKind, `{"k":"v"}`, at); err != nil {
		t.Fatal(err)
	}

	rows, err := repo.QueryUpdatedSince(ctx, conn, at.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("expected one row from QueryUpdatedSince")
	}
	var name, kind, spec string
	if err := rows.Scan(&name, &kind, &spec); err != nil {
		t.Fatal(err)
	}
	if name != "proj.ds.t1" || kind != string(internal.TableSpecKind) || spec != `{"k":"v"}` {
		t.Fatalf("got name=%q kind=%q spec=%q", name, kind, spec)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestDuckDBCatalogRepository_upsertQueryWithTimeNow(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog_now.duckdb")
	db, err := internal.OpenSQLBackend(internal.DuckDBBackend{}, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	raw, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	conn := internal.NewConn(raw, nil)

	repo := internal.NewDuckDBCatalogRepository()
	if err := repo.EnsureSchema(ctx, conn); err != nil {
		t.Fatal(err)
	}

	at := time.Now()
	if err := repo.Upsert(ctx, conn, "proj.ds.t_now", internal.TableSpecKind, `{"k":"now"}`, at); err != nil {
		t.Fatal(err)
	}

	rows, err := repo.QueryUpdatedSince(ctx, conn, at.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("expected one row when filtering with time.Now() bind (monotonic clock must not break DuckDB)")
	}
}
