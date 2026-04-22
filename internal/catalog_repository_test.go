package internal

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSQLiteCatalogRepository_ensureUpsertQuery(t *testing.T) {
	ctx := context.Background()
	dsn := fmt.Sprintf("file:catalog_repo_%d?mode=memory&cache=private", time.Now().UnixNano())
	db, err := OpenSQLBackend(SQLiteBackend{}, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	raw, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	conn := NewConn(raw, nil)

	repo := NewSQLiteCatalogRepository()
	if err := repo.EnsureSchema(ctx, conn); err != nil {
		t.Fatal(err)
	}

	at := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	if err := repo.Upsert(ctx, conn, "proj.ds.t1", TableSpecKind, `{"k":"v"}`, at); err != nil {
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
	if name != "proj.ds.t1" || kind != string(TableSpecKind) || spec != `{"k":"v"}` {
		t.Fatalf("got name=%q kind=%q spec=%q", name, kind, spec)
	}
	if rows.Next() {
		t.Fatal("unexpected second row")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if err := repo.Delete(ctx, conn, "proj.ds.t1"); err != nil {
		t.Fatal(err)
	}
	rows2, err := repo.QueryUpdatedSince(ctx, conn, at.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows2.Close() }()
	if rows2.Next() {
		t.Fatal("expected no rows after delete")
	}
}
