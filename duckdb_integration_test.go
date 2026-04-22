//go:build duckdb && duckdb_use_lib

package googlesqlite_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	googlesqlite "github.com/vantaboard/go-googlesqlite"
)

func TestGooglesqlduckOpenAndSimpleQuery(t *testing.T) {
	ctx := googlesqlite.WithCurrentTime(context.Background(), time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC))
	db, err := sql.Open("googlesqlduck", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatal(err)
	}
	if one != 1 {
		t.Fatalf("got %d", one)
	}
}
