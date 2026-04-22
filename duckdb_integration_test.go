//go:build duckdb && duckdb_use_lib

package googlesqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
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

// queryAll scans all rows into [][]interface{} (portable dual-backend comparison).
func queryAll(t *testing.T, db *sql.DB, ctx context.Context, q string) [][]interface{} {
	t.Helper()
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out [][]interface{}
	for rows.Next() {
		raw := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, raw)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDualBackend_paritySampleQueries(t *testing.T) {
	ctx := googlesqlite.WithCurrentTime(context.Background(), time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC))

	sqliteDB, err := sql.Open("googlesqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })

	duckDB, err := sql.Open("googlesqlduck", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = duckDB.Close() })

	cases := []string{
		"SELECT 1 AS x",
		"SELECT CAST(42 AS STRING) AS s",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			a := queryAll(t, sqliteDB, ctx, q)
			b := queryAll(t, duckDB, ctx, q)
			if !reflect.DeepEqual(normalizeRows(a), normalizeRows(b)) {
				t.Fatalf("sqlite=%v duckdb=%v", a, b)
			}
		})
	}
}

// normalizeRows stringifies values so int64 vs int driver differences still compare equal for simple literals.
func normalizeRows(rows [][]interface{}) [][]string {
	if rows == nil {
		return nil
	}
	out := make([][]string, len(rows))
	for i, r := range rows {
		out[i] = make([]string, len(r))
		for j, v := range r {
			if v == nil {
				out[i][j] = "<nil>"
				continue
			}
			switch x := v.(type) {
			case []byte:
				out[i][j] = string(x)
			default:
				out[i][j] = fmt.Sprint(x)
			}
		}
	}
	return out
}
