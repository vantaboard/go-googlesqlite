//go:build duckdb && duckdb_use_lib

// Dual-backend tests run the same GoogleSQL through googlesqlite (SQLite) and googlesqlduck (DuckDB).
// Use ORDER BY when row order is part of the contract; see docs/duckdb-parity-gates.md.

package googlesqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	googlesqlite "github.com/vantaboard/go-googlesqlite"
)

var duckDualBackendMemCounter uint64

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

func TestDualBackend_phase1Corpus(t *testing.T) {
	ctx := googlesqlite.WithCurrentTime(context.Background(), time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC))

	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "select_literal",
			sql:  "SELECT 1 AS x",
		},
		{
			name: "cast_string",
			sql:  "SELECT CAST(42 AS STRING) AS s",
		},
		{
			name: "unnest_ordered",
			sql:  "SELECT val FROM UNNEST([1, 2, 3]) AS val ORDER BY val",
		},
		{
			name: "trim",
			sql:  "SELECT TRIM('  x  ') AS t",
		},
		{
			name: "concat",
			sql:  "SELECT CONCAT('a', 'b') AS c",
		},
		{
			name: "strpos",
			sql:  "SELECT STRPOS('abc', 'b') AS p",
		},
		{
			name: "merge_matched_and_insert",
			sql: `
CREATE TEMP TABLE target(id INT64, name STRING);
CREATE TEMP TABLE source(id INT64, name STRING);
INSERT INTO target(id, name) VALUES (1, 'old');
INSERT INTO source(id, name) VALUES (1, 'new'), (2, 'only_source');
MERGE target T USING source S ON T.id = S.id
WHEN MATCHED THEN UPDATE SET name = S.name
WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) VALUES (id, name);
SELECT id, name FROM target ORDER BY id;
`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sqliteDSN := fmt.Sprintf("file:phase1_%d?mode=memory&cache=private", atomic.AddUint64(&duckDualBackendMemCounter, 1))
			sqliteDB, err := sql.Open("googlesqlite", sqliteDSN)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sqliteDB.Close() })

			duckPath := filepath.Join(t.TempDir(), "corpus.duckdb")
			duckDB, err := sql.Open("googlesqlduck", duckPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = duckDB.Close() })

			a := queryAll(t, sqliteDB, ctx, tc.sql)
			b := queryAll(t, duckDB, ctx, tc.sql)
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
