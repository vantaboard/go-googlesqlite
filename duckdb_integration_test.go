//go:build duckdb && duckdb_use_lib

// Dual-backend tests run the same GoogleSQL through googlesqlite (SQLite) and googlesqlduck (DuckDB).
// Use ORDER BY when row order is part of the contract; see docs/duckdb-parity-gates.md.

package googlesqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
		{
			name: "create_table_insert_select",
			sql: `
CREATE TABLE ddl_corpus_t (id INT64, name STRING);
INSERT INTO ddl_corpus_t(id, name) VALUES (1, 'a'), (2, 'b');
SELECT id, name FROM ddl_corpus_t ORDER BY id;
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

func TestDualBackend_phase2FunctionSurface(t *testing.T) {
	ctx := googlesqlite.WithCurrentTime(context.Background(), time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC))

	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "strpos_two_arg",
			// DuckDB INSTR is not BigQuery-compatible for 2-arg substring search; STRPOS matches both backends here.
			sql: "SELECT STRPOS('foobar', 'bar') AS p FROM (SELECT 1) AS _t ORDER BY p",
		},
		{
			name: "starts_with_as_int64",
			sql:  "SELECT CAST(STARTS_WITH('abc', 'ab') AS INT64) AS v FROM (SELECT 1) AS _t ORDER BY v",
		},
		{
			name: "md5_non_null",
			sql:  "SELECT CAST(MD5('x') IS NOT NULL AS INT64) AS ok FROM (SELECT 1) AS _t ORDER BY ok",
		},
		{
			name: "byte_length",
			sql:  "SELECT BYTE_LENGTH('ab') AS n FROM (SELECT 1) AS _t ORDER BY n",
		},
		{
			name: "current_timestamp_extract_ymd",
			sql:  "SELECT EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS y, EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS m, EXTRACT(DAY FROM CURRENT_TIMESTAMP()) AS d FROM (SELECT 1) AS _t ORDER BY y, m, d",
		},
		{
			name: "json_extract_scalar_cast",
			// JSON_VALUE yields a string scalar analyzable as INT64 cast; JSON_EXTRACT stays typed as JSON.
			sql: "SELECT CAST(JSON_VALUE(JSON '{\"a\":7}', '$.a') AS INT64) AS v FROM (SELECT 1) AS _t ORDER BY v",
		},
		{
			name: "sum_group_by",
			sql: `
CREATE TEMP TABLE sum_g (k INT64, v INT64);
INSERT INTO sum_g VALUES (1,10),(1,20),(2,5);
SELECT k, SUM(v) AS s FROM sum_g GROUP BY k ORDER BY k;
`,
		},
		{
			name: "row_number_over",
			sql: `
CREATE TEMP TABLE win_g (x INT64);
INSERT INTO win_g VALUES (30),(10),(20);
SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn FROM win_g ORDER BY x, rn;
`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sqliteDSN := fmt.Sprintf("file:phase2_%d?mode=memory&cache=private", atomic.AddUint64(&duckDualBackendMemCounter, 1))
			sqliteDB, err := sql.Open("googlesqlite", sqliteDSN)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sqliteDB.Close() })

			duckPath := filepath.Join(t.TempDir(), "phase2.duckdb")
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

func TestDualBackend_phase3DDLCTAS(t *testing.T) {
	ctx := googlesqlite.WithCurrentTime(context.Background(), time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC))
	// CTAS + multi-column DDL (workload-style). Keep projections simple so emitted SQL stays dialect-quoted.
	q := `
CREATE TABLE ctas_src (id INT64, v INT64);
INSERT INTO ctas_src VALUES (1, 10), (2, 20), (3, 5);
CREATE TABLE ctas_dst AS SELECT id, v FROM ctas_src WHERE v > 5;
SELECT id, v FROM ctas_dst ORDER BY id;
`
	sqliteDSN := fmt.Sprintf("file:phase3_%d?mode=memory&cache=private", atomic.AddUint64(&duckDualBackendMemCounter, 1))
	sqliteDB, err := sql.Open("googlesqlite", sqliteDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })

	duckPath := filepath.Join(t.TempDir(), "phase3_ctas.duckdb")
	duckDB, err := sql.Open("googlesqlduck", duckPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = duckDB.Close() })

	a := queryAll(t, sqliteDB, ctx, q)
	b := queryAll(t, duckDB, ctx, q)
	if !reflect.DeepEqual(normalizeRows(a), normalizeRows(b)) {
		t.Fatalf("sqlite=%v duckdb=%v", a, b)
	}
}

func TestDualBackend_namedParameter(t *testing.T) {
	ctx := googlesqlite.WithCurrentTime(context.Background(), time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC))
	q := `SELECT 40 + CAST(@p AS INT64) AS v`

	sqliteDSN := fmt.Sprintf("file:named_%d?mode=memory&cache=private", atomic.AddUint64(&duckDualBackendMemCounter, 1))
	sqliteDB, err := sql.Open("googlesqlite", sqliteDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })

	duckPath := filepath.Join(t.TempDir(), "named.duckdb")
	duckDB, err := sql.Open("googlesqlduck", duckPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = duckDB.Close() })

	a := queryAllArgs(t, sqliteDB, ctx, q, sql.Named("p", 2))
	b := queryAllArgs(t, duckDB, ctx, q, sql.Named("p", 2))
	if !reflect.DeepEqual(normalizeRows(a), normalizeRows(b)) {
		t.Fatalf("sqlite=%v duckdb=%v", a, b)
	}
}

func TestDualBackend_transactionCommitAndRollback(t *testing.T) {
	ctx := googlesqlite.WithCurrentTime(context.Background(), time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC))

	t.Run("sqlite", func(t *testing.T) {
		dsn := fmt.Sprintf("file:tx_%d?mode=memory&cache=private", atomic.AddUint64(&duckDualBackendMemCounter, 1))
		db, err := sql.Open("googlesqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		runTransactionSmoke(t, db, ctx)
	})
	t.Run("duckdb", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tx.duckdb")
		db, err := sql.Open("googlesqlduck", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		runTransactionSmoke(t, db, ctx)
	})
}

// TestDualBackend_dateBetweenTableSeed loads DATE seed rows into a real table (VARCHAR wire on DuckDB),
// then checks BETWEEN vs >= / <= and expected match count. See testdata/date_between_table_seed.sql.
func TestDualBackend_dateBetweenTableSeed(t *testing.T) {
	ctx := googlesqlite.WithCurrentTime(context.Background(), time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC))
	b, err := os.ReadFile("testdata/date_between_table_seed.sql")
	if err != nil {
		t.Fatal(err)
	}
	q := strings.TrimSpace(string(b))

	sqliteDSN := fmt.Sprintf("file:date_between_%d?mode=memory&cache=private", atomic.AddUint64(&duckDualBackendMemCounter, 1))
	sqliteDB, err := sql.Open("googlesqlite", sqliteDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })

	duckPath := filepath.Join(t.TempDir(), "date_between.duckdb")
	duckDB, err := sql.Open("googlesqlduck", duckPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = duckDB.Close() })

	a := queryAll(t, sqliteDB, ctx, q)
	bRows := queryAll(t, duckDB, ctx, q)
	if !reflect.DeepEqual(normalizeRows(a), normalizeRows(bRows)) {
		t.Fatalf("sqlite=%v duckdb=%v", normalizeRows(a), normalizeRows(bRows))
	}

	na := normalizeRows(a)
	if len(na) != 20 {
		t.Fatalf("want 20 seed rows, got %d", len(na))
	}
	var inRange int
	for i, r := range na {
		if len(r) != 5 {
			t.Fatalf("row %d: want 5 cols, got %d %v", i, len(r), r)
		}
		if r[3] != r[4] {
			t.Fatalf("row %d: is_between %q != is_range %q (row %v)", i, r[3], r[4], r)
		}
		if r[3] == "1" {
			inRange++
		}
	}
	if inRange != 10 {
		t.Fatalf("want 10 rows with StartDateA in [StartDateB, EndDateB], got %d", inRange)
	}
}

func runTransactionSmoke(t *testing.T, db *sql.DB, ctx context.Context) {
	t.Helper()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE tx_smoke (id INT64)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tx_smoke(id) VALUES (7)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rows := queryAll(t, db, ctx, `SELECT id FROM tx_smoke ORDER BY id`)
	if len(rows) != 1 || fmt.Sprint(rows[0][0]) != "7" {
		t.Fatalf("after commit: %v", rows)
	}

	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.ExecContext(ctx, `CREATE TABLE tx_rollback_t (id INT64)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.ExecContext(ctx, `INSERT INTO tx_rollback_t(id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatal(err)
	}
	rowsRb, err := db.QueryContext(ctx, `SELECT id FROM tx_rollback_t`)
	if err == nil {
		_ = rowsRb.Close()
		t.Fatal("expected error querying table after rollback")
	}
}

// queryAllArgs is like queryAll but passes optional args to QueryContext.
func queryAllArgs(t *testing.T, db *sql.DB, ctx context.Context, q string, args ...interface{}) [][]interface{} {
	t.Helper()
	rows, err := db.QueryContext(ctx, q, args...)
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
