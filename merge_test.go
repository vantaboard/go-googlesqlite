package googlesqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	googlesqlite "github.com/vantaboard/go-googlesqlite"
)

var mergeTestMemCounter uint64

// TestMerge covers BigQuery-style MERGE DML: MATCHED / NOT MATCHED BY TARGET / NOT MATCHED BY SOURCE.
// See https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
func TestMerge(t *testing.T) {
	t.Setenv("TZ", "UTC")
	ctx := googlesqlite.WithCurrentTime(context.Background(), time.Now().UTC())

	for _, tc := range []struct {
		name          string
		sql           string
		wantRows      [][]interface{}
		wantErrSubstr string
	}{
		{
			name: "matched update plus not matched insert same run",
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
			wantRows: [][]interface{}{
				{int64(1), "new"},
				{int64(2), "only_source"},
			},
		},
		{
			name: "explicit NOT MATCHED BY TARGET synonym for insert",
			sql: `
CREATE TEMP TABLE target(id INT64, v STRING);
CREATE TEMP TABLE source(id INT64, v STRING);
INSERT INTO source(id, v) VALUES (10, 'x');
MERGE target T USING source S ON T.id = S.id
WHEN NOT MATCHED BY TARGET THEN INSERT (id, v) VALUES (id, v);
SELECT id, v FROM target;
`,
			wantRows: [][]interface{}{{int64(10), "x"}},
		},
		{
			name: "not matched by source delete orphan target rows",
			sql: `
CREATE TEMP TABLE target(id INT64, name STRING);
CREATE TEMP TABLE source(id INT64, name STRING);
INSERT INTO target(id, name) VALUES (1, 'keep'), (2, 'gone'), (3, 'gone2');
INSERT INTO source(id, name) VALUES (1, 'src');
MERGE target T USING source S ON T.id = S.id
WHEN MATCHED THEN UPDATE SET name = S.name
WHEN NOT MATCHED BY SOURCE THEN DELETE;
SELECT id, name FROM target ORDER BY id;
`,
			wantRows: [][]interface{}{{int64(1), "src"}},
		},
		{
			name: "not matched by source update",
			sql: `
CREATE TEMP TABLE target(id INT64, flag BOOL);
CREATE TEMP TABLE source(id INT64, name STRING);
INSERT INTO target(id, flag) VALUES (1, true), (2, true);
INSERT INTO source(id, name) VALUES (1, 'x');
MERGE target T USING source S ON T.id = S.id
WHEN MATCHED THEN UPDATE SET flag = false
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET flag = true;
SELECT id, flag FROM target ORDER BY id;
`,
			wantRows: [][]interface{}{
				{int64(1), false},
				{int64(2), true},
			},
		},
		{
			name: "not matched by source with extra AND predicate",
			sql: `
CREATE TEMP TABLE target(id INT64, kind STRING);
CREATE TEMP TABLE source(id INT64, name STRING);
INSERT INTO target(id, kind) VALUES (1, 'a'), (2, 'washer'), (3, 'b');
INSERT INTO source(id, name) VALUES (1, 'x');
MERGE target T USING source S ON T.id = S.id
WHEN MATCHED THEN UPDATE SET kind = 'matched'
WHEN NOT MATCHED BY SOURCE AND kind LIKE '%washer%' THEN DELETE;
SELECT id, kind FROM target ORDER BY id;
`,
			wantRows: [][]interface{}{
				{int64(1), "matched"},
				{int64(3), "b"},
			},
		},
		{
			name: "not matched by source AND predicate without prior matched clause",
			sql: `
CREATE TEMP TABLE target(id INT64, kind STRING);
CREATE TEMP TABLE source(id INT64, name STRING);
INSERT INTO target(id, kind) VALUES (2, 'washer'), (3, 'b');
INSERT INTO source(id, name) VALUES (1, 'x');
MERGE target T USING source S ON T.id = S.id
WHEN NOT MATCHED BY SOURCE AND kind LIKE '%washer%' THEN DELETE;
SELECT id, kind FROM target ORDER BY id;
`,
			wantRows: [][]interface{}{
				{int64(3), "b"},
			},
		},
		{
			name: "first qualifying not matched by target wins",
			sql: `
CREATE TEMP TABLE target(id INT64, tier INT64);
CREATE TEMP TABLE source(id INT64, qty INT64);
INSERT INTO source(id, qty) VALUES (1, 5), (2, 50);
MERGE target T USING source S ON T.id = S.id
WHEN NOT MATCHED BY TARGET AND qty < 20 THEN INSERT (id, tier) VALUES (id, 1)
WHEN NOT MATCHED BY TARGET THEN INSERT (id, tier) VALUES (id, 2);
SELECT id, tier FROM target ORDER BY id;
`,
			wantRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(2), int64(2)},
			},
		},
		{
			name: "reject multiple source rows matching one target when matched update",
			sql: `
CREATE TEMP TABLE target(id INT64, name STRING);
CREATE TEMP TABLE source(id INT64, name STRING);
INSERT INTO target(id, name) VALUES (1, 't');
INSERT INTO source(id, name) VALUES (1, 'a'), (1, 'b');
MERGE target T USING source S ON T.id = S.id
WHEN MATCHED THEN UPDATE SET name = S.name;
SELECT 1;
`,
			wantErrSubstr: "MERGE must match at most one source row",
		},
		{
			name: "ON FALSE inserts source-only and deletes target-only by predicate",
			sql: `
CREATE TEMP TABLE inv(product STRING, quantity INT64);
CREATE TEMP TABLE src(product STRING, quantity INT64);
INSERT INTO inv(product, quantity) VALUES ('dryer', 50), ('front load washer', 20), ('microwave', 20);
INSERT INTO src(product, quantity) VALUES ('top load washer', 30);
MERGE inv T USING src S ON FALSE
WHEN NOT MATCHED BY TARGET AND product LIKE '%washer%' THEN INSERT (product, quantity) VALUES (product, quantity)
WHEN NOT MATCHED BY SOURCE AND product LIKE '%washer%' THEN DELETE;
SELECT product, quantity FROM inv ORDER BY product;
`,
			wantRows: [][]interface{}{
				{"dryer", int64(50)},
				{"microwave", int64(20)},
				{"top load washer", int64(30)},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Isolate each subtest: plain ":memory:" can share state across connections in some SQLite setups.
			dsn := fmt.Sprintf("file:merge_test_%d_%d?mode=memory&cache=private",
				time.Now().UnixNano(), atomic.AddUint64(&mergeTestMemCounter, 1))
			db, err := sql.Open("googlesqlite", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()

			rows, err := db.QueryContext(ctx, tc.sql)
			if err != nil {
				if tc.wantErrSubstr == "" {
					t.Fatal(err)
				}
				if !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("error %q; want substring %q", err.Error(), tc.wantErrSubstr)
				}
				return
			}
			defer func() { _ = rows.Close() }()
			if tc.wantErrSubstr != "" {
				t.Fatal("expected error")
			}
			cols, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			var got [][]interface{}
			rowNum := 0
			for rows.Next() {
				args := make([]interface{}, len(cols))
				ptrs := make([]interface{}, len(cols))
				for i := range args {
					ptrs[i] = &args[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					t.Fatal(err)
				}
				deref := make([]interface{}, len(cols))
				for i := range args {
					deref[i] = reflect.ValueOf(ptrs[i]).Elem().Interface()
				}
				if len(tc.wantRows) <= rowNum {
					t.Fatalf("unexpected extra row %v", deref)
				}
				if diff := cmp.Diff(tc.wantRows[rowNum], deref); diff != "" {
					t.Fatalf("row %d mismatch (-want +got):\n%s", rowNum, diff)
				}
				got = append(got, deref)
				rowNum++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if len(tc.wantRows) != rowNum {
				t.Fatalf("row count: got %d want %d (accumulated %v)", rowNum, len(tc.wantRows), got)
			}
		})
	}
}
