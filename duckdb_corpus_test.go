//go:build duckdb && duckdb_use_lib

package googlesqlite_test

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	googlesqlite "github.com/vantaboard/go-googlesqlite"
)

//go:embed testdata/duckdb_corpus/*.sql
var duckdbCorpusSQL embed.FS

// TestDuckdbCorpusSQLFiles runs each file under testdata/duckdb_corpus through SQLite and DuckDB
// (parity gates: ordered comparison via normalizeRows).
func TestDuckdbCorpusSQLFiles(t *testing.T) {
	ctx := googlesqlite.WithCurrentTime(context.Background(), time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC))
	entries, err := duckdbCorpusSQL.ReadDir("testdata/duckdb_corpus")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		name := e.Name()
		t.Run(name, func(t *testing.T) {
			b, err := duckdbCorpusSQL.ReadFile("testdata/duckdb_corpus/" + name)
			if err != nil {
				t.Fatal(err)
			}
			q := strings.TrimSpace(string(b))
			if q == "" {
				t.Skip("empty file")
			}

			sqliteDSN := fmt.Sprintf("file:corpus_%s?mode=memory&cache=private", name)
			sqliteDB, err := sql.Open("googlesqlite", sqliteDSN)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sqliteDB.Close() })

			duckPath := filepath.Join(t.TempDir(), strings.ReplaceAll(name, ".sql", ".duckdb"))
			duckDB, err := sql.Open("googlesqlduck", duckPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = duckDB.Close() })

			a := queryAll(t, sqliteDB, ctx, q)
			bRows := queryAll(t, duckDB, ctx, q)
			if !reflect.DeepEqual(normalizeRows(a), normalizeRows(bRows)) {
				t.Fatalf("sqlite=%v duckdb=%v", a, bRows)
			}
		})
	}
}
