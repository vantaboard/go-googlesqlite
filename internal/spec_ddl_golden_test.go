package internal

import (
	"strings"
	"testing"

	ast "github.com/vantaboard/go-googlesql/resolved_ast"
	"github.com/vantaboard/go-googlesql/types"
)

func TestTableSpecPhysicalDDL_sqliteVsDuckDB(t *testing.T) {
	spec := &TableSpec{
		NamePath: []string{"t"},
		Columns: []*ColumnSpec{
			{Name: "id", Type: &Type{Kind: int(types.INT64)}},
			{Name: "name", Type: &Type{Kind: int(types.STRING)}},
		},
		PrimaryKey: []string{"id"},
		CreateMode: ast.CreateDefaultMode,
	}

	sqlite := spec.PhysicalDDL(SQLiteDialect{})
	if !strings.Contains(sqlite, "WITHOUT ROWID") {
		t.Fatalf("sqlite DDL should use WITHOUT ROWID for PK table: %s", sqlite)
	}
	if !strings.Contains(sqlite, "COLLATE googlesqlengine_collate") {
		t.Fatalf("sqlite DDL should use PK collation: %s", sqlite)
	}
	if !strings.Contains(sqlite, "`name` TEXT") {
		t.Fatalf("sqlite string column: %s", sqlite)
	}
	if !strings.Contains(sqlite, "`id` INT") {
		t.Fatalf("sqlite int column: %s", sqlite)
	}

	duck := spec.PhysicalDDL(DuckDBDialect{})
	if strings.Contains(duck, "WITHOUT ROWID") {
		t.Fatalf("duckdb DDL should not use WITHOUT ROWID: %s", duck)
	}
	if strings.Contains(duck, "googlesqlengine_collate") {
		t.Fatalf("duckdb DDL should not use googlesqlengine_collate: %s", duck)
	}
	if !strings.Contains(duck, `"name" VARCHAR`) {
		t.Fatalf("duckdb string column: %s", duck)
	}
	if !strings.Contains(duck, `"id" BIGINT`) {
		t.Fatalf("duckdb int column: %s", duck)
	}
}

func TestTableSpecPhysicalDDL_sqliteSchemaAlias(t *testing.T) {
	spec := &TableSpec{
		NamePath:   []string{"x"},
		Columns:    []*ColumnSpec{{Name: "k", Type: &Type{Kind: int(types.INT64)}}},
		PrimaryKey: []string{"k"},
		CreateMode: ast.CreateIfNotExistsMode,
	}
	if g, w := spec.SQLiteSchema(), spec.PhysicalDDL(SQLiteDialect{}); g != w {
		t.Fatalf("SQLiteSchema != PhysicalDDL(SQLite)\ngot  %q\nwant %q", g, w)
	}
}
