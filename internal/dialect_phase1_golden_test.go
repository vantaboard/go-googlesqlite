package internal

import (
	"strings"
	"testing"
)

func TestDialectGolden_arraySubqueryAggregateName(t *testing.T) {
	if g, w := (SQLiteDialect{}).ArraySubqueryListAggregate(), "googlesqlite_array"; g != w {
		t.Fatalf("sqlite: got %q want %q", g, w)
	}
	if g, w := (DuckDBDialect{}).ArraySubqueryListAggregate(), "list"; g != w {
		t.Fatalf("duckdb: got %q want %q", g, w)
	}
}

func TestDialectGolden_groupByWrap(t *testing.T) {
	col := NewColumnExpression("x", "t")
	sqlite := (SQLiteDialect{}).WrapGroupByKey(col)
	if sqlite.Type != ExpressionTypeFunction || sqlite.FunctionCall == nil || sqlite.FunctionCall.Name != "googlesqlite_group_by" {
		t.Fatalf("sqlite wrap: %#v", sqlite)
	}
	duck := (DuckDBDialect{}).WrapGroupByKey(col)
	if duck != col {
		t.Fatalf("duckdb should not wrap GROUP BY keys, got %#v", duck)
	}
}

func TestSQLCastExpression_WriteSql(t *testing.T) {
	inner := NewLiteralExpression("42")
	cast := NewSQLCastExpression(inner, "BIGINT", false)
	if s := cast.String(); !strings.Contains(s, "CAST(") || !strings.Contains(s, "AS BIGINT") {
		t.Fatalf("got %q", s)
	}
	try := NewSQLCastExpression(inner, "VARCHAR", true)
	if s := try.String(); !strings.Contains(s, "TRY_CAST(") || !strings.Contains(s, "AS VARCHAR") {
		t.Fatalf("got %q", s)
	}
}

func TestJoinClause_lateralWriteSql(t *testing.T) {
	right := NewSubqueryFromItem(&SelectStatement{
		SelectList: []*SelectListItem{{Expression: NewLiteralExpression("1"), Alias: "a"}},
	}, "u")
	j := &JoinClause{
		Type:           JoinTypeInner,
		Left:           &FromItem{Type: FromItemTypeTable, TableName: "t", Alias: "t"},
		Right:          right,
		Condition:      NewLiteralExpression("true"),
		RightIsLateral: true,
	}
	got := (&FromItem{Type: FromItemTypeJoin, Join: j}).String()
	if !strings.Contains(got, "INNER JOIN") || !strings.Contains(got, "LATERAL") {
		t.Fatalf("expected INNER JOIN LATERAL in %q", got)
	}
}
