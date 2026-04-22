package internal

import (
	"strings"
	"testing"
)

func TestDuckDBRewrite_getStructField_oneBased(t *testing.T) {
	s := NewColumnExpression("s", "t")
	idx := NewLiteralExpression("0")
	out, ok := duckDBRewriteFunctionCall("googlesqlite_get_struct_field", []*SQLExpression{s, idx}, nil, DuckDBDialect{})
	if !ok {
		t.Fatal("expected rewrite")
	}
	got := out.String()
	if strings.Contains(got, "googlesqlite_get_struct_field") {
		t.Fatalf("got %q", got)
	}
	if !strings.Contains(got, "struct_extract(") || !strings.Contains(got, ", 1)") {
		t.Fatalf("expected struct_extract(..., 1), got %q", got)
	}
}

func TestDuckDBRewrite_getStructField_namedKey(t *testing.T) {
	s := NewColumnExpression("y", "t")
	wire, err := LiteralFromValue(StringValue("enrollmentDate"))
	if err != nil {
		t.Fatal(err)
	}
	keyLit := NewLiteralExpression(wire)
	out, ok := duckDBRewriteFunctionCall("googlesqlite_get_struct_field", []*SQLExpression{s, keyLit}, nil, DuckDBDialect{})
	if !ok {
		t.Fatal("expected rewrite")
	}
	got := out.String()
	if !strings.Contains(got, "struct_extract(") || !strings.Contains(got, "'enrollmentDate'") {
		t.Fatalf("expected struct_extract(..., 'enrollmentDate'), got %q", got)
	}
}

func TestDuckDBRewrite_dateCastAndMakeDate(t *testing.T) {
	x := NewColumnExpression("x")
	out1, ok := duckDBRewriteFunctionCall("googlesqlite_date", []*SQLExpression{x}, nil, DuckDBDialect{})
	if !ok {
		t.Fatal("expected rewrite")
	}
	if got := out1.String(); !strings.Contains(got, "CAST(") || !strings.Contains(got, " AS DATE)") {
		t.Fatalf("got %q", got)
	}
	y, m, d := NewLiteralExpression("2024"), NewLiteralExpression("6"), NewLiteralExpression("15")
	out3, ok := duckDBRewriteFunctionCall("googlesqlite_date", []*SQLExpression{y, m, d}, nil, DuckDBDialect{})
	if !ok {
		t.Fatal("expected rewrite")
	}
	if got := out3.String(); !strings.Contains(got, "make_date(") {
		t.Fatalf("got %q", got)
	}
}

func TestDuckDBAggregateWriteSql_trailingDistinctAndIgnoreNulls(t *testing.T) {
	col := NewColumnExpression("sid")
	distArg := &SQLExpression{Type: ExpressionTypeFunction, FunctionCall: &FunctionCall{Name: "googlesqlite_distinct", Arguments: []*SQLExpression{}}}
	ignArg := &SQLExpression{Type: ExpressionTypeFunction, FunctionCall: &FunctionCall{Name: "googlesqlite_ignore_nulls", Arguments: []*SQLExpression{}}}
	fc := &FunctionCall{Name: "array_agg", Arguments: []*SQLExpression{col, distArg, ignArg}}
	expr := &SQLExpression{Type: ExpressionTypeFunction, FunctionCall: fc}
	w := NewSQLWriterForDialect(DuckDBDialect{})
	expr.WriteSql(w)
	got := strings.ReplaceAll(w.String(), " ", "")
	if strings.Contains(got, "googlesqlite") {
		t.Fatalf("got %q", w.String())
	}
	if !strings.Contains(got, "DISTINCT") || !strings.Contains(got, "FILTER(WHERE") {
		t.Fatalf("got %q", w.String())
	}
}

func TestDuckDBAggregateWriteSql_countTrailingDistinct(t *testing.T) {
	col := NewColumnExpression("StudentID")
	distArg := &SQLExpression{Type: ExpressionTypeFunction, FunctionCall: &FunctionCall{Name: "googlesqlite_distinct", Arguments: []*SQLExpression{}}}
	fc := &FunctionCall{Name: "count", Arguments: []*SQLExpression{col, distArg}}
	expr := &SQLExpression{Type: ExpressionTypeFunction, FunctionCall: fc}
	w := NewSQLWriterForDialect(DuckDBDialect{})
	expr.WriteSql(w)
	got := w.String()
	if strings.Contains(got, "googlesqlite") {
		t.Fatalf("got %q", got)
	}
	if !strings.Contains(got, "DISTINCT") {
		t.Fatalf("got %q", got)
	}
}
