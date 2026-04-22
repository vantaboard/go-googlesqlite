package internal

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestTransformDuckDB_frozenClockCurrentTimestamp(t *testing.T) {
	coord := GetGlobalCoordinator()
	at := time.Unix(1700000000, 123456789).UTC()
	ctx := WithCurrentTime(context.Background(), at)

	fn := NewFunctionCallExpressionData("googlesqlite_current_timestamp")

	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)

	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	if !strings.Contains(got, "to_timestamp") || !strings.Contains(got, "1000000000.0") {
		t.Fatalf("expected to_timestamp(.../1000000000.0), got %q", got)
	}
}

func TestTransformDuckDB_instrTwoArgStrpos(t *testing.T) {
	coord := GetGlobalCoordinator()
	fn := NewFunctionCallExpressionData("googlesqlite_instr",
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("abc")}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("b")}},
	)
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := expr.String(); !strings.Contains(got, "strpos(") {
		t.Fatalf("got %q", got)
	}
}

func TestTransformDuckDB_betweenRangeAnd(t *testing.T) {
	coord := GetGlobalCoordinator()
	fn := NewFunctionCallExpressionData("googlesqlite_between",
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(5)}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(1)}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(10)}},
	)
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	if strings.Contains(got, "googlesqlite_between") {
		t.Fatalf("expected rewrite off googlesqlite_between, got %q", got)
	}
	if !strings.Contains(got, ">=") || !strings.Contains(got, "<=") || !strings.Contains(got, " AND ") {
		t.Fatalf("expected >= .. AND .. <=, got %q", got)
	}
}

func TestTransformDuckDB_parseJsonFirstArgOnly(t *testing.T) {
	coord := GetGlobalCoordinator()
	fn := NewFunctionCallExpressionData("googlesqlite_parse_json",
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("{}")}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("wide_padding_mode")}},
	)
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	if !strings.Contains(got, "CAST(") || !strings.Contains(got, " AS JSON)") || strings.Contains(got, "wide_padding_mode") {
		t.Fatalf("expected CAST(... AS JSON) with single arg, got %q", got)
	}
}
