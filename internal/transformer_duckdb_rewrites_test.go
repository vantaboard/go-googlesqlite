package internal

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/vantaboard/go-googlesql/types"
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

func TestTransformDuckDB_errorBuiltinRename(t *testing.T) {
	coord := GetGlobalCoordinator()
	fn := NewFunctionCallExpressionData("googlesqlite_error",
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("boom")}},
	)
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := expr.String(); !strings.Contains(got, "error(") || strings.Contains(got, "googlesqlite_error") {
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

func TestDuckDBTemporalComparisonCoercion(t *testing.T) {
	castDate := NewSQLCastExpression(NewColumnExpression("enrollmentDate"), "DATE", false)
	startCol := NewColumnExpression("startDate")
	raw := NewBinaryExpression(castDate, ">=", startCol)
	got := duckDBCoerceTemporalComparisons(raw).String()
	if !strings.Contains(got, "TRY_CAST(") || !strings.Contains(got, " AS DATE)") {
		t.Fatalf("expected TRY_CAST on VARCHAR side for DATE comparison, got %q", got)
	}
}

func TestDuckDBTemporalComparisonCoercionExprDataTwoDateColumns(t *testing.T) {
	left := NewColumnExpression("EnrollmentDate__54")
	right := NewColumnExpression("StartDate__10")
	ld := ExpressionData{
		Type:   ExpressionTypeColumn,
		Column: &ColumnRefData{ColumnName: "EnrollmentDate__54", Type: types.DateType()},
	}
	rd := ExpressionData{
		Type:   ExpressionTypeColumn,
		Column: &ColumnRefData{ColumnName: "StartDate__10", Type: types.DateType()},
	}
	l, r := duckDBApplyTemporalComparisonCoercionWithExprData(left, right, ld, rd, ">=")
	got := NewBinaryExpression(l, ">=", r).String()
	if !strings.Contains(got, "TRY_CAST(") || strings.Count(got, "TRY_CAST(") < 2 {
		t.Fatalf("expected TRY_CAST on both DATE-typed column refs, got %q", got)
	}
}

func TestTransformDuckDB_greaterOrEqualTemporalCoercion(t *testing.T) {
	coord := GetGlobalCoordinator()
	// Emulate CAST(col AS DATE) vs a STRING-typed column ref (primitive for optimizer).
	castArg := ExpressionData{
		Type: ExpressionTypeCast,
		Cast: &CastData{
			FromType:        types.StringType(),
			ToType:          types.DateType(),
			ReturnNullOnErr: false,
			SafeCast:        false,
			Expression: ExpressionData{
				Type: ExpressionTypeColumn,
				Column: &ColumnRefData{
					ColumnName: "x",
					Type:       types.StringType(),
				},
			},
		},
	}
	colArg := ExpressionData{
		Type: ExpressionTypeColumn,
		Column: &ColumnRefData{
			ColumnName: "y",
			Type:       types.StringType(),
		},
	}
	fn := NewFunctionCallExpressionData("googlesqlite_greater_or_equal", castArg, colArg)
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	if !strings.Contains(got, "TRY_CAST(") || !strings.Contains(got, " AS DATE)") {
		t.Fatalf("expected TRY_CAST for mixed DATE cast vs VARCHAR column, got %q", got)
	}
}

func TestTransformDuckDB_generateArrayToRange(t *testing.T) {
	coord := GetGlobalCoordinator()
	fn := NewFunctionCallExpressionData("googlesqlite_generate_array",
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(2003)}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(2027)}},
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
	if strings.Contains(got, "googlesqlite_generate_array") {
		t.Fatalf("expected rewrite off googlesqlite_generate_array, got %q", got)
	}
	if !strings.Contains(got, "range(") || !strings.Contains(strings.ToUpper(got), "CASE") {
		t.Fatalf("expected CASE + range(...), got %q", got)
	}
}

func TestTransformDuckDB_generateArrayThreeArgToRange(t *testing.T) {
	coord := GetGlobalCoordinator()
	fn := NewFunctionCallExpressionData("googlesqlite_generate_array",
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(0)}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(10)}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(3)}},
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
	if strings.Contains(got, "googlesqlite_generate_array") {
		t.Fatalf("expected rewrite, got %q", got)
	}
	if !strings.Contains(got, "range(") {
		t.Fatalf("expected range(...), got %q", got)
	}
}
