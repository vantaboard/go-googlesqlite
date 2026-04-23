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
	if !strings.Contains(got, "from_base64(") {
		t.Fatalf("expected wire unwrap before TRY_CAST for VARCHAR-backed DATE columns, got %q", got)
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
					ColumnID:   1,
					ColumnName: "x",
					Type:       types.StringType(),
				},
			},
		},
	}
	colArg := ExpressionData{
		Type: ExpressionTypeColumn,
		Column: &ColumnRefData{
			ColumnID:   2,
			ColumnName: "y",
			Type:       types.StringType(),
		},
	}
	fn := NewFunctionCallExpressionData("googlesqlite_greater_or_equal", castArg, colArg)
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	fcx := tctx.FragmentContext()
	fcx.RegisterColumnScope(1, "t")
	fcx.AddAvailableColumn(1, &ColumnInfo{Name: "x"})
	fcx.RegisterColumnScope(2, "t")
	fcx.AddAvailableColumn(2, &ColumnInfo{Name: "y"})
	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	if !strings.Contains(got, "TRY_CAST(") || !strings.Contains(got, " AS DATE)") {
		t.Fatalf("expected TRY_CAST for mixed DATE cast vs VARCHAR column, got %q", got)
	}
	if !strings.Contains(got, "from_base64(") {
		t.Fatalf("expected wire unwrap on STRING column before TRY_CAST, got %q", got)
	}
}

func TestTransformDuckDB_concatUnwrapsWireBeforeNativeConcat(t *testing.T) {
	coord := GetGlobalCoordinator()
	fn := NewFunctionCallExpressionData("googlesqlite_concat",
		ExpressionData{Type: ExpressionTypeColumn, Column: &ColumnRefData{ColumnID: 10, ColumnName: "fn", Type: types.StringType()}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue(" ")}},
		ExpressionData{Type: ExpressionTypeColumn, Column: &ColumnRefData{ColumnID: 11, ColumnName: "ln", Type: types.StringType()}},
	)
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	fcx := tctx.FragmentContext()
	fcx.RegisterColumnScope(10, "t")
	fcx.AddAvailableColumn(10, &ColumnInfo{Name: "fn"})
	fcx.RegisterColumnScope(11, "t")
	fcx.AddAvailableColumn(11, &ColumnInfo{Name: "ln"})
	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	if !strings.Contains(got, "concat(") {
		t.Fatalf("expected concat(, got %q", got)
	}
	if !strings.Contains(got, "from_base64(") || !strings.Contains(got, "json_extract_string(") {
		t.Fatalf("expected wire unwrap (from_base64 + json_extract_string), got %q", got)
	}
	if strings.Contains(got, "googlesqlite_concat") {
		t.Fatalf("expected rewrite off googlesqlite_concat, got %q", got)
	}
}

func TestTransformDuckDB_simpleCaseUsesSearchedCaseWithUnwiredEquality(t *testing.T) {
	coord := GetGlobalCoordinator()
	gl := ExpressionData{
		Type:   ExpressionTypeColumn,
		Column: &ColumnRefData{ColumnID: 4, ColumnName: "GradeLevel__4", Type: types.StringType()},
	}
	fn := NewFunctionCallExpressionData("googlesqlite_case_with_value",
		gl,
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("01")}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("1st")}},
	)
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	fcx := tctx.FragmentContext()
	fcx.RegisterColumnScope(4, "t")
	fcx.AddAvailableColumn(4, &ColumnInfo{Name: "GradeLevel__4"})
	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	u := strings.ToUpper(strings.TrimSpace(got))
	if !strings.HasPrefix(u, "CASE WHEN ") {
		t.Fatalf("expected searched CASE WHEN ... for DuckDB wire-safe simple CASE, got %q", got)
	}
	if !strings.Contains(got, "from_base64(") || !strings.Contains(got, "=") {
		t.Fatalf("expected unwire + equality in CASE branches, got %q", got)
	}
	cfg2 := DefaultTransformConfig()
	cfg2.Dialect = SQLiteDialect{}
	tctx2 := NewQueryTransformFactory(cfg2, coord).CreateTransformContext(ctx)
	fcx2 := tctx2.FragmentContext()
	fcx2.RegisterColumnScope(4, "t")
	fcx2.AddAvailableColumn(4, &ColumnInfo{Name: "GradeLevel__4"})
	sqliteExpr, err := coord.TransformExpression(fn, tctx2)
	if err != nil {
		t.Fatal(err)
	}
	sqliteGot := sqliteExpr.String()
	if !strings.Contains(strings.ToUpper(sqliteGot), "CASE ") {
		t.Fatalf("sqlite still uses simple CASE form, got %q", sqliteGot)
	}
}

func TestTransformDuckDB_extractYearCastsDateColumnForDatePart(t *testing.T) {
	coord := GetGlobalCoordinator()
	colED := ExpressionData{
		Type:   ExpressionTypeColumn,
		Column: &ColumnRefData{ColumnID: 15, ColumnName: "StartDate__15", Type: types.DateType()},
	}
	yearED := ExpressionData{
		Type:    ExpressionTypeLiteral,
		Literal: &LiteralData{Value: StringValue("YEAR")},
	}
	fn := NewFunctionCallExpressionData("googlesqlite_extract", colED, yearED)
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	fcx := tctx.FragmentContext()
	fcx.RegisterColumnScope(15, "t")
	fcx.AddAvailableColumn(15, &ColumnInfo{Name: "StartDate__15"})
	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	if !strings.Contains(got, "date_part(") {
		t.Fatalf("expected date_part, got %q", got)
	}
	if !strings.Contains(got, "TRY_CAST(") || !strings.Contains(got, " AS DATE)") {
		t.Fatalf("expected TRY_CAST ... AS DATE for VARCHAR-backed DATE column, got %q", got)
	}
	if !strings.Contains(got, "from_base64(") {
		t.Fatalf("expected wire unwrap inside date_part temporal cast, got %q", got)
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
	if !strings.Contains(got, "BIGINT") {
		t.Fatalf("expected BIGINT casts for DuckDB range() overload, got %q", got)
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

func TestTransformDuckDB_makeArrayToListValue(t *testing.T) {
	coord := GetGlobalCoordinator()
	fn := NewFunctionCallExpressionData("googlesqlite_make_array",
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(1)}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(2)}},
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
	if !strings.Contains(got, "list_value(") || strings.Contains(got, "googlesqlite_make_array") {
		t.Fatalf("expected list_value rewrite, got %q", got)
	}
}

func TestTransformDuckDB_makeStructToStructLiteral(t *testing.T) {
	coord := GetGlobalCoordinator()
	fn := NewFunctionCallExpressionData("googlesqlite_make_struct",
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("eDate")}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: IntValue(42)}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("sy")}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("2025")}},
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
	if !strings.Contains(got, "{") || !strings.Contains(got, "'eDate':") || !strings.Contains(got, "'sy':") {
		t.Fatalf("expected DuckDB struct literal with eDate and sy, got %q", got)
	}
	if strings.Contains(got, "googlesqlite_make_struct") {
		t.Fatalf("expected rewrite off googlesqlite_make_struct, got %q", got)
	}
}

func TestTransformDuckDB_replaceUnwrapsWireBeforeReplace(t *testing.T) {
	coord := GetGlobalCoordinator()
	col := ExpressionData{
		Type:   ExpressionTypeColumn,
		Column: &ColumnRefData{ColumnID: 20, ColumnName: "GradeLevel__36", Type: types.StringType()},
	}
	fn := NewFunctionCallExpressionData("googlesqlite_replace",
		col,
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("KN")}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("0")}},
	)
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	fcx := tctx.FragmentContext()
	fcx.RegisterColumnScope(20, "t")
	fcx.AddAvailableColumn(20, &ColumnInfo{Name: "GradeLevel__36"})
	expr, err := coord.TransformExpression(fn, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	if !strings.Contains(got, "replace(") || strings.Contains(got, "googlesqlite_replace") {
		t.Fatalf("expected native replace(, got %q", got)
	}
	if !strings.Contains(got, "from_base64(") {
		t.Fatalf("expected wire unwrap on replace subject, got %q", got)
	}
}

func TestTransformDuckDB_nestedReplaceAndCastToInt64(t *testing.T) {
	coord := GetGlobalCoordinator()
	col := ExpressionData{
		Type:   ExpressionTypeColumn,
		Column: &ColumnRefData{ColumnID: 21, ColumnName: "g", Type: types.StringType()},
	}
	inner := NewFunctionCallExpressionData("googlesqlite_replace",
		col,
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("KN")}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("0")}},
	)
	outer := NewFunctionCallExpressionData("googlesqlite_replace",
		inner,
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("TK")}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("-1")}},
	)
	castED := ExpressionData{
		Type: ExpressionTypeCast,
		Cast: &CastData{
			Expression:      outer,
			FromType:        types.StringType(),
			ToType:          types.Int64Type(),
			SafeCast:        false,
			ReturnNullOnErr: false,
		},
	}
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	fcx := tctx.FragmentContext()
	fcx.RegisterColumnScope(21, "t")
	fcx.AddAvailableColumn(21, &ColumnInfo{Name: "g"})
	expr, err := coord.TransformExpression(castED, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	if !strings.Contains(got, "replace(") || !strings.Contains(strings.ToUpper(got), "BIGINT") {
		t.Fatalf("expected replace( + BIGINT cast, got %q", got)
	}
	if !strings.Contains(got, "from_base64(") {
		t.Fatalf("expected wire unwrap in pipeline, got %q", got)
	}
}

func TestTransformDuckDB_stringColumnCastToInt64UnwrapsWire(t *testing.T) {
	coord := GetGlobalCoordinator()
	col := ExpressionData{
		Type:   ExpressionTypeColumn,
		Column: &ColumnRefData{ColumnID: 22, ColumnName: "sid", Type: types.StringType()},
	}
	castED := ExpressionData{
		Type: ExpressionTypeCast,
		Cast: &CastData{
			Expression:      col,
			FromType:        types.StringType(),
			ToType:          types.Int64Type(),
			SafeCast:        false,
			ReturnNullOnErr: false,
		},
	}
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = DuckDBDialect{}
	tctx := NewQueryTransformFactory(cfg, coord).CreateTransformContext(ctx)
	fcx := tctx.FragmentContext()
	fcx.RegisterColumnScope(22, "t")
	fcx.AddAvailableColumn(22, &ColumnInfo{Name: "sid"})
	expr, err := coord.TransformExpression(castED, tctx)
	if err != nil {
		t.Fatal(err)
	}
	got := expr.String()
	if !strings.Contains(strings.ToUpper(got), "BIGINT") {
		t.Fatalf("expected CAST ... BIGINT, got %q", got)
	}
	if !strings.Contains(got, "from_base64(") {
		t.Fatalf("expected wire unwrap before numeric cast, got %q", got)
	}
}

func TestTransformDuckDB_replaceOnLiteralStillEmitsReplace(t *testing.T) {
	coord := GetGlobalCoordinator()
	fn := NewFunctionCallExpressionData("googlesqlite_replace",
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("01")}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("0")}},
		ExpressionData{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("")}},
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
	if !strings.Contains(got, "replace(") || strings.Contains(got, "googlesqlite_replace") {
		t.Fatalf("expected native replace(, got %q", got)
	}
	// String literals may be emitted as VARCHAR wire payloads in SQL text; unwrap still composes safely.
	if !strings.Contains(got, "coalesce(") {
		t.Fatalf("expected unwrap coalesce around replace subject, got %q", got)
	}
}
