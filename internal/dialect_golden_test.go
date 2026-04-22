package internal

import (
	"context"
	"strings"
	"testing"
)

func TestDialectGolden_emitStringBuiltinRenames(t *testing.T) {
	coord := GetGlobalCoordinator()
	ctx := context.Background()
	for _, tc := range []struct {
		fnName     string
		sqliteNeed string
		duckNeed   string
	}{
		{"googlesqlite_trim", "googlesqlite_trim(", "trim("},
		{"googlesqlite_concat", "googlesqlite_concat(", "concat("},
		{"googlesqlite_strpos", "googlesqlite_strpos(", "strpos("},
		{"googlesqlite_replace", "googlesqlite_replace(", "replace("},
	} {
		t.Run(tc.fnName, func(t *testing.T) {
			args := []ExpressionData{
				{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("a")}},
				{Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("b")}},
			}
			if tc.fnName == "googlesqlite_replace" {
				args = append(args, ExpressionData{
					Type: ExpressionTypeLiteral, Literal: &LiteralData{Value: StringValue("c")},
				})
			}
			fn := NewFunctionCallExpressionData(tc.fnName, args...)
			for _, pair := range []struct {
				name    string
				dialect Dialect
				substr  string
			}{
				{"sqlite", SQLiteDialect{}, tc.sqliteNeed},
				{"duckdb", DuckDBDialect{}, tc.duckNeed},
			} {
				t.Run(pair.name, func(t *testing.T) {
					cfg := DefaultTransformConfig()
					cfg.Dialect = pair.dialect
					factory := NewQueryTransformFactory(cfg, coord)
					tctx := factory.CreateTransformContext(ctx)
					expr, err := coord.TransformExpression(fn, tctx)
					if err != nil {
						t.Fatal(err)
					}
					got := expr.String()
					if !strings.Contains(got, pair.substr) {
						t.Fatalf("got %q, want substring %q", got, pair.substr)
					}
				})
			}
		})
	}
}

func TestDialectGolden_emitFunctionNameLength(t *testing.T) {
	coord := GetGlobalCoordinator()
	// String literal argument for LENGTH(...)
	lit := ExpressionData{
		Type: ExpressionTypeLiteral,
		Literal: &LiteralData{
			Value: StringValue("abc"),
		},
	}
	fn := NewFunctionCallExpressionData("googlesqlite_length", lit)

	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		dialect    Dialect
		wantSubstr string
	}{
		{"sqlite", SQLiteDialect{}, "googlesqlite_length("},
		{"duckdb", DuckDBDialect{}, "length("},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultTransformConfig()
			cfg.Dialect = tc.dialect
			factory := NewQueryTransformFactory(cfg, coord)
			tctx := factory.CreateTransformContext(ctx)
			expr, err := coord.TransformExpression(fn, tctx)
			if err != nil {
				t.Fatal(err)
			}
			got := expr.String()
			if !strings.Contains(got, tc.wantSubstr) {
				t.Fatalf("got %q, want substring %q", got, tc.wantSubstr)
			}
		})
	}
}

func TestDialectGolden_windowPartitionCollation(t *testing.T) {
	coord := GetGlobalCoordinator()
	colData := ExpressionData{
		Type: ExpressionTypeColumn,
		Column: &ColumnRefData{
			ColumnID:   7,
			ColumnName: "part_key",
		},
	}
	win := ExpressionData{
		Type: ExpressionTypeFunction,
		Function: &FunctionCallData{
			Name:      "row_number",
			Arguments: []ExpressionData{},
			WindowSpec: &WindowSpecificationData{
				PartitionBy: []*ExpressionData{&colData},
			},
		},
	}

	ctx := context.Background()
	for _, tc := range []struct {
		name         string
		dialect      Dialect
		wantCollate  bool
		collateToken string
	}{
		{"sqlite", SQLiteDialect{}, true, "COLLATE googlesqlite_collate"},
		{"duckdb", DuckDBDialect{}, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultTransformConfig()
			cfg.Dialect = tc.dialect
			factory := NewQueryTransformFactory(cfg, coord)
			tctx := factory.CreateTransformContext(ctx)
			fc := tctx.FragmentContext()
			fc.RegisterColumnScope(7, "t")
			fc.AddAvailableColumn(7, &ColumnInfo{Name: "part_key"})

			expr, err := coord.TransformExpression(win, tctx)
			if err != nil {
				t.Fatal(err)
			}
			got := expr.String()
			has := strings.Contains(got, tc.collateToken)
			if tc.wantCollate && !has {
				t.Fatalf("got %q, want substring %q", got, tc.collateToken)
			}
			if !tc.wantCollate && strings.Contains(got, "googlesqlite_collate") {
				t.Fatalf("unexpected collation in duckdb SQL: %q", got)
			}
		})
	}
}

func TestDialectGolden_orderByCollation(t *testing.T) {
	tr := NewOrderByScanTransformer(GetGlobalCoordinator())
	item := &OrderByItemData{
		Expression: ExpressionData{
			Type: ExpressionTypeColumn,
			Column: &ColumnRefData{
				ColumnID:   3,
				ColumnName: "sortcol",
			},
		},
		IsDescending: false,
		NullOrder:    0,
	}
	ctx := context.Background()
	cfg := DefaultTransformConfig()
	cfg.Dialect = SQLiteDialect{}
	tctx := NewQueryTransformFactory(cfg, tr.coordinator).CreateTransformContext(ctx)
	fcx := tctx.FragmentContext()
	fcx.RegisterColumnScope(3, "t")
	fcx.AddAvailableColumn(3, &ColumnInfo{Name: "sortcol"})

	sqlExpr, err := tr.coordinator.TransformExpression(item.Expression, tctx)
	if err != nil {
		t.Fatal(err)
	}
	items, err := tr.createOrderByItems(sqlExpr, item, tctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("len=%d", len(items))
	}
	if !strings.Contains(items[0].Expression.String(), "COLLATE googlesqlite_collate") {
		t.Fatalf("sqlite: %q", items[0].Expression.String())
	}

	cfg2 := DefaultTransformConfig()
	cfg2.Dialect = DuckDBDialect{}
	tctx2 := NewQueryTransformFactory(cfg2, tr.coordinator).CreateTransformContext(ctx)
	fcx2 := tctx2.FragmentContext()
	fcx2.RegisterColumnScope(3, "t")
	fcx2.AddAvailableColumn(3, &ColumnInfo{Name: "sortcol"})
	sqlExpr2, err := tr.coordinator.TransformExpression(item.Expression, tctx2)
	if err != nil {
		t.Fatal(err)
	}
	items2, err := tr.createOrderByItems(sqlExpr2, item, tctx2)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(items2[0].Expression.String(), "googlesqlite_collate") {
		t.Fatalf("duckdb should omit collation: %q", items2[0].Expression.String())
	}
}
