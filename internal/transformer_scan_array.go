package internal

import (
	"fmt"

	sqlexpr "github.com/vantaboard/go-googlesql-engine/internal/sqlexpr"
)

// fromItemPrimaryAlias returns the alias whose columns are visible as the unnest join's outer row.
func fromItemPrimaryAlias(f *FromItem) string {
	if f == nil {
		return ""
	}
	switch f.Type {
	case FromItemTypeSubquery, FromItemTypeTable, FromItemTypeTableFunction, FromItemTypeUnnest, FromItemTypeWithRef:
		return f.Alias
	case FromItemTypeJoin:
		if f.Join != nil && f.Join.Left != nil {
			return fromItemPrimaryAlias(f.Join.Left)
		}
	}
	return ""
}

// qualifyPlainColumnWithTable fills in TableAlias on an unqualified column reference (copy-on-write).
// Laterally joined json_each(...) must read the wire column from the outer row alias.
func qualifyPlainColumnWithTable(e *SQLExpression, tableAlias string) *SQLExpression {
	if e == nil || tableAlias == "" {
		return e
	}
	if e.Type != ExpressionTypeColumn || e.TableAlias != "" {
		return e
	}
	out := *e
	out.TableAlias = tableAlias
	return &out
}

// ArrayScanTransformer handles array scan (UNNEST operations) transformations from GoogleSQL to SQLite.
//
// In BigQuery/GoogleSQL, array scans represent UNNEST operations that flatten array values
// into individual rows. This enables queries to iterate over array elements as if they
// were rows in a table, with optional position/offset information and join conditions.
//
// The transformer converts GoogleSQL ArrayScan nodes by:
// - Transforming array expressions through the coordinator
// - Using SQLite's json_each() table function with googlesqlengine_decode_array() for UNNEST
// - Using DuckDB json_each() over wire-decoded ARRAY JSON (VARCHAR storage) with JOIN LATERAL when correlated
// - Handling correlated arrays with proper JOIN semantics (INNER vs LEFT)
// - Managing element and offset column availability in the fragment context
// - Supporting both standalone UNNEST and UNNEST with input scans
//
// The json_each() approach provides 'key' (offset) and 'value' (element) columns
// that map to GoogleSQL's array element and offset semantics in SQLite.
type ArrayScanTransformer struct {
	coordinator Coordinator
}

// NewArrayScanTransformer creates a new ArrayScanTransformer
func NewArrayScanTransformer(coordinator Coordinator) *ArrayScanTransformer {
	return &ArrayScanTransformer{
		coordinator: coordinator,
	}
}

// unnestExpansionFromItem builds a FROM item that expands an array into rows with columns
// `value` (element) and optionally `key` (0-based offset, matching SQLite json_each).
func (t *ArrayScanTransformer) unnestExpansionFromItem(
	arrayExpr *SQLExpression,
	arrayAlias string,
	includeOffset bool,
	correlated bool,
) *FromItem {
	if !correlated {
		// (SELECT expr AS _arr) CROSS JOIN LATERAL json_each(decoded array JSON).
		src := NewSelectStatement()
		src.SelectList = []*SelectListItem{{Expression: arrayExpr, Alias: "_arr"}}
		srcFrom := NewSubqueryFromItem(src, "_unnest_src")

		arrCol := NewColumnExpression("_arr", "_unnest_src")
		jeFrom := &FromItem{
			Type: FromItemTypeTableFunction,
			TableFunction: &TableFunction{
				Name:      "json_each",
				Arguments: []*SQLExpression{sqlexpr.DuckDBGooglesqlWireArrayJSONForJsonEach(arrCol)},
			},
			Alias: "_je",
		}

		body := NewSelectStatement()
		body.FromClause = &FromItem{
			Type: FromItemTypeJoin,
			Join: &JoinClause{
				Type:           JoinTypeCross,
				Left:           srcFrom,
				Right:          jeFrom,
				RightIsLateral: true,
			},
		}
		items := []*SelectListItem{
			{Expression: NewColumnExpression("value", "_je"), Alias: "value"},
		}
		if includeOffset {
			keyRaw := NewColumnExpression("key", "_je")
			items = append(items, &SelectListItem{
				Expression: NewSQLCastExpression(NewFunctionExpression("try", keyRaw), "BIGINT", true),
				Alias:      "key",
			})
		}
		body.SelectList = items
		return NewSubqueryFromItem(body, arrayAlias)
	}

	if includeOffset {
		return duckDBCorrelatedUnnestWithOffset(arrayExpr, arrayAlias)
	}
	return &FromItem{
		Type: FromItemTypeTableFunction,
		TableFunction: &TableFunction{
			Name:      "json_each",
			Arguments: []*SQLExpression{sqlexpr.DuckDBGooglesqlWireArrayJSONForJsonEach(arrayExpr)},
		},
		Alias: arrayAlias,
	}
}

// duckDBCorrelatedUnnestWithOffset expands a correlated wire-backed array with json_each key/value
// (same semantics as SQLite json_each).
func duckDBCorrelatedUnnestWithOffset(arrayExpr *SQLExpression, arrayAlias string) *FromItem {
	body := NewSelectStatement()
	body.FromClause = &FromItem{
		Type: FromItemTypeTableFunction,
		TableFunction: &TableFunction{
			Name:      "json_each",
			Arguments: []*SQLExpression{sqlexpr.DuckDBGooglesqlWireArrayJSONForJsonEach(arrayExpr)},
		},
		Alias: "_unnest_json",
	}
	keyRaw := NewColumnExpression("key", "_unnest_json")
	items := []*SelectListItem{
		{Expression: NewColumnExpression("value", "_unnest_json"), Alias: "value"},
		{
			Expression: NewSQLCastExpression(NewFunctionExpression("try", keyRaw), "BIGINT", true),
			Alias:      "key",
		},
	}
	body.SelectList = items
	return NewSubqueryFromItem(body, arrayAlias)
}

// Transform converts ArrayScanData to a FromItem representing UNNEST operation
func (t *ArrayScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeArray || data.ArrayScan == nil {
		return nil, fmt.Errorf("expected array scan data, got type %v", data.Type)
	}

	arrayData := data.ArrayScan

	var innerFromItem *FromItem
	if arrayData.InputScan != nil {
		// Handle input scan for correlated arrays
		// Transform the input scan
		inputFromItem, err := t.coordinator.TransformScan(*arrayData.InputScan, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform input scan for array: %w", err)
		}
		innerFromItem = inputFromItem
	}

	// Transform the array expression to UNNEST
	arrayExpr, err := t.coordinator.TransformExpression(arrayData.ArrayExpr, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform array expression: %w", err)
	}

	correlated := arrayData.InputScan != nil
	if correlated && ctx.Dialect().ArrayUnnestUseLateralCorrelation() && innerFromItem != nil {
		if ta := fromItemPrimaryAlias(innerFromItem); ta != "" {
			arrayExpr = qualifyPlainColumnWithTable(arrayExpr, ta)
		}
	}

	arrayAlias := fmt.Sprintf("$array_%s", ctx.FragmentContext().GetID())
	includeOffset := arrayData.ArrayOffsetColumn != nil

	var expansion *FromItem
	if ctx.Dialect().ArrayUnnestUseLateralCorrelation() {
		expansion = t.unnestExpansionFromItem(arrayExpr, arrayAlias, includeOffset, correlated)
	} else {
		expansion = &FromItem{
			Type: FromItemTypeTableFunction,
			TableFunction: &TableFunction{
				Name: "json_each",
				Arguments: []*SQLExpression{
					NewFunctionExpression(
						"googlesqlengine_decode_array",
						arrayExpr,
					),
				},
			},
			Alias: arrayAlias,
		}
	}

	// The element / key columns must be made available prior to the JoinExpr being transformed
	// since they reference return values from the unnest expansion which do not exist in GoogleSQL
	ctx.FragmentContext().AddAvailableColumn(arrayData.ElementColumn.ID, &ColumnInfo{
		ID:   arrayData.ElementColumn.ID,
		Name: "value",
		Expression: NewColumnExpression("value", expansion.Alias),
	})
	ctx.FragmentContext().RegisterColumnScope(arrayData.ElementColumn.ID, expansion.Alias)

	if offsetColumn := arrayData.ArrayOffsetColumn; offsetColumn != nil {
		ctx.FragmentContext().AddAvailableColumn(offsetColumn.ID, &ColumnInfo{
			ID:   offsetColumn.ID,
			Name: "key",
			Expression: NewColumnExpression("key", expansion.Alias),
		})
		ctx.FragmentContext().RegisterColumnScope(offsetColumn.ID, expansion.Alias)
	}

	// Create a subquery that selects the proper column names
	unnestSelect := NewSelectStatement()

	// Always select 'value' as the element column
	unnestSelect.SelectList = []*SelectListItem{}
	unnestSelect.FromClause = expansion

	for _, col := range data.ColumnList {
		name, table := ctx.FragmentContext().GetQualifiedColumnRef(col.ID)
		unnestSelect.SelectList = append(unnestSelect.SelectList, &SelectListItem{
			Expression: NewColumnExpression(name, table),
			Alias:      generateIDBasedAlias(col.Name, col.ID),
		})
	}

	// If there's no InputScan() we can return the select directly
	if arrayData.InputScan == nil {
		return NewSubqueryFromItem(unnestSelect, ""), nil
	}

	// Determine join type based on IsOuter flag
	var joinType JoinType
	if arrayData.IsOuter {
		joinType = JoinTypeLeft
	} else {
		joinType = JoinTypeInner
	}

	// Handle join condition if present
	var joinCondition *SQLExpression
	if arrayData.JoinExpr != nil {
		conditionExpr, err := t.coordinator.TransformExpression(*arrayData.JoinExpr, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform join expression: %w", err)
		}
		joinCondition = conditionExpr
	} else {
		// If there is no join expression, use CROSS JOIN for inner joins
		// For outer joins (LEFT JOIN UNNEST), we need an explicit ON condition
		// to preserve rows with empty arrays
		if arrayData.IsOuter {
			joinCondition = NewLiteralExpression("true")
		} else {
			joinType = JoinTypeCross
		}
	}

	useLateral := ctx.Dialect().ArrayUnnestUseLateralCorrelation()

	// Set the FROM clause to be a JOIN between input and UNNEST
	unnestSelect.FromClause = &FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:           joinType,
			Left:           innerFromItem,
			Right:          expansion,
			Condition:      joinCondition,
			RightIsLateral: useLateral,
		},
	}

	return NewSubqueryFromItem(unnestSelect, ""), nil
}
