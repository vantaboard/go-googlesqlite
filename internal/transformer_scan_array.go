package internal

import (
	"fmt"
)

// ArrayScanTransformer handles array scan (UNNEST operations) transformations from GoogleSQL to SQLite.
//
// In BigQuery/GoogleSQL, array scans represent UNNEST operations that flatten array values
// into individual rows. This enables queries to iterate over array elements as if they
// were rows in a table, with optional position/offset information and join conditions.
//
// The transformer converts GoogleSQL ArrayScan nodes by:
// - Transforming array expressions through the coordinator
// - Using SQLite's json_each() table function with googlesqlite_decode_array() for UNNEST
// - Using DuckDB unnest() + generate_subscripts() (0-based offset) with JOIN LATERAL when correlated
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
		// Single-row source: (SELECT expr AS _arr) then unnest in outer SELECT.
		src := NewSelectStatement()
		src.SelectList = []*SelectListItem{{Expression: arrayExpr, Alias: "_arr"}}
		srcFrom := NewSubqueryFromItem(src, "_unnest_src")

		body := NewSelectStatement()
		body.FromClause = srcFrom
		body.SelectList = duckDBUnnestSelectList(arrayExpr, includeOffset, true)
		return NewSubqueryFromItem(body, arrayAlias)
	}

	body := NewSelectStatement()
	body.SelectList = duckDBUnnestSelectList(arrayExpr, includeOffset, false)
	return NewSubqueryFromItem(body, arrayAlias)
}

// duckDBUnnestSelectList builds SELECT list for DuckDB unnest; when useArrColumn is true,
// unnest(generate_subscripts) read from _arr in _unnest_src.
func duckDBUnnestSelectList(arrayExpr *SQLExpression, includeOffset bool, useArrColumn bool) []*SelectListItem {
	var unnestArg *SQLExpression
	if useArrColumn {
		unnestArg = NewColumnExpression("_arr", "_unnest_src")
	} else {
		unnestArg = arrayExpr
	}

	unnestCol := NewFunctionExpression("unnest", unnestArg)
	items := []*SelectListItem{{Expression: unnestCol, Alias: "value"}}
	if includeOffset {
		gs := NewFunctionExpression("generate_subscripts", unnestArg, NewLiteralExpression("1"))
		keyExpr := NewBinaryExpression(gs, "-", NewLiteralExpression("1"))
		items = append(items, &SelectListItem{Expression: keyExpr, Alias: "key"})
	}
	return items
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

	arrayAlias := fmt.Sprintf("$array_%s", ctx.FragmentContext().GetID())
	includeOffset := arrayData.ArrayOffsetColumn != nil

	var expansion *FromItem
	if ctx.Dialect().ArrayUnnestUseLateralCorrelation() {
		correlated := arrayData.InputScan != nil
		expansion = t.unnestExpansionFromItem(arrayExpr, arrayAlias, includeOffset, correlated)
	} else {
		expansion = &FromItem{
			Type: FromItemTypeTableFunction,
			TableFunction: &TableFunction{
				Name: "json_each",
				Arguments: []*SQLExpression{
					NewFunctionExpression(
						"googlesqlite_decode_array",
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
