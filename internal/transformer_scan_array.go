package internal

import (
	"fmt"

	core "github.com/vantaboard/go-googlesql-engine/internal/dialect/core"
	sqlexpr "github.com/vantaboard/go-googlesql-engine/internal/sqlexpr"
	"github.com/vantaboard/go-googlesql/types"
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

func duckDBUnnestWantsStructPayloadColumn(d Dialect, el *ExpressionData) bool {
	if d == nil || el == nil || d.ID() != core.IDDuckDB {
		return false
	}
	st := duckDBStructTypeFromExpressionData(*el)
	return st != nil && st.IsStruct()
}

// duckDBUnnestElementExprForStructPayload returns ExpressionData for DuckDB struct UNNEST lowering.
// When the element column is not struct-typed (e.g. wire STRING) but the resolved array expression
// is ARRAY<STRUCT<...>>, the analyzer still provides the struct element type on ArrayExpr; we merge
// that onto a copy of the element column so TRY_CAST/gs__ projection and get_struct_field match.
func duckDBUnnestElementExprForStructPayload(el *ExpressionData, arrayElType types.Type) *ExpressionData {
	if el == nil {
		return nil
	}
	if st := duckDBStructTypeFromExpressionData(*el); st != nil && st.IsStruct() {
		return el
	}
	if arrayElType == nil || !arrayElType.IsStruct() {
		return el
	}
	if el.Type != ExpressionTypeColumn || el.Column == nil {
		return el
	}
	out := *el
	col := *el.Column
	col.Type = arrayElType
	out.Column = &col
	return &out
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
// - Using DuckDB UNNEST(JSON[]) from wire body + DuckDBUnwireGooglesqlStringOperand per element (base64 layout)
// - Handling correlated arrays with proper JOIN semantics (INNER vs LEFT)
// - Managing element and offset column availability in the fragment context
// - Supporting both standalone UNNEST and UNNEST with input scans
//
// SQLite json_each provides 'key' (offset) and 'value' (element); DuckDB uses the same column names.
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
// When el/structColName are set for a STRUCT element on DuckDB, a single TRY_CAST struct column
// is also projected (alias structColName) so struct field reads reference a short column name.
func (t *ArrayScanTransformer) unnestExpansionFromItem(
	arrayExpr *SQLExpression,
	arrayAlias string,
	includeOffset bool,
	correlated bool,
	el *ExpressionData,
	d Dialect,
	structColName string,
) *FromItem {
	if !correlated {
		// (SELECT expr AS _arr) CROSS JOIN LATERAL UNNEST(JSON[] from wire) with unwrapped elements.
		src := NewSelectStatement()
		src.SelectList = []*SelectListItem{{Expression: arrayExpr, Alias: "_arr"}}
		srcFrom := NewSubqueryFromItem(src, "_unnest_src")

		arrCol := NewColumnExpression("_arr", "_unnest_src")
		splitList := sqlexpr.DuckDBGooglesqlWireArraySplitList(arrCol)

		if includeOffset {
			listSel := NewSelectStatement()
			listSel.SelectList = []*SelectListItem{{Expression: splitList, Alias: "lst"}}
			listSel.FromClause = srcFrom
			listFrom := NewSubqueryFromItem(listSel, "_ls")
			return duckDBJoinSplitListWithRange(listFrom, arrayAlias, el, d, structColName)
		}

		unnestFrom := &FromItem{
			Type:              FromItemTypeUnnest,
			UnnestExpr:        splitList,
			Alias:             "_je",
			UnnestColumnAlias: "value",
		}

		body := NewSelectStatement()
		body.FromClause = &FromItem{
			Type: FromItemTypeJoin,
			Join: &JoinClause{
				Type:           JoinTypeCross,
				Left:           srcFrom,
				Right:          unnestFrom,
				RightIsLateral: true,
			},
		}
		selList := []*SelectListItem{
			{Expression: sqlexpr.DuckDBUnwireGooglesqlStringOperand(NewColumnExpression("value", "_je")), Alias: "value"},
		}
		if structColName != "" && duckDBUnnestWantsStructPayloadColumn(d, el) {
			structPayload := duckDBStructFieldAccessBaseExpr(NewColumnExpression("value", "_je"), []ExpressionData{*el})
			if structPayload != nil {
				selList = append(selList, &SelectListItem{Expression: structPayload, Alias: structColName})
			}
		}
		body.SelectList = selList
		return NewSubqueryFromItem(body, arrayAlias)
	}

	if includeOffset {
		return duckDBCorrelatedUnnestWithOffset(arrayExpr, arrayAlias, el, d, structColName)
	}

	unnestInner := &FromItem{
		Type:              FromItemTypeUnnest,
		UnnestExpr:        sqlexpr.DuckDBGooglesqlWireArraySplitList(arrayExpr),
		Alias:             "_u",
		UnnestColumnAlias: "value",
	}
	wrap := NewSelectStatement()
	wrap.FromClause = unnestInner
	wrapSelect := []*SelectListItem{
		{Expression: sqlexpr.DuckDBUnwireGooglesqlStringOperand(NewColumnExpression("value", "_u")), Alias: "value"},
	}
	if structColName != "" && duckDBUnnestWantsStructPayloadColumn(d, el) {
		structPayload := duckDBStructFieldAccessBaseExpr(NewColumnExpression("value", "_u"), []ExpressionData{*el})
		if structPayload != nil {
			wrapSelect = append(wrapSelect, &SelectListItem{Expression: structPayload, Alias: structColName})
		}
	}
	wrap.SelectList = wrapSelect
	return NewSubqueryFromItem(wrap, arrayAlias)
}

// duckDBJoinSplitListWithRange expands lst (JSON[]) with 0-based key matching SQLite json_each.
// listFrom must expose column "lst" (table alias typically "_ls").
func duckDBJoinSplitListWithRange(listFrom *FromItem, arrayAlias string, el *ExpressionData, d Dialect, structColName string) *FromItem {
	lstRef := NewColumnExpression("lst", "_ls")
	lenLst := NewFunctionExpression("len", lstRef)
	stopExclusive := NewBinaryExpression(lenLst, "+", NewLiteralExpression("1"))
	rngFrom := &FromItem{
		Type: FromItemTypeTableFunction,
		TableFunction: &TableFunction{
			Name: "range",
			Arguments: []*SQLExpression{
				NewLiteralExpression("1"),
				stopExclusive,
				NewLiteralExpression("1"),
			},
		},
		Alias: "_gs",
	}
	join := &FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:           JoinTypeCross,
			Left:           listFrom,
			Right:          rngFrom,
			RightIsLateral: true,
		},
	}
	rngIdx := NewColumnExpression("range", "_gs")
	extracted := NewFunctionExpression("list_extract", lstRef, rngIdx)
	trimmedVal := sqlexpr.DuckDBUnwireGooglesqlStringOperand(extracted)
	keyExpr := NewBinaryExpression(NewSQLCastExpression(rngIdx, "BIGINT", true), "-", NewLiteralExpression("1"))

	body := NewSelectStatement()
	body.FromClause = join
	selList := []*SelectListItem{
		{Expression: trimmedVal, Alias: "value"},
		{Expression: keyExpr, Alias: "key"},
	}
	if structColName != "" && duckDBUnnestWantsStructPayloadColumn(d, el) {
		structPayload := duckDBStructFieldAccessBaseExpr(extracted, []ExpressionData{*el})
		if structPayload != nil {
			selList = append(selList, &SelectListItem{Expression: structPayload, Alias: structColName})
		}
	}
	body.SelectList = selList
	return NewSubqueryFromItem(body, arrayAlias)
}

// duckDBCorrelatedUnnestWithOffset expands a correlated wire-backed array with list_extract + range
// (json_each key/value semantics without TRY_CAST(... AS JSON)).
func duckDBCorrelatedUnnestWithOffset(arrayExpr *SQLExpression, arrayAlias string, el *ExpressionData, d Dialect, structColName string) *FromItem {
	listSel := NewSelectStatement()
	listSel.SelectList = []*SelectListItem{{Expression: sqlexpr.DuckDBGooglesqlWireArraySplitList(arrayExpr), Alias: "lst"}}
	listSel.FromClause = &FromItem{Type: FromItemTypeSingleRow}
	listFrom := NewSubqueryFromItem(listSel, "_ls")
	return duckDBJoinSplitListWithRange(listFrom, arrayAlias, el, d, structColName)
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

	elForStruct := duckDBUnnestElementExprForStructPayload(arrayData.ElementColumnExpr, arrayData.ArrayExprElementType)
	structColName := ""
	if elForStruct != nil && duckDBUnnestWantsStructPayloadColumn(ctx.Dialect(), elForStruct) {
		structColName = generateIDBasedAlias("gs", arrayData.ElementColumn.ID)
	}

	var expansion *FromItem
	if ctx.Dialect().ArrayUnnestUseLateralCorrelation() {
		expansion = t.unnestExpansionFromItem(arrayExpr, arrayAlias, includeOffset, correlated, elForStruct, ctx.Dialect(), structColName)
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
	elementColName := "value"
	elementColExpr := NewColumnExpression("value", expansion.Alias)
	if structColName != "" {
		elementColName = structColName
		elementColExpr = NewColumnExpression(structColName, expansion.Alias)
	}
	ctx.FragmentContext().AddAvailableColumn(arrayData.ElementColumn.ID, &ColumnInfo{
		ID:         arrayData.ElementColumn.ID,
		Name:       elementColName,
		Expression: elementColExpr,
	})
	ctx.FragmentContext().RegisterColumnScope(arrayData.ElementColumn.ID, expansion.Alias)

	if offsetColumn := arrayData.ArrayOffsetColumn; offsetColumn != nil {
		ctx.FragmentContext().AddAvailableColumn(offsetColumn.ID, &ColumnInfo{
			ID:         offsetColumn.ID,
			Name:       "key",
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
