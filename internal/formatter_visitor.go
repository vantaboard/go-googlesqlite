package internal

import (
	"context"
	"fmt"
	"github.com/goccy/go-json"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

// AST Visitor with fragment storage

type SQLBuilderVisitor struct {
	context         context.Context
	fragmentContext *FragmentContext
}

func NewSQLBuilderVisitor(ctx context.Context) *SQLBuilderVisitor {
	return &SQLBuilderVisitor{
		context:         ctx,
		fragmentContext: NewFragmentContext(),
	}
}

// Main visitor entry points

func (v *SQLBuilderVisitor) VisitQuery(node *ast.QueryStmtNode) (SQLFragment, error) {
	scan, err := v.VisitScan(node.Query())
	if err != nil {
		return nil, fmt.Errorf("failed to visit query: %w", err)
	}

	selectStatement := NewSelectStatement()
	selectStatement.FromClause = scan

	for _, column := range node.OutputColumnList() {
		expr, err := v.VisitExpression(column)
		if err != nil {
			return nil, err
		}
		item := &SelectListItem{
			Expression: expr.(*SQLExpression),
			Alias:      column.Name(),
		}
		selectStatement.SelectList = append(selectStatement.SelectList, item)
	}

	return selectStatement, nil
}

type ColumnListProvider interface {
	ColumnList() []*ast.Column
}

// VisitScan always returns SQLFragment FromItem
func (v *SQLBuilderVisitor) VisitScan(scan ast.Node) (*FromItem, error) {
	v.fragmentContext.PushScope(fmt.Sprintf("Scan(%s)", scan.Kind()))

	var fragment SQLFragment
	var err error
	switch s := scan.(type) {
	case *ast.TableScanNode:
		fragment, err = v.VisitTableScan(s)
	case *ast.ProjectScanNode:
		fragment, err = v.VisitProjectScan(s)
	case *ast.JoinScanNode:
		fragment, err = v.VisitJoinScan(s)
	case *ast.ArrayScanNode:
		fragment, err = v.VisitArrayScan(s)
	case *ast.SingleRowScanNode:
		fragment, err = v.VisitSingleRowScanNode(s)
	case *ast.WithScanNode:
		fragment, err = v.VisitWithScanNode(s)
	case *ast.WithRefScanNode:
		fragment, err = v.VisitWithRefScanNode(s)
	case *ast.SetOperationScanNode:
		fragment, err = v.VisitSetOperationScanNode(s)
	case *ast.FilterScanNode:
		fragment, err = v.VisitFilterScanNode(s)
	case *ast.OrderByScanNode:
		fragment, err = v.VisitOrderByScanNode(s)
	case *ast.LimitOffsetScanNode:
		fragment, err = v.VisitLimitOffsetScanNode(s)
	case *ast.AnalyticScanNode:
		fragment, err = v.VisitAnalyticScanNode(s)
	case *ast.AggregateScanNode:
		fragment, err = v.VisitAggregateScanNode(s)
	default:
		return nil, fmt.Errorf("unsupported scan type: %T", scan)
	}

	if provider, ok := scan.(ColumnListProvider); ok {
		for _, col := range provider.ColumnList() {
			v.fragmentContext.AddAvailableColumn(col, &ColumnInfo{
				Type: col.Type().Kind().String(),
			})
		}
	}

	if err != nil {
		return nil, err
	}

	// Finalize scope
	switch s := fragment.(type) {
	case *SelectStatement, *FromItem, nil:
		// SingleRowScanNode is the only time we should expect a nil fragment
		if _, ok := scan.(*ast.SingleRowScanNode); !ok && s == nil {
			return nil, fmt.Errorf("unexpected scan expression: %T", scan)
		}
		return v.finalizeFromItem(s), nil
	default:
		return nil, fmt.Errorf("unexpected scan expression: %T", scan)
	}
}

func (v *SQLBuilderVisitor) VisitExpression(expr ast.Node) (SQLFragment, error) {
	switch e := expr.(type) {
	case *ast.LiteralNode:
		return v.VisitLiteralNode(e)
	case *ast.MakeStructNode:
		return v.VisitMakeStructNode(e)
	case *ast.GetJsonFieldNode:
		return v.VisitGetJsonFieldNode(e)
	case *ast.GetStructFieldNode:
		return v.VisitGetStructFieldNode(e)
	case *ast.FunctionCallNode:
		return v.VisitFunctionCallNode(e)
	case *ast.CastNode:
		return v.VisitCastNode(e)
	case *ast.ColumnRefNode:
		return v.VisitColumnRefNode(e)
	case *ast.SubqueryExprNode:
		return v.VisitSubqueryExpressionNode(e)
	case *ast.AnalyticFunctionCallNode:
		return v.VisitAnalyticFunctionCallNode(e)
	case *ast.AggregateFunctionCallNode:
		return v.VisitAggregateFunctionCallNode(e)
	case *ast.ComputedColumnNode:
		return v.VisitComputedColumnNode(e)
	case *ast.OutputColumnNode:
		return v.VisitOutputColumnNode(e)
	case *ast.ParameterNode:
		return v.VisitParameterNode(e)
	case *ast.ArgumentRefNode:
		return v.VisitArgumentRefNode(e)

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// Example visitor methods showing fragment storage patterns

func (v *SQLBuilderVisitor) VisitTableScan(node *ast.TableScanNode) (SQLFragment, error) {
	nodeID := NodeID(fmt.Sprintf("table_%p", node))

	// Always generate a table alias to help with column disambiguation
	tableAlias := v.fragmentContext.aliasGenerator.GenerateTableAlias()

	fromItem := NewTableFromItem(node.Table().Name(), tableAlias)

	// Create column information for output
	outputColumns := make([]*ColumnInfo, 0)
	for _, col := range node.ColumnList() {
		columnInfo := &ColumnInfo{
			Type:       col.Type().Kind().String(),
			TableAlias: tableAlias,
			Source:     nodeID,
		}
		outputColumns = append(outputColumns, columnInfo)

		// Make columns available in current scope
		// Store by column name for simple lookup
		v.fragmentContext.AddAvailableColumn(col, columnInfo)
	}

	// Store fragment with metadata
	metadata := &FragmentMetadata{
		NodeType:      "TableScan",
		OutputColumns: outputColumns,
		TableAliases:  []string{tableAlias},
	}

	v.fragmentContext.StoreFragment(nodeID, fromItem, metadata)
	return fromItem, nil
}

func (v *SQLBuilderVisitor) VisitProjectScan(node *ast.ProjectScanNode) (SQLFragment, error) {
	from, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, fmt.Errorf("failed to visit input scan: %w", err)
	}

	// Produce expressions before visiting the input scan so they are available in context
	for i, computedCol := range node.ExprList() {
		// Visit expression to get fragment
		exprFragment, err := v.VisitExpression(computedCol.Expr())
		if err != nil {
			return nil, fmt.Errorf("failed to visit expression %d: %w", i, err)
		}

		v.fragmentContext.AddAvailableColumn(computedCol.Column(), &ColumnInfo{
			Expression: exprFragment.(*SQLExpression),
			Name:       computedCol.Column().Name(),
		})
	}
	// Create SELECT statement
	stmt := NewSelectStatement()
	stmt.FromClause = from

	// Build SELECT list
	for _, col := range node.ColumnList() {
		stmt.SelectList = append(stmt.SelectList, &SelectListItem{
			Expression: v.fragmentContext.GetColumnExpression(col),
			Alias:      fmt.Sprintf("col%d", col.ColumnID()),
		})
	}

	// TODO: Maybe delete fragment metadata approach
	// Store fragment with metadata
	//nodeID := NodeID(fmt.Sprintf("project_%p", node))
	//metadata := &FragmentMetadata{
	//	NodeType:      "ProjectScan",
	//	OutputColumns: outputColumns,
	//	IsOrdered:     node.IsOrdered(),
	//	Dependencies:  []NodeID{NodeID(fmt.Sprintf("%p", node.InputScan()))},
	//}
	//
	//v.fragmentContext.StoreFragment(nodeID, stmt, metadata)

	return stmt, nil
}

func (v *SQLBuilderVisitor) VisitJoinScan(node *ast.JoinScanNode) (SQLFragment, error) {
	nodeID := NodeID(fmt.Sprintf("join_%p", node))

	// Visit left and right inputs
	// Push new scope for LeftScan
	leftFromItem, err := v.VisitScan(node.LeftScan())
	if err != nil {
		return nil, fmt.Errorf("failed to visit left scan: %w", err)
	}

	rightFromItem, err := v.VisitScan(node.RightScan())
	if err != nil {
		return nil, fmt.Errorf("failed to visit right scan: %w", err)
	}

	// Build join condition AFTER visiting left and right scans so column aliases are available
	var joinCondition *SQLExpression
	if node.JoinExpr() != nil {
		conditionFragment, err := v.VisitExpression(node.JoinExpr())
		if err != nil {
			return nil, fmt.Errorf("failed to visit join condition: %w", err)
		}
		joinCondition = conditionFragment.(*SQLExpression)
	}

	selectStatement := NewSelectStatement()
	// Create join fragment
	joinType := convertJoinType(node.JoinType())
	selectStatement.FromClause = &FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:      joinType,
			Left:      leftFromItem,
			Right:     rightFromItem,
			Condition: joinCondition,
		},
	}

	// Build SELECT list with properly qualified column references
	outputColumns := make([]*ColumnInfo, 0)
	for _, col := range node.ColumnList() {
		selectStatement.SelectList = append(selectStatement.SelectList, &SelectListItem{
			Expression: v.fragmentContext.GetColumnExpression(col),
		})

		// Create output column info for this join result
		joinColumnInfo := &ColumnInfo{
			Type:   col.Type().Kind().String(),
			Source: nodeID,
		}
		outputColumns = append(outputColumns, joinColumnInfo)

		// Make column available for parent scopes (without table alias since it's now from the join)
		v.fragmentContext.AddAvailableColumn(col, joinColumnInfo)
	}

	// Store fragment metadata
	metadata := &FragmentMetadata{
		NodeType:      "JoinScan",
		OutputColumns: outputColumns,
		Dependencies: []NodeID{
			NodeID(fmt.Sprintf("%p", node.LeftScan())),
			NodeID(fmt.Sprintf("%p", node.RightScan())),
		},
	}

	v.fragmentContext.StoreFragment(nodeID, selectStatement, metadata)
	return selectStatement, nil
}

// Helper methods

func (v *SQLBuilderVisitor) finalizeFromItem(fragment SQLFragment) *FromItem {
	switch f := fragment.(type) {
	case *FromItem:
		v.fragmentContext.PopScope(f.Alias)
		return f
	case *SelectStatement:
		// Wrap complex query in subquery
		alias := v.fragmentContext.aliasGenerator.GenerateSubqueryAlias()
		v.fragmentContext.PopScope(alias)
		return NewSubqueryFromItem(f, alias)
	case nil:
		v.fragmentContext.PopScope("")
		return nil
	default:
		panic(fmt.Sprintf("unexpected fragment type: %T", fragment))
	}
}

// Adds ColumnInfo to context and generates alias
func (v *SQLBuilderVisitor) finalizeSelectStatement(fragment *SelectStatement, columns []*ast.Column) *FromItem {
	alias := v.fragmentContext.aliasGenerator.GenerateSubqueryAlias()
	for _, column := range columns {
		v.fragmentContext.AddAvailableColumn(column, &ColumnInfo{
			Type:       column.Type().Kind().String(),
			TableAlias: alias,
			Source:     NodeID(fmt.Sprintf("%p", fragment)),
		})
	}
	return NewSubqueryFromItem(fragment, alias)
}

func (v *SQLBuilderVisitor) VisitArrayScan(node *ast.ArrayScanNode) (SQLFragment, error) {
	var inputFromItem *FromItem
	if node.InputScan() != nil {
		fromItem, err := v.VisitScan(node.InputScan())
		if err != nil {
			return nil, fmt.Errorf("failed to visit input scan: %w", err)
		}
		inputFromItem = fromItem
	}

	// Visit the array expression
	arrayExprFragment, err := v.VisitExpression(node.ArrayExpr())
	if err != nil {
		return nil, fmt.Errorf("failed to visit array expression: %w", err)
	}

	// Create UNNEST FromItem for the array
	unnestExpr := arrayExprFragment.(*SQLExpression)
	// Create the json_each table function call
	jsonEachFromItem := &FromItem{
		Type: FromItemTypeTableFunction,
		TableFunction: &TableFunction{
			Name: "json_each",
			Arguments: []*SQLExpression{
				NewFunctionExpression(
					"zetasqlite_decode_array",
					unnestExpr,
				),
			},
		},
	}

	// Create a subquery that selects the proper column names
	unnestSelect := NewSelectStatement()

	// Always select 'value' as the element column
	unnestSelect.SelectList = []*SelectListItem{}

	// Add 'value' as element column to fragment context
	v.fragmentContext.AddAvailableColumn(node.ElementColumn(), &ColumnInfo{
		Expression: NewColumnExpression("value"),
		Name:       fmt.Sprintf("col%d", node.ElementColumn().ColumnID()),
	})

	// Add 'key' to fragment context as offset column if present
	if node.ArrayOffsetColumn() != nil {
		v.fragmentContext.AddAvailableColumn(node.ArrayOffsetColumn().Column(), &ColumnInfo{
			Expression: NewColumnExpression("key"),
			Name:       fmt.Sprintf("col%d", node.ArrayOffsetColumn().Column().ColumnID()),
		})
	}

	for _, col := range node.ColumnList() {
		unnestSelect.SelectList = append(unnestSelect.SelectList, &SelectListItem{
			Expression: v.fragmentContext.GetColumnExpression(col),
			Alias:      fmt.Sprintf("col%d", col.ColumnID()),
		})
	}

	// If there's no InputScan() we can return the select directly
	if inputFromItem == nil {
		unnestSelect.FromClause = jsonEachFromItem
		return unnestSelect, nil
	}
	// Otherwise we handle input scan if present (for correlated array scans)

	// Create join based on join expression and outer flag
	var joinType JoinType
	if node.IsOuter() {
		joinType = JoinTypeLeft
	} else {
		joinType = JoinTypeInner
	}

	// Handle join condition if present
	var joinCondition *SQLExpression
	if node.JoinExpr() != nil {
		conditionFragment, err := v.VisitExpression(node.JoinExpr())
		if err != nil {
			return nil, fmt.Errorf("failed to visit join expression: %w", err)
		}
		joinCondition = conditionFragment.(*SQLExpression)
	} else {
		// If there is no join expression use a CROSS JOIN
		joinType = JoinTypeCross
	}

	// Return a JOINed query combining input and UNNEST
	unnestSelect.FromClause = &FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:      joinType,
			Left:      inputFromItem,
			Right:     jsonEachFromItem,
			Condition: joinCondition,
		},
	}

	return unnestSelect, nil
}

func (v *SQLBuilderVisitor) VisitSingleRowScanNode(node *ast.SingleRowScanNode) (SQLFragment, error) {
	return nil, nil
}

func (v *SQLBuilderVisitor) VisitLiteralNode(node *ast.LiteralNode) (SQLFragment, error) {
	value, err := LiteralFromZetaSQLValue(node.Value())
	if err != nil {
		return nil, fmt.Errorf("failed to convert literal value: %w", err)
	}
	return NewLiteralExpression(value), nil
}

func (v *SQLBuilderVisitor) VisitMakeStructNode(node *ast.MakeStructNode) (SQLFragment, error) {
	typ := node.Type().AsStruct()
	fieldNum := typ.NumFields()
	fields := node.FieldList()
	args := make([]*SQLExpression, 0, fieldNum*2)
	for i := 0; i < fieldNum; i++ {
		fieldName := typ.Field(i).Name()
		args = append(args, NewLiteralExpressionFromGoValue(types.StringType(), fieldName))
		field, err := v.VisitExpression(fields[i])
		if err != nil {
			return nil, err
		}
		args = append(args, field.(*SQLExpression))
	}
	return &SQLExpression{
		Type: ExpressionTypeFunction,
		Function: &FunctionCall{
			Name:      "zetasqlite_make_struct",
			Arguments: args,
		},
	}, nil
}

func (v *SQLBuilderVisitor) VisitGetJsonFieldNode(node *ast.GetJsonFieldNode) (SQLFragment, error) {
	args := make([]*SQLExpression, 0, 2)
	expr, err := v.VisitExpression(node.Expr())
	if err != nil {
		return nil, err
	}
	args = append(args, expr.(*SQLExpression))
	args = append(args, NewLiteralExpressionFromGoValue(types.StringType(), node.FieldName()))
	return &SQLExpression{
		Type: ExpressionTypeFunction,
		Function: &FunctionCall{
			Name:      "zetasqlite_get_json_field",
			Arguments: args,
		},
	}, nil
}

func (v *SQLBuilderVisitor) VisitGetStructFieldNode(node *ast.GetStructFieldNode) (SQLFragment, error) {
	args := make([]*SQLExpression, 0, 2)
	expr, err := v.VisitExpression(node.Expr())
	if err != nil {
		return nil, err
	}
	args = append(args, expr.(*SQLExpression))
	args = append(args, NewLiteralExpression(fmt.Sprintf("%d", node.FieldIdx())))
	return &SQLExpression{
		Type: ExpressionTypeFunction,
		Function: &FunctionCall{
			Name:      "zetasqlite_get_struct_field",
			Arguments: args,
		},
	}, nil
}

func (v *SQLBuilderVisitor) VisitFunctionCallNode(node *ast.FunctionCallNode) (SQLFragment, error) {
	//ctx := v.fragmentContext
	funcName, args, err := v.getFuncNameAndArgs(node.BaseFunctionCallNode, false)
	if err != nil {
		return nil, err
	}
	switch funcName {
	case "zetasqlite_ifnull":
		return NewCaseExpression(
			[]*WhenClause{
				{
					Condition: NewBinaryExpression(args[0], "IS", NewLiteralExpression("NULL")),
					Result:    args[1],
				},
			},
			args[0],
		), nil
	case "zetasqlite_if":

		return NewCaseExpression(
			[]*WhenClause{
				{
					Condition: args[0],
					Result:    args[1],
				},
			},
			args[2],
		), nil
	case "zetasqlite_case_no_value":
		whenClauses := make([]*WhenClause, 0, len(args)/2)
		for i := 0; i < len(args)-1; i += 2 {
			whenClauses = append(whenClauses, &WhenClause{
				Condition: args[i],
				Result:    args[i+1],
			})
		}
		var elseExpr *SQLExpression
		// if args length is odd number, else statement exists.
		if len(args) > (len(args)/2)*2 {
			elseExpr = args[len(args)-1]
		}
		return NewCaseExpression(whenClauses, elseExpr), nil
	case "zetasqlite_case_with_value":
		if len(args) < 2 {
			return nil, fmt.Errorf("not enough arguments for case with value")
		}
		val := args[0]
		args = args[1:]
		var whenClauses []*WhenClause
		for i := 0; i < len(args)-1; i += 2 {
			whenClauses = append(whenClauses, &WhenClause{
				Condition: NewBinaryExpression(val, "=", args[i]),
				Result:    args[i+1],
			})
		}
		// if args length is odd number, else statement exists.
		var elseExpr *SQLExpression
		if len(args) > (len(args)/2)*2 {
			elseExpr = args[len(args)-1]
		}
		return NewCaseExpression(whenClauses, elseExpr), nil
	}
	funcMap := funcMapFromContext(v.context)
	if spec, exists := funcMap[funcName]; exists {
		return spec.CallSQL(v.context, node.BaseFunctionCallNode, args)
	}
	return NewFunctionExpression(
		funcName,
		args...,
	), nil
}

func (v *SQLBuilderVisitor) VisitCastNode(node *ast.CastNode) (SQLFragment, error) {
	expr, err := v.VisitExpression(node.Expr())
	if err != nil {
		return nil, err
	}
	fromType := newType(node.Expr().Type())
	jsonEncodedFromType, err := json.Marshal(fromType)
	if err != nil {
		return nil, err
	}
	toType := newType(node.Type())
	jsonEncodedToType, err := json.Marshal(toType)
	if err != nil {
		return nil, err
	}
	return NewFunctionExpression(
		"zetasqlite_cast",
		expr.(*SQLExpression),
		NewLiteralExpressionFromGoValue(types.StringType(), string(jsonEncodedFromType)),
		NewLiteralExpressionFromGoValue(types.StringType(), string(jsonEncodedToType)),
		NewLiteralExpressionFromGoValue(types.BoolType(), node.ReturnNullOnError()),
	), nil
}

func (v *SQLBuilderVisitor) VisitColumnRefNode(node *ast.ColumnRefNode) (SQLFragment, error) {
	return v.fragmentContext.GetColumnExpression(node.Column()), nil
}

func (v *SQLBuilderVisitor) VisitWithEntryNode(node *ast.WithEntryNode) (SQLFragment, error) {
	subquery, err := v.VisitScan(node.WithSubquery())
	if err != nil {
		return nil, err
	}

	v.fragmentContext.AddWithEntryColumnMapping(
		node.WithQueryName(),
		node.WithSubquery().ColumnList(),
	)

	return &WithClause{
		Name:  node.WithQueryName(),
		Query: NewSelectStarStatement(subquery),
	}, nil
}

// WithRefScanNode
func (v *SQLBuilderVisitor) VisitWithRefScanNode(node *ast.WithRefScanNode) (SQLFragment, error) {
	selectStatement := NewSelectStatement()
	selectStatement.FromClause = &FromItem{
		Type:      FromItemTypeTable,
		TableName: node.WithQueryName(),
	}
	selectStatement.SelectList = []*SelectListItem{}

	mapping := v.fragmentContext.WithEntries[node.WithQueryName()]

	for _, column := range node.ColumnList() {
		selectStatement.SelectList = append(selectStatement.SelectList,
			&SelectListItem{
				Expression: NewColumnExpression(mapping[column.Name()]),
				Alias:      fmt.Sprintf("col%d", column.ColumnID()),
			},
		)
	}
	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitWithScanNode(node *ast.WithScanNode) (SQLFragment, error) {
	withClauses := []*WithClause{}
	for _, entry := range node.WithEntryList() {
		sql, err := v.VisitWithEntryNode(entry)
		if err != nil {
			return nil, err
		}
		withClauses = append(withClauses, sql.(*WithClause))
	}
	query, err := v.VisitScan(node.Query())
	if err != nil {
		return nil, err
	}
	selectStatement := NewSelectStarStatement(query)
	selectStatement.WithClauses = withClauses
	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitSetOperationItemNode(node *ast.SetOperationItemNode) (*FromItem, error) {
	return v.VisitScan(node.Scan())
}

func (v *SQLBuilderVisitor) VisitSetOperationScanNode(node *ast.SetOperationScanNode) (SQLFragment, error) {
	var opType string
	var modifier string
	switch node.OpType() {
	case ast.SetOperationTypeUnionAll:
		opType = "UNION"
		modifier = "ALL"
	case ast.SetOperationTypeUnionDistinct:
		opType = "UNION"
	case ast.SetOperationTypeIntersectAll:
		opType = "INTERSECT"
		modifier = "ALL"
	case ast.SetOperationTypeIntersectDistinct:
		opType = "INTERSECT"
	case ast.SetOperationTypeExceptAll:
		opType = "EXCEPT"
		modifier = "ALL"
	case ast.SetOperationTypeExceptDistinct:
		opType = "EXCEPT"
	default:
		opType = "UNKNOWN"
	}

	operation := &SetOperation{
		Type:     opType,
		Modifier: modifier,
	}

	for _, item := range node.InputItemList() {
		query, err := v.VisitSetOperationItemNode(item)
		if err != nil {
			return nil, err
		}
		operation.Items = append(operation.Items, query.Subquery)
	}

	setStatement := NewSelectStatement()
	setStatement.SetOperation = operation

	// Move all WITH queries from items to top-level
	for _, item := range operation.Items {
		for _, with := range item.WithClauses {
			setStatement.WithClauses = append(setStatement.WithClauses, with)
		}
		item.WithClauses = item.WithClauses[:0]
	}

	selectStatement := NewSelectStatement()
	selectStatement.FromClause = &FromItem{Type: FromItemTypeSubquery, Subquery: setStatement}
	for i, col := range node.ColumnList() {
		v.fragmentContext.AddAvailableColumn(col, &ColumnInfo{})
		column := &SelectListItem{
			Expression: NewColumnExpression(fmt.Sprintf("col%d", node.InputItemList()[0].OutputColumnList()[i].ColumnID())),
			Alias:      fmt.Sprintf("col%d", col.ColumnID()),
		}
		selectStatement.SelectList = append(selectStatement.SelectList, column)
	}
	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitSubqueryExpressionNode(node *ast.SubqueryExprNode) (SQLFragment, error) {
	subquery, err := v.VisitScan(node.Subquery())
	if err != nil {
		return nil, err
	}
	expression := &SQLExpression{
		Type:     ExpressionTypeSubquery,
		Subquery: NewSelectStarStatement(subquery),
	}
	switch node.SubqueryType() {
	case ast.SubqueryTypeScalar:
	case ast.SubqueryTypeArray:
		if len(node.Subquery().ColumnList()) == 0 {
			return nil, fmt.Errorf("failed to find computed column names for array subquery")
		}
		selectStatement := NewSelectStatement()
		selectStatement.SelectList = []*SelectListItem{
			{
				Expression: NewFunctionExpression(
					"zetasqlite_array",
					v.fragmentContext.GetColumnExpression(node.Subquery().ColumnList()[0]),
				),
			},
		}
		selectStatement.FromClause = subquery
		expression.Subquery = selectStatement
	case ast.SubqueryTypeExists:
		return NewExistsExpression(NewSelectStarStatement(subquery)), nil
	case ast.SubqueryTypeIn:
		expr, err := v.VisitExpression(node.InExpr())
		if err != nil {
			return nil, err
		}

		return NewBinaryExpression(
			expr.(*SQLExpression),
			"IN",
			expression,
		), nil
	case ast.SubqueryTypeLikeAny:
	case ast.SubqueryTypeLikeAll:
	}

	return expression, nil
}

func (v *SQLBuilderVisitor) VisitFilterScanNode(node *ast.FilterScanNode) (SQLFragment, error) {
	input, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, err
	}
	filter, err := v.VisitExpression(node.FilterExpr())
	if err != nil {
		return nil, err
	}
	selectStatement := NewSelectStarStatement(input)
	selectStatement.WhereClause = filter.(*SQLExpression)

	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitOrderByScanNode(node *ast.OrderByScanNode) (SQLFragment, error) {
	input, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, err
	}

	orderByItems := make([]*OrderByItem, 0, len(node.OrderByItemList())*2)
	for _, itemNode := range node.OrderByItemList() {
		items, err := v.VisitOrderByItemNode(itemNode)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			orderByItems = append(orderByItems, item)
		}
	}

	selectStatement := NewSelectStarStatement(input)
	// Add ORDER BY clause
	selectStatement.OrderByList = orderByItems

	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitLimitOffsetScanNode(node *ast.LimitOffsetScanNode) (SQLFragment, error) {
	input, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, err
	}

	selectStatement := NewSelectStatement()
	selectStatement.FromClause = input

	for _, col := range node.ColumnList() {
		selectStatement.SelectList = append(selectStatement.SelectList, &SelectListItem{
			Expression: NewColumnExpression(fmt.Sprintf("col%d", col.ColumnID())),
			Alias:      fmt.Sprintf("col%d", col.ColumnID()),
		})
	}

	if node.Limit() != nil {
		limit, err := v.VisitExpression(node.Limit())
		if err != nil {
			return nil, err
		}
		selectStatement.LimitClause = limit.(*SQLExpression)
	}
	if node.Offset() != nil {
		offset, err := v.VisitExpression(node.Offset())
		if err != nil {
			return nil, err
		}
		selectStatement.OffsetClause = offset.(*SQLExpression)
	}

	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitAnalyticScanNode(node *ast.AnalyticScanNode) (SQLFragment, error) {
	input, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, err
	}

	selectStatement := NewSelectStatement()
	selectStatement.FromClause = input

	// Visit and store analytic function expressions in the fragmentContext
	for _, group := range node.FunctionGroupList() {
		_, err := v.VisitAnalyticFunctionGroupNode(group)
		if err != nil {
			return nil, err
		}
	}

	for _, col := range node.ColumnList() {
		selectStatement.SelectList = append(selectStatement.SelectList, &SelectListItem{
			Expression: v.fragmentContext.GetColumnExpression(col),
			Alias:      fmt.Sprintf("col%d", col.ColumnID()),
		})
	}

	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitOrderByItemNode(node *ast.OrderByItemNode) ([]*OrderByItem, error) {
	// Returns either a column reference, or the underlying expression for the node
	expr := v.fragmentContext.GetColumnExpression(node.ColumnRef().Column())
	expr.Collation = "zetasqlite_collate"

	items := make([]*OrderByItem, 0)
	if node.NullOrder() != ast.NullOrderModeOrderUnspecified {
		nullExpr := NewBinaryExpression(
			expr,
			"IS NOT",
			NewLiteralExpression("NULL"),
		)
		nullExpr.Collation = "zetasqlite_collate"

		switch node.NullOrder() {
		case ast.NullOrderModeNullsFirst:
			items = append(items, &OrderByItem{
				Direction:  "ASC",
				Expression: nullExpr,
			})
		case ast.NullOrderModeNullsLast:
			items = append(items, &OrderByItem{
				Direction:  "DESC",
				Expression: nullExpr,
			})
		}
	}
	columnItem := &OrderByItem{
		Expression: expr,
	}

	if node.IsDescending() {
		columnItem.Direction = "DESC"
	} else {
		columnItem.Direction = "ASC"
	}

	items = append(items, columnItem)

	return items, nil
}

func (v *SQLBuilderVisitor) VisitAnalyticFunctionGroupNode(node *ast.AnalyticFunctionGroupNode) ([]*SelectListItem, error) {
	specification := &WindowSpecification{
		OrderBy:     make([]*OrderByItem, 0),
		PartitionBy: make([]*SQLExpression, 0),
	}

	if orderBy := node.OrderBy(); orderBy != nil {
		for _, order := range node.OrderBy().OrderByItemList() {
			items, err := v.VisitOrderByItemNode(order)
			if err != nil {
				return nil, err
			}
			for _, item := range items {
				specification.OrderBy = append(specification.OrderBy, item)
			}
		}
	}

	if partitionBy := node.PartitionBy(); partitionBy != nil {
		for _, partition := range node.PartitionBy().PartitionByList() {
			fragment, err := v.VisitExpression(partition)
			if err != nil {
				return nil, err
			}
			expr := fragment.(*SQLExpression)
			expr.Collation = "zetasqlite_collate"
			specification.PartitionBy = append(specification.PartitionBy, expr)
		}
	}

	items := make([]*SelectListItem, 0)
	for _, call := range node.AnalyticFunctionList() {
		fragment, err := v.VisitExpression(call)
		if err != nil {
			return nil, err
		}

		item := fragment.(*SQLExpression)
		if item.Type != ExpressionTypeFunction && item.Function == nil {
			return nil, fmt.Errorf("expected analytic function, got unexpected expression type: %d", item.Type)
		} else {
			// Copy over group context to the function call spec
			item.Function.WindowSpec.OrderBy = specification.OrderBy
			item.Function.WindowSpec.PartitionBy = specification.PartitionBy
		}

		v.fragmentContext.AddAvailableColumn(call.Column(), &ColumnInfo{
			Expression: item,
		})
	}
	return items, nil
}

func getWindowBoundaryType(boundaryType ast.BoundaryType, literal SQLFragment) string {
	switch boundaryType {
	case ast.UnboundedPrecedingType:
		return "UNBOUNDED PRECEDING"
	case ast.OffsetPrecedingType:
		return fmt.Sprintf("%s PRECEDING", literal.String())
	case ast.CurrentRowType:
		return "CURRENT ROW"
	case ast.OffsetFollowingType:
		return fmt.Sprintf("%s FOLLOWING", literal.String())
	case ast.UnboundedFollowingType:
		return "UNBOUNDED FOLLOWING"
	}
	return ""
}

func (v *SQLBuilderVisitor) getWindowBoundaryOptionFuncSQL(node *ast.WindowFrameNode) (*FrameClause, error) {
	if node == nil {
		return &FrameClause{Unit: "ROWS", Start: &FrameBound{Type: "UNBOUNDED PRECEDING"}, End: &FrameBound{Type: "UNBOUNDED FOLLOWING"}}, nil
	}

	frameNodes := [2]*ast.WindowFrameExprNode{node.StartExpr(), node.EndExpr()}
	frames := make([]string, 0, 2)
	for _, expr := range frameNodes {
		typ := expr.BoundaryType()
		switch typ {
		case ast.UnboundedPrecedingType, ast.CurrentRowType, ast.UnboundedFollowingType:
			frames = append(frames, getWindowBoundarySQL(typ, ""))
		case ast.OffsetPrecedingType, ast.OffsetFollowingType:
			literal, err := v.VisitExpression(expr.Expression())
			if err != nil {
				return nil, err
			}
			frames = append(frames, getWindowBoundaryType(typ, literal))
		default:
			return nil, fmt.Errorf("unexpected boundary type %d", typ)
		}
	}
	var unit string
	switch node.FrameUnit() {
	case ast.FrameUnitRows:
		unit = "ROWS"
	case ast.FrameUnitRange:
		unit = "RANGE"
	default:
		return nil, fmt.Errorf("unexpected frame unit %d", node.FrameUnit())
	}
	return &FrameClause{Unit: unit, Start: &FrameBound{Type: frames[0]}, End: &FrameBound{Type: frames[1]}}, nil
}

var windowFuncFixedRangesVisitor = map[string]*FrameClause{
	"zetasqlite_window_ntile": {
		Unit:  "ROWS",
		Start: &FrameBound{Type: "CURRENT ROW"},
		End:   &FrameBound{Type: "UNBOUNDED FOLLOWING"},
	},
	"zetasqlite_window_cume_dist": {
		Unit:  "GROUPS",
		Start: &FrameBound{Type: "FOLLOWING", Offset: NewLiteralExpressionFromGoValue(types.Int64Type(), int64(1))},
		End:   &FrameBound{Type: "UNBOUNDED FOLLOWING"},
	},
	"zetasqlite_window_dense_rank": {
		Unit:  "RANGE",
		Start: &FrameBound{Type: "UNBOUNDED PRECEDING"},
		End:   &FrameBound{Type: "CURRENT ROW"},
	},
	"zetasqlite_window_rank": {
		Unit:  "GROUPS",
		Start: &FrameBound{Type: "UNBOUNDED PRECEDING"},
		End:   &FrameBound{Type: "CURRENT ROW EXCLUDE TIES"},
	},
	"zetasqlite_window_percent_rank": {
		Unit:  "GROUPS",
		Start: &FrameBound{Type: "CURRENT ROW"},
		End:   &FrameBound{Type: "UNBOUNDED FOLLOWING"},
	},
	"zetasqlite_window_row_number": {
		Unit:  "ROWS",
		Start: &FrameBound{Type: "UNBOUNDED PRECEDING"},
		End:   &FrameBound{Type: "CURRENT ROW"},
	},
	"zetasqlite_window_lag": {
		Unit:  "ROWS",
		Start: &FrameBound{Type: "UNBOUNDED PRECEDING"},
		End:   &FrameBound{Type: "CURRENT ROW"},
	},
	"zetasqlite_window_lead": {
		Unit:  "ROWS",
		Start: &FrameBound{Type: "CURRENT ROW"},
		End:   &FrameBound{Type: "UNBOUNDED FOLLOWING"},
	},
}

func (v *SQLBuilderVisitor) VisitAnalyticFunctionCallNode(node *ast.AnalyticFunctionCallNode) (SQLFragment, error) {
	funcName, args, err := v.getFuncNameAndArgs(node.BaseFunctionCallNode, true)
	if err != nil {
		return nil, err
	}

	if node.Distinct() {
		args = append(args, NewFunctionExpression("zetasqlite_distinct"))
	}

	_, ignoreNullsByDefault := windowFunctionsIgnoreNullsByDefault[funcName]

	switch node.NullHandlingModifier() {
	case ast.IgnoreNulls:
		args = append(args, NewFunctionExpression("zetasqlite_ignore_nulls"))
	case ast.DefaultNullHandling:
		if ignoreNullsByDefault {
			args = append(args, NewFunctionExpression("zetasqlite_ignore_nulls"))
		}
	}

	frame := node.WindowFrame()
	frameClause, found := windowFuncFixedRangesVisitor[funcName]
	if found && frame != nil {
		return nil, fmt.Errorf("%s: window framing clause is not allowed for analytic function", node.BaseFunctionCallNode.Function().Name())
	}
	if !found {
		frameClause, err = v.getWindowBoundaryOptionFuncSQL(node.WindowFrame())
		if err != nil {
			return nil, nil
		}
	}

	// Ordering and partitioning comes from the AnalyticFunctionGroupNode; omit it here
	specification := &WindowSpecification{}
	specification.FrameClause = frameClause

	funcMap := funcMapFromContext(v.context)

	if spec, exists := funcMap[funcName]; exists {
		return spec.CallSQL(v.context, node.BaseFunctionCallNode, args)
	}

	return &SQLExpression{
		Type: ExpressionTypeFunction,
		Function: &FunctionCall{
			Name:       funcName,
			Arguments:  args,
			IsDistinct: false,
			WindowSpec: specification,
		},
	}, nil
}

func (v *SQLBuilderVisitor) VisitOutputColumnNode(node *ast.OutputColumnNode) (SQLFragment, error) {
	return v.fragmentContext.GetColumnExpression(node.Column()), nil
}

func (v *SQLBuilderVisitor) VisitComputedColumnNode(node *ast.ComputedColumnNode) (SQLFragment, error) {
	fragment, err := v.VisitExpression(node.Expr())
	if err != nil {
		return nil, err
	}
	expr := fragment.(*SQLExpression)
	expr.Alias = node.Column().Name()
	return expr, nil
}

/*
ProjectScan
+-column_list=[$groupby.day#29, $aggregate.total#28]
+-input_scan=
  +-AggregateScan
    +-column_list=[$groupby.day#29, $aggregate.total#28]
    +-input_scan=
    | +-WithRefScan(column_list=Sales.[sku#25, day#26, price#27], with_query_name="Sales")
    +-group_by_list=
    | +-day#29 := ColumnRef(parse_location=265-268, type=INT64, column=Sales.day#26)
    +-aggregate_list=
      +-total#28 :=
        +-AggregateFunctionCall(ZetaSQL:sum(DOUBLE) -> DOUBLE)
          +-parse_location=272-282
          +-ColumnRef(parse_location=276-281, type=DOUBLE, column=Sales.price#27)

*/

func (v *SQLBuilderVisitor) VisitAggregateFunctionCallNode(node *ast.AggregateFunctionCallNode) (SQLFragment, error) {
	funcName, args, err := v.getFuncNameAndArgs(node.BaseFunctionCallNode, false)
	if err != nil {
		return nil, err
	}

	funcMap := funcMapFromContext(v.context)
	if spec, exists := funcMap[funcName]; exists {
		return spec.CallSQL(v.context, node.BaseFunctionCallNode, args)
	}

	var opts []*SQLExpression
	for _, item := range node.OrderByItemList() {
		columnRef := item.ColumnRef()

		opts = append(opts, NewFunctionExpression(
			"zetasqlite_order_by",
			v.fragmentContext.GetColumnExpression(columnRef.Column()),
			NewLiteralExpressionFromGoValue(types.BoolType(), !item.IsDescending()),
		))
	}
	if node.Distinct() {
		opts = append(opts, NewFunctionExpression("zetasqlite_distinct"))
	}
	if node.Limit() != nil {
		limit, err := v.VisitExpression(node.Limit())
		if err != nil {
			return nil, err
		}
		opts = append(opts, NewFunctionExpression("zetasqlite_limit", limit.(*SQLExpression)))
	}
	switch node.NullHandlingModifier() {
	case ast.IgnoreNulls:
		opts = append(opts, NewFunctionExpression("zetasqlite_ignore_nulls"))
	case ast.RespectNulls:
	}
	args = append(args, opts...)

	return NewFunctionExpression(funcName, args...), nil
}

func (v *SQLBuilderVisitor) VisitAggregateScanNode(node *ast.AggregateScanNode) (SQLFragment, error) {
	nodeID := NodeID(fmt.Sprintf("aggregate_%p", node))

	inputFromItem, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, fmt.Errorf("failed to visit input scan: %w", err)
	}

	// Process aggregate functions and store column mappings
	aggregateExpressions := map[string]*SQLExpression{}
	for _, agg := range node.AggregateList() {
		fragment, err := v.VisitExpression(agg)
		if err != nil {
			return nil, fmt.Errorf("failed to visit aggregate expression: %w", err)
		}
		expr := fragment.(*SQLExpression)

		// Store column reference mapping for this aggregate
		colName := agg.Column().Name()
		v.fragmentContext.AddAvailableColumn(agg.Column(), &ColumnInfo{
			Name:   colName,
			Type:   agg.Column().Type().Kind().String(),
			Source: nodeID,
		})

		aggregateExpressions[colName] = expr
	}

	// Process GROUP BY columns
	groupByColumns := map[string]*SQLExpression{}
	groupByColumnMap := make(map[string]struct{})
	for _, col := range node.GroupByList() {
		fragment, err := v.VisitExpression(col)
		if err != nil {
			return nil, fmt.Errorf("failed to visit group by expression: %w", err)
		}
		expr := fragment.(*SQLExpression)
		colName := col.Column().Name()
		groupByColumns[colName] = expr
		groupByColumnMap[colName] = struct{}{}
		v.fragmentContext.AddAvailableColumn(col.Column(), &ColumnInfo{
			Expression: expr,
			Type:       col.Column().Type().Kind().String(),
		})
	}

	// Build output columns list
	outputColumns := make([]*SelectListItem, 0, len(node.ColumnList()))
	columnNames := make([]string, 0, len(node.ColumnList()))
	for _, col := range node.ColumnList() {
		colName := col.Name()
		columnNames = append(columnNames, colName)

		var expr *SQLExpression
		// Check if this is an aggregate or group by column
		if groupByExpr, found := groupByColumns[colName]; found {
			expr = groupByExpr
		}
		if aggregateExpr, found := aggregateExpressions[colName]; found {
			expr = aggregateExpr
		}

		if expr == nil {
			return nil, fmt.Errorf("failed to find column %s in aggregate or group by expressions", colName)
		}

		outputColumns = append(outputColumns, &SelectListItem{
			Expression: expr,
			Alias:      fmt.Sprintf("col%d", col.ColumnID()),
		})
	}

	// Handle GROUPING SETS if present
	if len(node.GroupingSetList()) != 0 {
		return v.buildGroupingSetsQuery(node, inputFromItem, outputColumns, columnNames, groupByColumns, groupByColumnMap)
	}

	// Simple GROUP BY case
	selectStatement := NewSelectStatement()
	selectStatement.SelectList = outputColumns
	selectStatement.FromClause = inputFromItem

	// Add GROUP BY clause with zetasqlite_group_by wrapper
	if len(groupByColumns) > 0 {
		wrappedGroupBy := make([]*SQLExpression, 0, len(groupByColumns))
		for _, groupByCol := range groupByColumns {
			wrappedGroupBy = append(wrappedGroupBy, NewFunctionExpression(
				"zetasqlite_group_by",
				groupByCol,
			))
		}
		selectStatement.GroupByList = wrappedGroupBy
	}

	// Store fragment metadata
	outputColumnInfos := make([]*ColumnInfo, 0, len(node.ColumnList()))
	for _, col := range node.ColumnList() {
		columnInfo := &ColumnInfo{
			Name:   getUniqueColumnName(col),
			Type:   col.Type().Kind().String(),
			Source: nodeID,
		}
		outputColumnInfos = append(outputColumnInfos, columnInfo)
	}

	metadata := &FragmentMetadata{
		NodeType:      "AggregateScan",
		OutputColumns: outputColumnInfos,
		Dependencies:  []NodeID{NodeID(fmt.Sprintf("%p", node.InputScan()))},
	}

	v.fragmentContext.StoreFragment(nodeID, selectStatement, metadata)
	return selectStatement, nil
}

// buildGroupingSetsQuery handles the complex case of GROUPING SETS
func (v *SQLBuilderVisitor) buildGroupingSetsQuery(
	node *ast.AggregateScanNode,
	inputFromItem *FromItem,
	outputColumns []*SelectListItem,
	columnNames []string,
	groupByColumns map[string]*SQLExpression,
	groupByColumnMap map[string]struct{},
) (SQLFragment, error) {

	statements := make([]*SelectStatement, 0, len(node.GroupingSetList()))

	for _, groupingSet := range node.GroupingSetList() {
		// Process columns in this grouping set
		groupBySetColumns := make([]*SQLExpression, 0)
		groupBySetColumnMap := make(map[string]struct{})

		for _, col := range groupingSet.GroupByColumnList() {
			fragment, err := v.VisitExpression(col)
			if err != nil {
				return nil, fmt.Errorf("failed to visit grouping set column: %w", err)
			}
			expr := fragment.(*SQLExpression)
			colName := col.Column().Name()

			// Wrap with zetasqlite_group_by
			wrappedExpr := NewFunctionExpression("zetasqlite_group_by", expr)
			groupBySetColumns = append(groupBySetColumns, wrappedExpr)
			groupBySetColumnMap[colName] = struct{}{}
		}

		// Determine which columns should be NULL
		nullColumnNameMap := make(map[string]struct{})
		for colName := range groupByColumnMap {
			if _, exists := groupBySetColumnMap[colName]; !exists {
				nullColumnNameMap[colName] = struct{}{}
			}
		}

		// Build SELECT list for this grouping set
		selectList := make([]*SelectListItem, 0, len(outputColumns))
		for i, originalCol := range outputColumns {
			colName := columnNames[i]

			if _, shouldBeNull := nullColumnNameMap[colName]; shouldBeNull {
				// This column should be NULL in this grouping set
				selectList = append(selectList, &SelectListItem{
					Expression: NewLiteralExpression("NULL"),
					Alias:      colName,
				})
			} else {
				// Use the original expression
				selectList = append(selectList, originalCol)
			}
		}

		// Create SELECT statement for this grouping set
		stmt := NewSelectStatement()
		stmt.SelectList = selectList
		stmt.FromClause = inputFromItem

		if len(groupBySetColumns) > 0 {
			stmt.GroupByList = groupBySetColumns
		}

		statements = append(statements, stmt)
	}

	// Combine with UNION ALL
	if len(statements) == 1 {
		return statements[0], nil
	}

	setOperation := &SetOperation{
		Type:     "UNION",
		Modifier: "ALL",
		Items:    statements,
	}

	unionStatement := NewSelectStatement()
	unionStatement.SetOperation = setOperation

	// Add ORDER BY with collation for proper grouping behavior
	if len(groupByColumns) > 0 {
		orderByItems := make([]*OrderByItem, 0, len(groupByColumns))
		for _, groupByCol := range groupByColumns {
			// Create a copy with collation
			orderExpr := &SQLExpression{
				Type:      groupByCol.Type,
				Value:     groupByCol.Value,
				Collation: "zetasqlite_collate",
			}

			orderByItems = append(orderByItems, &OrderByItem{
				Expression: orderExpr,
				Direction:  "ASC",
			})
		}
		unionStatement.OrderByList = orderByItems
	}

	return unionStatement, nil
}

// VisitParameterNode returns a literal expression that is used by the SQLite driver for parameter bindings
func (v *SQLBuilderVisitor) VisitParameterNode(node *ast.ParameterNode) (SQLFragment, error) {
	if node.Name() == "" {
		return NewLiteralExpression("?"), nil
	}
	return NewLiteralExpression(fmt.Sprintf("@%s", node.Name())), nil
}

// VisitArgumentRefNode returns a literal expression that is used by the SQLite driver for parameter bindings
func (v *SQLBuilderVisitor) VisitArgumentRefNode(node *ast.ArgumentRefNode) (SQLFragment, error) {
	return NewLiteralExpression(fmt.Sprintf("@%s", node.Name())), nil
}

// Utility functions

func convertJoinType(joinType ast.JoinType) JoinType {
	switch joinType {
	case ast.JoinTypeInner:
		return JoinTypeInner
	case ast.JoinTypeLeft:
		return JoinTypeLeft
	case ast.JoinTypeRight:
		return JoinTypeRight
	case ast.JoinTypeFull:
		return JoinTypeFull
	default:
		return JoinTypeInner
	}
}
