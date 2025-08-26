package internal

import (
	"context"
	"fmt"
	"github.com/goccy/go-json"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"strings"
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
	return v.VisitScan(node.Query())
}

// VisitScan will always return a SelectStatement (which implements SQLFragment)
func (v *SQLBuilderVisitor) VisitScan(scan ast.Node) (SQLFragment, error) {
	switch s := scan.(type) {
	case *ast.TableScanNode:
		return v.VisitTableScan(s)
	case *ast.ProjectScanNode:
		return v.VisitProjectScan(s)
	case *ast.JoinScanNode:
		return v.VisitJoinScan(s)
	case *ast.ArrayScanNode:
		return v.VisitArrayScan(s)
	case *ast.SingleRowScanNode:
		return v.VisitSingleRowScanNode(s)
	case *ast.WithScanNode:
		return v.VisitWithScanNode(s)
	case *ast.WithRefScanNode:
		return v.VisitWithRefScanNode(s)
	case *ast.SetOperationScanNode:
		return v.VisitSetOperationScanNode(s)
	case *ast.FilterScanNode:
		return v.VisitFilterScanNode(s)
	case *ast.OrderByScanNode:
		return v.VisitOrderByScanNode(s)
	case *ast.LimitOffsetScanNode:
		return v.VisitLimitOffsetScanNode(s)
	case *ast.AnalyticScanNode:
		return v.VisitAnalyticScanNode(s)
	case *ast.AggregateScanNode:
		return v.VisitAggregateScanNode(s)
	default:
		return nil, fmt.Errorf("unsupported scan type: %T", scan)
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
	case *ast.AnalyticFunctionGroupNode:
		return v.VisitAnalyticFunctionGroupNode(e)
	case *ast.AnalyticFunctionCallNode:
		return v.VisitAnalyticFunctionCallNode(e)
	case *ast.AggregateFunctionCallNode:
		return v.VisitAggregateFunctionCallNode(e)
	case *ast.ComputedColumnNode:
		return v.VisitComputedColumnNode(e)

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// Example visitor methods showing fragment storage patterns

func (v *SQLBuilderVisitor) VisitTableScan(node *ast.TableScanNode) (SQLFragment, error) {
	nodeID := NodeID(fmt.Sprintf("table_%p", node))

	// Create table reference fragment
	tableAlias := ""
	if needsAlias(node.Table().Name()) {
		tableAlias = v.fragmentContext.aliasGenerator.GenerateTableAlias()
	}

	fromItem := NewTableFromItem(node.Table().Name(), tableAlias)

	// Create column information for output
	outputColumns := make([]*ColumnInfo, 0)
	for _, col := range node.ColumnList() {
		columnInfo := &ColumnInfo{
			Name:       col.Name(),
			Type:       col.Type().Kind().String(),
			TableAlias: tableAlias,
			Source:     nodeID,
		}
		outputColumns = append(outputColumns, columnInfo)

		// Make columns available in current scope
		columnKey := col.Name()
		if tableAlias != "" {
			columnKey = fmt.Sprintf("%s.%s", tableAlias, col.Name())
		}
		v.fragmentContext.AddAvailableColumn(columnKey, columnInfo)
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
	nodeID := NodeID(fmt.Sprintf("project_%p", node))

	// First visit input scan (bottom-up)
	inputFragment, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, fmt.Errorf("failed to visit input scan: %w", err)
	}

	// Create SELECT statement
	stmt := NewSelectStatement()

	if inputFragment == nil && node.InputScan().Kind() == ast.SingleRowScan {
		// SingleRowScans do not produce any from items (i.e. the query `SELECT 2`)
		stmt.FromClause = nil
	} else if fromItem, ok := inputFragment.(*FromItem); ok {
		// Set FROM clause from input
		stmt.FromClause = fromItem
	} else {
		// Input is a complex query, wrap in subquery
		subquery := inputFragment.(*SelectStatement)
		subqueryAlias := v.fragmentContext.aliasGenerator.GenerateSubqueryAlias()
		stmt.FromClause = NewSubqueryFromItem(subquery, subqueryAlias)
	}

	// Build SELECT list
	outputColumns := make([]*ColumnInfo, 0)
	columnMap := map[string]*SelectListItem{}
	for _, outputColumn := range node.ColumnList() {
		columnMap[outputColumn.Name()] = &SelectListItem{
			Expression: NewColumnExpression(outputColumn.Name()),
			Alias:      outputColumn.Name(),
		}
	}

	for i, computedCol := range node.ExprList() {
		// Visit expression to get fragment
		exprFragment, err := v.VisitExpression(computedCol.Expr())
		if err != nil {
			return nil, fmt.Errorf("failed to visit expression %d: %w", i, err)
		}

		// Convert to SQLExpression
		sqlExpr := exprFragment.(*SQLExpression)

		// Determine alias
		alias := computedCol.Column().Name()

		// Add to SELECT list
		columnMap[alias] = &SelectListItem{
			Expression: sqlExpr,
			Alias:      alias,
		}

		// Create output column info
		columnInfo := &ColumnInfo{
			Name:   alias,
			Type:   computedCol.Column().Type().Kind().String(),
			Source: nodeID,
		}
		outputColumns = append(outputColumns, columnInfo)

		// Make available for parent scopes
		v.fragmentContext.AddAvailableColumn(alias, columnInfo)
	}

	for _, col := range node.ColumnList() {
		stmt.SelectList = append(stmt.SelectList, columnMap[col.Name()])
	}

	// TODO: Maybe delete fragment metadata approach
	// Store fragment with metadata
	metadata := &FragmentMetadata{
		NodeType:      "ProjectScan",
		OutputColumns: outputColumns,
		IsOrdered:     node.IsOrdered(),
		Dependencies:  []NodeID{NodeID(fmt.Sprintf("%p", node.InputScan()))},
	}

	v.fragmentContext.StoreFragment(nodeID, stmt, metadata)
	return stmt, nil
}

func (v *SQLBuilderVisitor) VisitJoinScan(node *ast.JoinScanNode) (SQLFragment, error) {
	//nodeID := NodeID(fmt.Sprintf("join_%p", node))

	// Visit left and right inputs
	leftFragment, err := v.VisitScan(node.LeftScan())
	if err != nil {
		return nil, fmt.Errorf("failed to visit left scan: %w", err)
	}

	rightFragment, err := v.VisitScan(node.RightScan())
	if err != nil {
		return nil, fmt.Errorf("failed to visit right scan: %w", err)
	}

	// Convert fragments to FromItems
	leftFromItem := v.fragmentToFromItem(leftFragment)
	rightFromItem := v.fragmentToFromItem(rightFragment)

	// Build join condition
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

	// Combine output columns from both sides
	//leftMeta := v.fragmentContext.fragmentMetadata[NodeID(fmt.Sprintf("%p", node.LeftScan()))]
	//rightMeta := v.fragmentContext.fragmentMetadata[NodeID(fmt.Sprintf("%p", node.RightScan()))]
	//

	for _, col := range node.ColumnList() {
		selectStatement.SelectList = append(selectStatement.SelectList, &SelectListItem{
			Expression: NewColumnExpression(col.Name()),
			Alias:      col.Name(),
		})
	}

	//outputColumns := make([]*ColumnInfo, 0)
	//outputColumns = append(outputColumns, leftMeta.OutputColumns...)
	//outputColumns = append(outputColumns, rightMeta.OutputColumns...)

	//// Store fragment
	//metadata := &FragmentMetadata{
	//	NodeType:      "JoinScan",
	//	OutputColumns: outputColumns,
	//	Dependencies: []NodeID{
	//		NodeID(fmt.Sprintf("%p", node.LeftScan)),
	//		NodeID(fmt.Sprintf("%p", node.RightScan)),
	//	},
	//}

	//v.fragmentContext.StoreFragment(nodeID, joinClause, metadata)
	return selectStatement, nil
}

// Helper methods

func (v *SQLBuilderVisitor) fragmentToFromItem(fragment SQLFragment) *FromItem {
	switch f := fragment.(type) {
	case *FromItem:
		return f
	case *SelectStatement:
		// Wrap complex query in subquery
		alias := v.fragmentContext.aliasGenerator.GenerateSubqueryAlias()
		return NewSubqueryFromItem(f, alias)
	default:
		panic(fmt.Sprintf("unexpected fragment type: %T", fragment))
	}
}

func createUnnestSelectStatement(node *ast.ArrayScanNode, arrayExpr *SQLExpression) *SelectStatement {
	selectStatement := NewSelectStatement()
	selectStatement.SelectList = []*SelectListItem{
		{
			Expression: NewColumnExpression("value"),
			Alias:      node.ElementColumn().Name(),
		},
	}

	if offsetColumn := node.ArrayOffsetColumn(); offsetColumn != nil {
		selectStatement.SelectList = append(selectStatement.SelectList,
			&SelectListItem{
				Expression: NewColumnExpression("key"),
				Alias:      offsetColumn.Column().Name(),
			})
	}

	selectStatement.FromClause = &FromItem{
		Type: FromItemTypeTableFunction,
		TableFunction: &TableFunction{
			Name: "json_each",
			Arguments: []*SQLExpression{
				NewFunctionExpression(
					"zetasqlite_decode_array",
					arrayExpr,
				),
			},
		},
	}
	return selectStatement
}

func (v *SQLBuilderVisitor) VisitArrayScan(node *ast.ArrayScanNode) (SQLFragment, error) {
	nodeID := NodeID(fmt.Sprintf("array_%p", node))

	// Visit the array expression
	arrayExprFragment, err := v.VisitExpression(node.ArrayExpr())
	if err != nil {
		return nil, fmt.Errorf("failed to visit array expression: %w", err)
	}

	// Generate table alias for the array scan
	scanAlias := v.fragmentContext.aliasGenerator.GenerateTableAlias()

	// Create output columns
	outputColumns := make([]*ColumnInfo, 0)

	// Element column (the array value)
	elementColumnInfo := &ColumnInfo{
		Name:       node.ElementColumn().Name(),
		Type:       node.ElementColumn().Type().Kind().String(),
		TableAlias: scanAlias,
		Source:     nodeID,
	}
	outputColumns = append(outputColumns, elementColumnInfo)
	v.fragmentContext.AddAvailableColumn(node.ElementColumn().Name(), elementColumnInfo)

	// Array offset column if present
	if node.ArrayOffsetColumn() != nil {
		offsetColumnInfo := &ColumnInfo{
			Name:       node.ArrayOffsetColumn().Column().Name(),
			Type:       node.ArrayOffsetColumn().Column().Type().Kind().String(),
			TableAlias: scanAlias,
			Source:     nodeID,
		}
		outputColumns = append(outputColumns, offsetColumnInfo)
		v.fragmentContext.AddAvailableColumn(node.ArrayOffsetColumn().Column().Name(), offsetColumnInfo)
	}

	// Store fragment with metadata
	var dependencies []NodeID
	if node.InputScan() != nil {
		dependencies = append(dependencies, NodeID(fmt.Sprintf("%p", node.InputScan())))
	}

	// Create UNNEST expression for the array
	unnestExpr := arrayExprFragment.(*SQLExpression)
	selectStatement := createUnnestSelectStatement(node, unnestExpr)
	//metadata := &FragmentMetadata{
	//	NodeType:      "ArrayScan",
	//	OutputColumns: outputColumns,
	//	TableAliases:  []string{scanAlias},
	//	Dependencies:  dependencies,
	//}

	// Handle input scan if present (for correlated array scans)
	if node.InputScan() != nil {
		inputFragment, err := v.VisitScan(node.InputScan())
		if err != nil {
			return nil, fmt.Errorf("failed to visit input scan: %w", err)
		}

		// Convert input to FromItem
		inputFromItem := v.fragmentToFromItem(inputFragment)

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
		}

		// Create the join
		selectStatement.FromClause = &FromItem{
			Type: FromItemTypeJoin,
			Join: &JoinClause{
				Type:      joinType,
				Left:      inputFromItem,
				Right:     selectStatement.FromClause,
				Condition: joinCondition,
			},
		}
	}

	//v.fragmentContext.StoreFragment(nodeID, fromItem, metadata)
	return selectStatement, nil
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
	// TODO: runtime-defined functions
	//funcMap := funcMapFromContext(ctx)
	//if spec, exists := funcMap[funcName]; exists {
	//	return spec.CallSQL(ctx, node.BaseFunctionCallNode, args)
	//}
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
	return NewColumnExpression(node.Column().Name()), nil
}

func (v *SQLBuilderVisitor) VisitWithEntryNode(node *ast.WithEntryNode) (SQLFragment, error) {
	subquery, err := v.VisitScan(node.WithSubquery())
	if err != nil {
		return nil, err
	}
	return &WithClause{
		Name:  node.WithQueryName(),
		Query: subquery.(*SelectStatement),
	}, nil
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
	selectStatement := query.(*SelectStatement)
	selectStatement.WithClauses = withClauses
	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitSetOperationItemNode(node *ast.SetOperationItemNode) (SQLFragment, error) {
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
		operation.Items = append(operation.Items, query.(*SelectStatement))
	}

	setStatement := NewSelectStatement()
	setStatement.SetOperation = operation

	selectStatement := NewSelectStatement()
	selectStatement.SelectList = []*SelectListItem{}
	selectStatement.FromClause = &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: setStatement,
	}

	if len(node.InputItemList()) != 0 {
		for _, col := range node.InputItemList()[0].OutputColumnList() {
			column := &SelectListItem{
				Expression: NewColumnExpression(col.Name()),
				Alias:      col.Name(),
			}
			selectStatement.SelectList = append(selectStatement.SelectList, column)
		}
	}

	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitWithRefScanNode(node *ast.WithRefScanNode) (SQLFragment, error) {
	selectStatement := NewSelectStatement()
	selectStatement.FromClause = &FromItem{
		Type:      FromItemTypeTable,
		TableName: node.WithQueryName(),
	}
	selectStatement.SelectList = []*SelectListItem{}
	for _, column := range node.ColumnList() {
		selectStatement.SelectList = append(selectStatement.SelectList,
			&SelectListItem{
				Expression: NewColumnExpression(column.Name()),
				Alias:      column.Name(),
			},
		)
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
		Subquery: subquery.(*SelectStatement),
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
					NewColumnExpression(node.Subquery().ColumnList()[0].Name()),
				),
			},
		}
		selectStatement.FromClause = &FromItem{
			Type:     FromItemTypeSubquery,
			Subquery: subquery.(*SelectStatement),
		}
	case ast.SubqueryTypeExists:
		return NewExistsExpression(subquery.(*SelectStatement)), nil
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

	selectStatement := input.(*SelectStatement)
	selectStatement.WhereClause = filter.(*SQLExpression)
	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitOrderByScanNode(node *ast.OrderByScanNode) (SQLFragment, error) {
	input, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, err
	}

	orderByItems := make([]*OrderByItem, 0, len(node.OrderByItemList()))
	for _, itemNode := range node.OrderByItemList() {
		orderByItem, err := v.VisitOrderByItemNode(itemNode)
		if err != nil {
			return nil, err
		}
		orderByItems = append(orderByItems, orderByItem.(*OrderByItem))
	}
	selectStatement := input.(*SelectStatement)
	selectStatement.OrderByList = orderByItems
	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitLimitOffsetScanNode(node *ast.LimitOffsetScanNode) (SQLFragment, error) {
	input, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, err
	}
	selectStatement := input.(*SelectStatement)
	if node.Limit() != nil {
		limit, err := v.VisitExpression(node.Limit())
		if err != nil {
			return nil, err
		}
		selectStatement.LimitClause = limit.(*SQLExpression)
	}
	if node.Offset() != nil {
		limit, err := v.VisitExpression(node.Offset())
		if err != nil {
			return nil, err
		}
		selectStatement.OffsetClause = limit.(*SQLExpression)
	}

	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitAnalyticScanNode(node *ast.AnalyticScanNode) (SQLFragment, error) {
	input, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, err
	}
	selectStatement := input.(*SelectStatement)

	for _, group := range node.FunctionGroupList() {
		fragment, err := v.VisitAnalyticFunctionGroupNode(group)
		if err != nil {
			return nil, err
		}
		expr := fragment.(*SelectStatement)
		for _, item := range expr.SelectList {
			selectStatement.SelectList = append(selectStatement.SelectList, item)
		}
	}

	return selectStatement, nil
}

func (v *SQLBuilderVisitor) VisitOrderByItemNode(node *ast.OrderByItemNode) (SQLFragment, error) {
	orderByItem := &OrderByItem{}
	columnIdentifier := node.ColumnRef().Column().Name()
	switch node.NullOrder() {
	case ast.NullOrderModeNullsFirst:
		orderByItem.Expression = NewBinaryExpression(
			NewColumnExpression(columnIdentifier),
			"IS",
			NewLiteralExpression("NULL"),
		)
	case ast.NullOrderModeNullsLast:
		orderByItem.Expression = NewBinaryExpression(
			NewColumnExpression(columnIdentifier),
			"IS NOT",
			NewLiteralExpression("NULL"),
		)
	default:
		orderByItem.Expression = &SQLExpression{
			Type:      ExpressionTypeColumn,
			Value:     columnIdentifier,
			Collation: "zetasqlite_collate",
		}
	}

	if node.IsDescending() {
		orderByItem.Direction = "DESC"
	} else {
		orderByItem.Direction = "ASC"
	}
	return orderByItem, nil

}

func (v *SQLBuilderVisitor) VisitAnalyticFunctionGroupNode(node *ast.AnalyticFunctionGroupNode) (SQLFragment, error) {
	specification := &WindowSpecification{
		OrderBy:     make([]*OrderByItem, 0),
		PartitionBy: make([]*SQLExpression, 0),
	}

	if orderBy := node.OrderBy(); orderBy != nil {
		for _, order := range node.OrderBy().OrderByItemList() {
			item, err := v.VisitOrderByItemNode(order)
			if err != nil {
				return nil, err
			}
			specification.OrderBy = append(specification.OrderBy, item.(*OrderByItem))
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

	selectStatement := NewSelectStatement()
	selectStatement.SelectList = []*SelectListItem{}
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
		selectStatement.SelectList = append(selectStatement.SelectList, &SelectListItem{
			Expression: item,
			Alias:      item.Alias,
		})
	}
	return selectStatement, nil
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

	return &SQLExpression{
		Type: ExpressionTypeFunction,
		Function: &FunctionCall{
			Name:       funcName,
			Arguments:  args,
			IsDistinct: false,
			WindowSpec: specification,
		},
	}, nil

	// TODO: runtime-defined functions
	//funcMap := funcMapFromContext(ctx)

	//if spec, exists := funcMap[funcName]; exists {
	//	return spec.CallSQL(ctx, n.node.BaseFunctionCallNode, args)
	//}
}

func (v *SQLBuilderVisitor) VisitOutputColumnNode(node *ast.OutputColumnNode) (SQLFragment, error) {
	return NewColumnExpression(node.Name()), nil
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
	// TODO: Runtime defined functions
	//funcMap := funcMapFromContext(ctx)
	//if spec, exists := funcMap[funcName]; exists {
	//	return spec.CallSQL(ctx, node.BaseFunctionCallNode, args)
	//}
	var opts []*SQLExpression
	for _, item := range node.OrderByItemList() {
		columnRef := item.ColumnRef()
		opts = append(opts, NewFunctionExpression(
			"zetasqlite_order_by",
			NewColumnExpression(columnRef.Column().Name()),
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
		v.fragmentContext.AddAvailableColumn(colName, &ColumnInfo{
			Name:   colName,
			Type:   agg.Column().Type().Kind().String(),
			Source: nodeID,
		})

		aggregateExpressions[colName] = expr
	}

	// Process input scan
	inputFragment, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, fmt.Errorf("failed to visit input scan: %w", err)
	}

	// Convert input to FromItem
	inputFromItem := v.fragmentToFromItem(inputFragment)

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
			Alias:      colName,
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
		for colName, _ := range groupByColumnMap {
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

// Utility functions

func needsAlias(tableName string) bool {
	// Logic to determine if table needs an alias
	// For example, if it's a complex table name or conflicts with keywords
	return strings.Contains(tableName, ".") || strings.Contains(tableName, " ")
}

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
