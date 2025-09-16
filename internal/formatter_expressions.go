// Package internal provides the core SQL formatting functionality for go-zetasqlite.
// This file (formatter_expressions.go) implements expression conversion from ZetaSQL AST
// nodes to SQLite-compatible SQL fragments.
//
// The main functionality includes:
// - Expression dispatch and type-specific conversion
// - FunctionCall call handling with special cases for control flow
// - Type casting and column reference resolution
// - Subquery expression conversion
// - Parameter and argument reference handling
//
// The code uses the visitor pattern to traverse ZetaSQL AST nodes and generate
// equivalent SQLite SQL syntax, handling semantic differences between the two systems.
package internal

import (
	"fmt"
	"github.com/goccy/go-json"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// ColumnListProvider provides a common interface for AST nodes that contain column lists.
// This interface allows different node types to be treated uniformly when accessing their columns.
type ColumnListProvider interface {
	ColumnList() []*ast.Column
}

// VisitExpression is the central dispatcher that routes different ZetaSQL expression types
// to their specific handlers. It converts ZetaSQL AST expressions into SQLite-compatible
// SQL fragments by using the visitor pattern.
//
// Supported expression types include literals, structs, function calls, casts, column
// references, subqueries, aggregate functions, and parameters.
//
// Returns a SQLFragment representing the converted expression, or an error if the
// expression type is unsupported or conversion fails.
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
	case *ast.DMLDefaultNode:
		return v.VisitDMLDefaultNode(e)
	case *ast.DMLValueNode:
		return v.VisitDMLValueNode(e)

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// VisitSingleRowScanNode handles single-row table scans. They are effectively no-ops but needed as input for other scans
func (v *SQLBuilderVisitor) VisitSingleRowScanNode(node *ast.SingleRowScanNode) (SQLFragment, error) {
	return nil, nil
}

// VisitLiteralNode converts ZetaSQL literal values into SQLite-compatible literal expressions.
// It handles type conversion and proper escaping of literal values.
//
// Returns a LiteralExpression fragment containing the converted value.
func (v *SQLBuilderVisitor) VisitLiteralNode(node *ast.LiteralNode) (SQLFragment, error) {
	value, err := LiteralFromZetaSQLValue(node.Value())
	if err != nil {
		return nil, fmt.Errorf("failed to convert literal value: %w", err)
	}
	return NewLiteralExpression(value), nil
}

// VisitMakeStructNode creates STRUCT expressions using the zetasqlite_make_struct function.
// It converts ZetaSQL STRUCT constructors into function calls with alternating field names and values.
//
// The function:
// 1. Extracts field names and types from the struct definition
// 2. Visits each field expression to get its SQL representation
// 3. Creates alternating name/value argument pairs
// 4. Calls zetasqlite_make_struct(name1, value1, name2, value2, ...)
//
// Returns a FunctionCall expression for the struct constructor.
func (v *SQLBuilderVisitor) VisitMakeStructNode(node *ast.MakeStructNode) (SQLFragment, error) {
	typ := node.Type().AsStruct()
	fieldNum := typ.NumFields()
	fields := node.FieldList()
	args := make([]*SQLExpression, 0, fieldNum*2)
	for i := 0; i < fieldNum; i++ {
		fieldName := typ.Field(i).Name()
		args = append(args, NewLiteralExpression(fieldName))
		field, err := v.VisitExpression(fields[i])
		if err != nil {
			return nil, err
		}
		args = append(args, field.(*SQLExpression))
	}
	return &SQLExpression{
		Type: ExpressionTypeFunction,
		FunctionCall: &FunctionCall{
			Name:      "zetasqlite_make_struct",
			Arguments: args,
		},
	}, nil
}

// VisitGetJsonFieldNode extracts fields from JSON objects using the zetasqlite_get_json_field function.
// It converts ZetaSQL JSON field access operations into SQLite-compatible function calls.
//
// The function creates a call to zetasqlite_get_json_field(json_expr, field_name).
//
// Returns a FunctionCall expression for the JSON field access.
func (v *SQLBuilderVisitor) VisitGetJsonFieldNode(node *ast.GetJsonFieldNode) (SQLFragment, error) {
	args := make([]*SQLExpression, 0, 2)
	expr, err := v.VisitExpression(node.Expr())
	if err != nil {
		return nil, err
	}
	args = append(args, expr.(*SQLExpression))
	args = append(args, NewLiteralExpression(node.FieldName()))
	return &SQLExpression{
		Type: ExpressionTypeFunction,
		FunctionCall: &FunctionCall{
			Name:      "zetasqlite_get_json_field",
			Arguments: args,
		},
	}, nil
}

// VisitGetStructFieldNode extracts fields from STRUCT objects using the zetasqlite_get_struct_field function.
// It converts ZetaSQL STRUCT field access operations into SQLite-compatible function calls.
//
// The function creates a call to zetasqlite_get_struct_field(struct_expr, field_index).
// The field index is used instead of field name for efficient access.
//
// Returns a FunctionCall expression for the STRUCT field access.
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
		FunctionCall: &FunctionCall{
			Name:      "zetasqlite_get_struct_field",
			Arguments: args,
		},
	}, nil
}

// VisitFunctionCallNode handles ZetaSQL function calls, with special handling for control flow functions.
// It converts function calls to SQLite-compatible syntax, transforming certain functions into CASE expressions.
//
// Special transformations:
// - zetasqlite_ifnull → CASE expression with NULL check
// - zetasqlite_if → CASE expression with condition
// - zetasqlite_case_no_value → CASE expression without value comparison
// - zetasqlite_case_with_value → CASE expression with value comparison
//
// For other functions, it checks the context function map for custom implementations,
// falling back to standard function call syntax.
//
// Returns a SQLFragment representing the function call or converted CASE expression.
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

// VisitCastNode handles ZetaSQL type casting operations using the zetasqlite_cast function.
// It converts type casts by encoding both source and target type information as JSON.
//
// The function creates a call to zetasqlite_cast with the following arguments:
// 1. Expression to cast
// 2. JSON-encoded source type information
// 3. JSON-encoded target type information
// 4. Boolean flag indicating whether to return NULL on cast errors
//
// Returns a FunctionCall expression for the type cast operation.
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
		NewLiteralExpression(string(jsonEncodedFromType)),
		NewLiteralExpression(string(jsonEncodedToType)),
		NewLiteralExpression(fmt.Sprintf("%t", node.ReturnNullOnError())),
	), nil
}

// VisitColumnRefNode handles column references by looking up the column expression from the fragment context.
// This allows columns to be properly qualified with table aliases and resolved to their source expressions.
//
// Returns the column expression from the fragment context, which may be a simple column reference
// or a more complex expression depending on the column's origin.
func (v *SQLBuilderVisitor) VisitColumnRefNode(node *ast.ColumnRefNode) (SQLFragment, error) {
	return v.fragmentContext.GetColumnExpression(node.Column()), nil
}

// VisitSubqueryExpressionNode handles different types of subquery expressions in ZetaSQL.
// It converts subqueries based on their type and context within the larger querybuilder.
//
// Supported subquery types:
// - Scalar: Returns a single value from the subquery
// - Array: Wraps the result using zetasqlite_array function to create an array
// - Exists: Creates an EXISTS(subquery) expression
// - In: Creates an "expr IN (subquery)" expression
// - LikeAny/LikeAll: Not fully implemented
//
// Returns a SQLFragment representing the subquery expression based on its type.
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

// VisitOutputColumnNode handles output column references by delegating to the fragment context.
// Output columns represent the final columns in a querybuilder's result set.
//
// Returns the column expression from the fragment context.
func (v *SQLBuilderVisitor) VisitOutputColumnNode(node *ast.OutputColumnNode) (SQLFragment, error) {
	return v.fragmentContext.GetColumnExpression(node.Column()), nil
}

// VisitComputedColumnNode handles computed columns by visiting their underlying expressions
// and assigning the column's name as an alias.
//
// Computed columns represent expressions that are calculated and given a column name.
//
// Returns the computed expression with the column name set as its alias.
func (v *SQLBuilderVisitor) VisitComputedColumnNode(node *ast.ComputedColumnNode) (SQLFragment, error) {
	return v.VisitExpression(node.Expr())
}

// VisitOrderByItemNode converts ZetaSQL ORDER BY items into SQLite ORDER BY clauses.
// It handles null ordering behavior by generating additional ORDER BY items when needed.
//
// The function:
// 1. Gets the column expression and applies zetasqlite_collate collation
// 2. Handles NULL ordering (NULLS FIRST/LAST) by creating additional ORDER BY items
// 3. Sets the sort direction (ASC/DESC) based on the node's IsDescending flag
//
// Returns a slice of OrderByItem objects representing the complete ordering specification.
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

// VisitAggregateScanNode handles ZetaSQL GROUP BY operations and aggregate functions.
// It processes both simple GROUP BY queries and complex GROUPING SETS operations.
//
// The function:
// 1. Visits the input scan to get the base data source
// 2. Processes all aggregate expressions and makes them available in context
// 3. Processes GROUP BY columns and wraps them with zetasqlite_group_by function
// 4. Builds the output column list matching ZetaSQL semantics
// 5. Delegates to buildGroupingSetsQuery for GROUPING SETS or creates simple GROUP BY
//
// Returns a SelectStatement with the aggregate operation and proper grouping.
func (v *SQLBuilderVisitor) VisitAggregateScanNode(node *ast.AggregateScanNode) (SQLFragment, error) {
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
			Name: colName,
			Type: agg.Column().Type().Kind().String(),
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
			Alias:      col.Name(),
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
			Name: GetUniqueColumnName(col),
			Type: col.Type().Kind().String(),
		}
		outputColumnInfos = append(outputColumnInfos, columnInfo)
	}

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

// VisitParameterNode converts ZetaSQL parameter references into SQLite parameter syntax.
// It handles both named and positional parameters used in prepared statements.
//
// Parameter formats:
// - Named parameters: "@parameter_name"
// - Positional parameters: "?"
//
// Returns a LiteralExpression containing the parameter placeholder.
func (v *SQLBuilderVisitor) VisitParameterNode(node *ast.ParameterNode) (SQLFragment, error) {
	if node.Name() == "" {
		return NewLiteralExpression("?"), nil
	}
	return NewLiteralExpression(fmt.Sprintf("@%s", node.Name())), nil
}

// VisitArgumentRefNode converts ZetaSQL function argument references into SQLite parameter syntax.
// This is used in the context of user-defined functions where arguments are referenced by name.
//
// Returns a LiteralExpression containing the named parameter reference ("@argument_name").
func (v *SQLBuilderVisitor) VisitArgumentRefNode(node *ast.ArgumentRefNode) (SQLFragment, error) {
	return NewLiteralExpression(fmt.Sprintf("@%s", node.Name())), nil
}
