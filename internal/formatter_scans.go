// Package internal provides scan operation handling for the go-zetasqlite SQL transpiler.
// This file (formatter_scans.go) implements the bottom-up traversal of ZetaSQL scan nodes,
// converting them into SQLite-compatible SQL fragments.
//
// SCAN TRAVERSAL ARCHITECTURE:
//
// ZetaSQL uses a tree of scan nodes where each scan processes its input scan(s) and produces
// output columns. The traversal follows a bottom-up approach:
//
// 1. BOTTOM-UP PROCESSING: Child scans are visited first, then parent scans process their results
// 2. SCOPE MANAGEMENT: Each scan creates a new scope that defines which columns are available
// 3. COLUMN EXPOSURE: Scans expose their output columns to parent scans through the fragment context
// 4. BOUNDARY HANDLING: Column availability is managed at scan scope boundaries
//
// This approach mirrors go-zetasql's design, ensuring that:
// - Column references are resolved correctly across scan boundaries
// - Proper scoping prevents column name conflicts
// - Complex nested queries maintain correct column visibility
//
// SCAN TYPES AND THEIR ROLES:
//
// - TableScan: Base data source, exposes table columns
// - ProjectScan: Computes expressions and exposes computed columns
// - JoinScan: Combines left/right scans, exposes merged column set
// - FilterScan: Adds WHERE conditions, passes through input columns
// - ArrayScan: UNNEST operations, exposes array element columns
// - AggregateScan: GROUP BY operations, exposes aggregate result columns
// - SetOperationScan: UNION/INTERSECT/EXCEPT, exposes unified column set
// - OrderByScan: Sorting operations, passes through input columns
// - LimitOffsetScan: Pagination, passes through input columns
// - AnalyticScan: Window functions, exposes input + analytic columns
// - WithScan: Common table expressions, manages WITH clause scoping
//
// The fragment context maintains column mappings and scope information to ensure
// proper column resolution throughout the scan tree traversal.
package internal

import (
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// VisitScan is the central dispatcher for all scan node types in the ZetaSQL AST.
// It implements the bottom-up traversal pattern where child scans are processed first,
// then parent scans build upon their results.
//
// The function:
// 1. Pushes a new scope for this scan operation
// 2. Dispatches to the appropriate scan-specific visitor
// 3. Handles column list exposure for scans that implement ColumnListProvider
// 4. Finalizes the scope and converts the result to a FromItem
//
// This ensures proper column scoping and availability management throughout
// the scan tree traversal, following the go-zetasql architectural pattern.
//
// Returns a FromItem suitable for use in FROM clauses of parent scans.
func (v *SQLBuilderVisitor) VisitScan(scan ast.Node) (*FromItem, error) {
	v.fragmentContext.PushScope(fmt.Sprintf("Scan(%s)", scan.Kind()))

	var fragment SQLFragment
	var err error
	switch s := scan.(type) {
	case *ast.TableScanNode:
		fragment, err = v.VisitTableScan(s, false)
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

// VisitWithEntryNode processes individual entries in WITH clauses (Common Table Expressions).
// It creates a named subquery that can be referenced by other parts of the querybuilder.
//
// The function:
// 1. Visits the subquery to get its SQL representation
// 2. Registers the WITH entry's column mappings in the fragment context
// 3. Creates a WithClause fragment with the querybuilder name and SELECT * wrapper
//
// This enables proper column resolution when the WITH entry is later referenced.
//
// Returns a WithClause fragment representing the CTE definition.
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

// VisitWithRefScanNode handles references to previously defined WITH clauses (CTEs).
// It creates a SELECT statement that references the WITH clause by name and maps
// its columns to the expected output format.
//
// The function:
// 1. Creates a SELECT statement with the WITH querybuilder name as the table
// 2. Uses stored column mappings to properly reference CTE columns
// 3. Assigns output column aliases matching the expected format
//
// This enables queries to reference CTEs defined earlier in the WITH clause.
//
// Returns a SelectStatement that references the WITH clause.
func (v *SQLBuilderVisitor) VisitWithRefScanNode(node *ast.WithRefScanNode) (SQLFragment, error) {
	//tableAlias := v.fragmentContext.AliasGenerator.GenerateTableAlias()
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
				Expression: NewColumnExpression(mapping[column.Name()], node.WithQueryName()),
			},
		)
		//v.fragmentContext.AddAvailableColumn(column, &ColumnInfo{
		//	TableAlias: tableAlias,
		//})
	}
	return selectStatement, nil
}

// VisitWithScanNode handles complete WITH statements that define multiple CTEs
// and execute a main querybuilder that can reference those CTEs.
//
// The function:
// 1. Processes all WITH entries to create CTE definitions
// 2. Visits the main querybuilder that uses those CTEs
// 3. Combines them into a SELECT statement with WITH clauses
//
// This implements ZetaSQL's WITH clause semantics in SQLite-compatible syntax.
//
// Returns a SelectStatement with the complete WITH clause structure.
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

// VisitSetOperationItemNode processes individual items in set operations (UNION, INTERSECT, EXCEPT).
// Each item represents a subquery that contributes to the set operation.
//
// Returns the FromItem representing the subquery scan.
func (v *SQLBuilderVisitor) VisitSetOperationItemNode(node *ast.SetOperationItemNode) (*FromItem, error) {
	return v.VisitScan(node.Scan())
}

// VisitSetOperationScanNode handles ZetaSQL set operations (UNION, INTERSECT, EXCEPT)
// and converts them to SQLite-compatible syntax.
//
// The function:
// 1. Maps ZetaSQL set operation types to SQLite equivalents
// 2. Processes all input items (subqueries) in the set operation
// 3. Creates a SetOperation fragment with proper type and modifier
// 4. Handles WITH clause propagation from subqueries to top level
// 5. Creates a wrapper SELECT that exposes the unified column set
//
// Set operations combine multiple queries with the same column structure.
//
// Returns a SelectStatement containing the set operation.
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
	selectStatement.FromClause = &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: setStatement,
		Alias:    v.fragmentContext.AliasGenerator.GenerateTableAlias(),
	}
	for _, col := range node.ColumnList() {
		v.fragmentContext.AddAvailableColumn(col, &ColumnInfo{})
		column := &SelectListItem{
			Expression: NewColumnExpression(col.Name(), selectStatement.FromClause.Alias),
		}
		selectStatement.SelectList = append(selectStatement.SelectList, column)
	}
	return selectStatement, nil
}

// VisitFilterScanNode handles ZetaSQL filter operations by adding WHERE clauses.
// It wraps the input scan with a SELECT statement that includes the filter condition.
//
// The function:
// 1. Visits the input scan to get the base data source
// 2. Visits the filter expression to get the WHERE condition
// 3. Creates a SELECT * statement with the WHERE clause applied
//
// This preserves all columns from the input while applying the filter.
//
// Returns a SelectStatement with the WHERE clause.
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

// VisitOrderByScanNode handles ZetaSQL ordering operations by adding ORDER BY clauses.
// It processes ORDER BY items which may include NULL ordering specifications.
//
// The function:
// 1. Visits the input scan to get the base data source
// 2. Processes each ORDER BY item, handling NULL ordering requirements
// 3. Creates a SELECT * statement with the ORDER BY clause applied
//
// Note that each OrderByItemNode may generate multiple OrderByItems to handle
// ZetaSQL's NULLS FIRST/LAST semantics in SQLite.
//
// Returns a SelectStatement with the ORDER BY clause.
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

// VisitLimitOffsetScanNode handles ZetaSQL LIMIT and OFFSET operations for pagination.
// It creates a SELECT statement with LIMIT and/or OFFSET clauses.
//
// The function:
// 1. Visits the input scan to get the base data source
// 2. Builds the SELECT list with proper column references
// 3. Adds LIMIT clause if specified
// 4. Adds OFFSET clause if specified
//
// Both LIMIT and OFFSET are optional and converted from expressions.
//
// Returns a SelectStatement with LIMIT/OFFSET clauses.
func (v *SQLBuilderVisitor) VisitLimitOffsetScanNode(node *ast.LimitOffsetScanNode) (SQLFragment, error) {
	scan, err := v.VisitScan(node.InputScan())
	if err != nil {
		return nil, err
	}

	selectStatement := NewSelectStatement()
	selectStatement.FromClause = scan

	for _, col := range node.ColumnList() {
		selectStatement.SelectList = append(selectStatement.SelectList, &SelectListItem{
			Expression: v.fragmentContext.GetColumnExpression(col),
		})
	}

	clause := &LimitClause{}

	if node.Limit() != nil {
		limit, err := v.VisitExpression(node.Limit())
		if err != nil {
			return nil, err
		}
		clause.Count = limit.(*SQLExpression)
	}

	if node.Offset() != nil {
		offset, err := v.VisitExpression(node.Offset())
		if err != nil {
			return nil, err
		}
		clause.Offset = offset.(*SQLExpression)
	}

	selectStatement.LimitClause = clause

	return selectStatement, nil
}

// VisitAnalyticScanNode handles ZetaSQL window function operations.
// It processes analytic functions (window functions) and combines them with input columns.
//
// The function:
// 1. Visits the input scan to get the base data source
// 2. Processes all analytic function groups and stores them in context
// 3. Builds a SELECT list combining input columns and analytic results
// 4. Uses the fragment context to resolve column expressions properly
//
// Analytic functions add computed columns based on window specifications
// while preserving all input columns.
//
// Returns a SelectStatement with window functions in the SELECT list.
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
			Alias:      col.Name(),
		})
	}

	return selectStatement, nil
}

// VisitTableScan converts a ZetaSQL table scan node into a SQLite FROM clause.
// It generates unique table aliases to prevent column name conflicts and creates
// column metadata for tracking output columns.
//
// The function:
// - Generates a unique table alias using the alias generator
// - Creates ColumnInfo entries for all columns in the table
// - Stores fragment metadata for later reference
// - Makes columns available in the current scope
//
// Returns a TableFromItem fragment representing the table reference.
func (v *SQLBuilderVisitor) VisitTableScan(node *ast.TableScanNode, fromOnly bool) (SQLFragment, error) {
	// Always generate a table alias to help with column disambiguation
	tableAlias := v.fragmentContext.AliasGenerator.GenerateTableAlias()
	fromItem := NewTableFromItem(
		namePathFromContext(v.context).format([]string{node.Table().Name()}),
		tableAlias,
	)

	// TODO HANDLE WILDCARD TABLES
	selectStatement := NewSelectStatement()
	selectStatement.FromClause = fromItem

	for _, col := range node.ColumnList() {
		selectStatement.SelectList = append(selectStatement.SelectList, &SelectListItem{
			Expression: NewColumnExpression(col.Name(), tableAlias),
		})

		columnInfo := &ColumnInfo{
			Type:       col.Type().Kind().String(),
			TableAlias: tableAlias,
		}

		// Make columns available in current scope
		// Store by column name for simple lookup
		v.fragmentContext.AddAvailableColumn(col, columnInfo)
	}

	if fromOnly {
		return fromItem, nil
	}
	return selectStatement, nil
}

// VisitProjectScan handles ZetaSQL projection operations by converting them into
// SQLite SELECT statements. A ProjectScan represents a SELECT with computed columns.
//
// The function:
// 1. Visits the input scan to get the base FROM clause
// 2. Processes computed expressions and makes them available in context
// 3. Builds a SELECT statement with the projected column list
// 4. Assigns column aliases using the format "col{ColumnID}"
//
// Returns a SelectStatement fragment representing the projection.
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
			Alias:      col.Name(),
		})
	}

	return stmt, nil
}

// VisitJoinScan converts ZetaSQL JOIN operations into SQLite JOIN syntax.
// It handles all join types (INNER, LEFT, RIGHT, FULL, CROSS) and properly
// manages column scoping and qualification.
//
// The function:
// 1. Visits left and right input scans
// 2. Processes the join condition after both sides are available
// 3. Creates a SelectStatement with a JOIN clause
// 4. Builds the output column list with proper column references
// 5. Makes joined columns available for parent scopes
//
// Returns a SelectStatement fragment with the JOIN operation.
func (v *SQLBuilderVisitor) VisitJoinScan(node *ast.JoinScanNode) (SQLFragment, error) {
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

	// TODO HANDLE INNER, RIGHT JOIN,FULL JOIN
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
			Type: col.Type().Kind().String(),
		}
		outputColumns = append(outputColumns, joinColumnInfo)

		// Make column available for parent scopes (without table alias since it's now from the join)
		v.fragmentContext.AddAvailableColumn(col, joinColumnInfo)
	}

	return selectStatement, nil
}

// Helper methods

// finalizeFromItem converts a SQLFragment into a FromItem suitable for use in FROM clauses.
// It handles scope cleanup and wraps complex queries in subqueries when necessary.
//
// The function:
// 1. Handles FromItem fragments directly by cleaning up their scope
// 2. Wraps SelectStatement fragments in subqueries with generated aliases
// 3. Handles nil fragments (e.g., from SingleRowScanNode)
// 4. Manages fragment context scope cleanup
//
// This ensures that all scan results can be used as FROM clause sources
// while maintaining proper scope boundaries.
//
// Returns a FromItem suitable for use in parent scan FROM clauses.
func (v *SQLBuilderVisitor) finalizeFromItem(fragment SQLFragment) *FromItem {
	switch f := fragment.(type) {
	case *FromItem:
		v.fragmentContext.PopScope(f.Alias)
		return f
	case *SelectStatement:
		// Wrap complex querybuilder in subquery
		alias := v.fragmentContext.AliasGenerator.GenerateSubqueryAlias()
		v.fragmentContext.PopScope(alias)
		return NewSubqueryFromItem(f, alias)
	case nil:
		v.fragmentContext.PopScope("")
		return nil
	default:
		panic(fmt.Sprintf("unexpected fragment type: %T", fragment))
	}
}

// VisitArrayScan implements ZetaSQL UNNEST functionality using SQLite's json_each table function.
// This converts array operations into table-valued functions that can be joined with other tables.
//
// The function:
// 1. Processes the input scan if present (for correlated arrays)
// 2. Converts the array expression using zetasqlite_decode_array
// 3. Uses json_each to unnest the array into rows
// 4. Maps 'value' column to array elements and 'key' to array indices
// 5. Handles OUTER joins for optional array elements
//
// Returns a SelectStatement with the unnest operation, potentially joined with input.
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
			Alias:      col.Name(),
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

	// Return a JOINed querybuilder combining input and UNNEST
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
