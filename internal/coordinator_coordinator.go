package internal

import (
	"fmt"
	"github.com/goccy/go-json"
	"reflect"

	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// QueryCoordinator orchestrates the transformation process by delegating to appropriate transformers
type QueryCoordinator struct {
	// Expression transformers mapped by AST node type
	expressionTransformers map[reflect.Type]ExpressionTransformer

	// Statement transformers mapped by AST node type
	statementTransformers map[reflect.Type]StatementTransformer

	// Scan transformers mapped by AST node type
	scanTransformers map[reflect.Type]ScanTransformer

	// Node data extractors
	extractor *NodeExtractor
}

// NewQueryCoordinator creates a new coordinator with default transformers
func NewQueryCoordinator(extractor *NodeExtractor) *QueryCoordinator {
	coordinator := &QueryCoordinator{
		expressionTransformers: make(map[reflect.Type]ExpressionTransformer),
		statementTransformers:  make(map[reflect.Type]StatementTransformer),
		scanTransformers:       make(map[reflect.Type]ScanTransformer),
		extractor:              extractor,
	}

	coordinator.registerDefaultTransformers()
	return coordinator
}

// registerDefaultTransformers sets up the default transformer mappings
func (c *QueryCoordinator) registerDefaultTransformers() {
	// Expression transformers
	c.RegisterExpressionTransformer(reflect.TypeOf(&ast.ColumnRefNode{}), NewColumnRefTransformer(c))
	c.RegisterExpressionTransformer(reflect.TypeOf(&ast.LiteralNode{}), NewLiteralTransformer())
	c.RegisterExpressionTransformer(reflect.TypeOf(&ast.FunctionCallNode{}), NewFunctionCallTransformer(c))
	c.RegisterExpressionTransformer(reflect.TypeOf(&ast.CastNode{}), NewCastTransformer(c))
	c.RegisterExpressionTransformer(reflect.TypeOf(&ast.ParameterNode{}), NewParameterTransformer())

	// Statement transformers
	//c.RegisterStatementTransformer(reflect.TypeOf(&ast.QueryStmtNode{}), NewSelectTransformer(c))

	// Scan transformers
	c.RegisterScanTransformer(reflect.TypeOf(&ast.TableScanNode{}), NewTableScanTransformer())
	c.RegisterScanTransformer(reflect.TypeOf(&ast.ProjectScanNode{}), NewProjectScanTransformer(c))
	c.RegisterScanTransformer(reflect.TypeOf(&ast.FilterScanNode{}), NewFilterScanTransformer(c))
	c.RegisterScanTransformer(reflect.TypeOf(&ast.JoinScanNode{}), NewJoinScanTransformer(c))
	c.RegisterScanTransformer(reflect.TypeOf(&ast.AggregateScanNode{}), NewAggregateScanTransformer(c))
	c.RegisterScanTransformer(reflect.TypeOf(&ast.OrderByScanNode{}), NewOrderByScanTransformer(c))
	c.RegisterScanTransformer(reflect.TypeOf(&ast.LimitOffsetScanNode{}), NewLimitScanTransformer(c))
	c.RegisterScanTransformer(reflect.TypeOf(&ast.SingleRowScanNode{}), NewSingleRowScanTransformer(c))
	c.RegisterScanTransformer(reflect.TypeOf(&ast.WithScanNode{}), NewWithScanTransformer(c))
	c.RegisterScanTransformer(reflect.TypeOf(&ast.WithRefScanNode{}), NewWithRefScanTransformer(c))
	c.RegisterScanTransformer(reflect.TypeOf(&ast.ArrayScanNode{}), NewArrayScanTransformer(c))
}

// RegisterExpressionTransformer registers a transformer for a specific expression node type
func (c *QueryCoordinator) RegisterExpressionTransformer(nodeType reflect.Type, transformer ExpressionTransformer) {
	c.expressionTransformers[nodeType] = transformer
}

// RegisterStatementTransformer registers a transformer for a specific statement node type
func (c *QueryCoordinator) RegisterStatementTransformer(nodeType reflect.Type, transformer StatementTransformer) {
	c.statementTransformers[nodeType] = transformer
}

// RegisterScanTransformer registers a transformer for a specific scan node type
func (c *QueryCoordinator) RegisterScanTransformer(nodeType reflect.Type, transformer ScanTransformer) {
	c.scanTransformers[nodeType] = transformer
}

// TransformStatement transforms a statement AST node to SQLFragment
func (c *QueryCoordinator) TransformStatementNode(node ast.Node, ctx TransformContext) (SQLFragment, error) {
	if node == nil {
		return nil, fmt.Errorf("cannot transform nil statement node")
	}

	debug := ctx.Config().Debug
	if debug {
		fmt.Println("--- AST:")
		fmt.Println(node.DebugString())
	}

	nodeType := reflect.TypeOf(node)
	transformer, exists := c.statementTransformers[nodeType]
	if !exists {
		return nil, fmt.Errorf("no transformer registered for statement node type: %v", nodeType)
	}

	// Extract pure data from the AST node
	data, err := c.extractor.ExtractStatementData(node, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract statement data from %v: %w", nodeType, err)
	}

	token := ctx.FragmentContext().EnterScope()
	defer ctx.FragmentContext().ExitScope(token)

	if debug {
		j, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		fmt.Println("--- EXTRACTED DATA:")
		fmt.Println(string(j))
	}

	// Delegate to the appropriate transformer
	result, err := transformer.Transform(data, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform statement %v: %w", nodeType, err)
	}

	if debug {
		fmt.Println("--- FORMATTED QUERY:")
		fmt.Println(result)
		fmt.Println("---")
	}

	return result, nil
}

// Data-based transformation methods (for transformers working with pure data)

// TransformExpressionData transforms expression data to SQLExpression
func (c *QueryCoordinator) TransformExpression(exprData ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	// Route based on expression data type
	var transformer ExpressionTransformer
	var exists bool

	switch exprData.Type {
	case ExpressionTypeLiteral:
		transformer, exists = c.expressionTransformers[reflect.TypeOf(&ast.LiteralNode{})]
	case ExpressionTypeFunction:
		transformer, exists = c.expressionTransformers[reflect.TypeOf(&ast.FunctionCallNode{})]
	case ExpressionTypeCast:
		transformer, exists = c.expressionTransformers[reflect.TypeOf(&ast.CastNode{})]
	case ExpressionTypeColumn:
		transformer, exists = c.expressionTransformers[reflect.TypeOf(&ast.ColumnRefNode{})]
	case ExpressionTypeSubquery:
		transformer, exists = c.expressionTransformers[reflect.TypeOf(&ast.SubqueryExprNode{})]
	case ExpressionTypeParameter:
		transformer, exists = c.expressionTransformers[reflect.TypeOf(&ast.ParameterNode{})]
	default:
		return nil, fmt.Errorf("unsupported expression data type: %v", exprData.Type)
	}

	if !exists || transformer == nil {
		return nil, fmt.Errorf("no transformer registered for expression data type: %v", exprData.Type)
	}

	return transformer.Transform(exprData, ctx)
}

// TransformStatementData transforms statement data to SQLFragment
func (c *QueryCoordinator) TransformStatement(stmtData StatementData, ctx TransformContext) (SQLFragment, error) {
	// Route based on statement data type
	var transformer StatementTransformer
	var exists bool

	token := ctx.FragmentContext().EnterScope()
	defer ctx.FragmentContext().ExitScope(token)

	switch stmtData.Type {
	case StatementTypeSelect:
		transformer, exists = c.statementTransformers[reflect.TypeOf(&ast.QueryStmtNode{})]
	case StatementTypeInsert:
		transformer, exists = c.statementTransformers[reflect.TypeOf(&ast.InsertStmtNode{})]
	case StatementTypeUpdate:
		transformer, exists = c.statementTransformers[reflect.TypeOf(&ast.UpdateStmtNode{})]
	case StatementTypeDelete:
		transformer, exists = c.statementTransformers[reflect.TypeOf(&ast.DeleteStmtNode{})]
	case StatementTypeCreate:
		// For CREATE statements, we need to check the create type
		if stmtData.Create != nil {
			switch stmtData.Create.Type {
			case CreateTypeTable:
				transformer, exists = c.statementTransformers[reflect.TypeOf(&ast.CreateTableStmtNode{})]
			case CreateTypeView:
				transformer, exists = c.statementTransformers[reflect.TypeOf(&ast.CreateViewStmtNode{})]
			case CreateTypeFunction:
				transformer, exists = c.statementTransformers[reflect.TypeOf(&ast.CreateFunctionStmtNode{})]
			}
		}
	case StatementTypeDrop:
		// For DROP statements, we use the same transformer for both DropStmt and DropFunctionStmt
		transformer, exists = c.statementTransformers[reflect.TypeOf(&ast.DropStmtNode{})]
	default:
		return nil, fmt.Errorf("unsupported statement data type: %v", stmtData.Type)
	}

	if !exists {
		return nil, fmt.Errorf("no transformer registered for statement data type: %v", stmtData.Type)
	}

	return transformer.Transform(stmtData, ctx)
}

// TransformScanData transforms scan data to FromItem
func (c *QueryCoordinator) TransformScan(scanData ScanData, ctx TransformContext) (*FromItem, error) {
	// Route based on scan data type
	var transformer ScanTransformer
	var exists bool

	token := ctx.FragmentContext().EnterScope()
	defer ctx.FragmentContext().ExitScope(token)

	var alias string
	switch scanData.Type {
	case ScanTypeTable:
		alias = "table_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.TableScanNode{})]
	case ScanTypeJoin:
		alias = "join_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.JoinScanNode{})]
	case ScanTypeFilter:
		alias = "filter_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.FilterScanNode{})]
	case ScanTypeProject:
		alias = "project_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.ProjectScanNode{})]
	case ScanTypeAggregate:
		alias = "aggregate_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.AggregateScanNode{})]
	case ScanTypeOrderBy:
		alias = "order_by_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.OrderByScanNode{})]
	case ScanTypeLimit:
		alias = "limit_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.LimitOffsetScanNode{})]
	case ScanTypeSetOp:
		alias = "set_op_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.SetOperationScanNode{})]
	case ScanTypeSingleRow:
		alias = "single_row_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.SingleRowScanNode{})]
	case ScanTypeWith:
		alias = "with_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.WithScanNode{})]
	case ScanTypeWithRef:
		alias = "with_ref_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.WithRefScanNode{})]
	case ScanTypeWithEntry:
		alias = "with_entry_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.WithEntryNode{})]
	case ScanTypeArray:
		alias = "array_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.ArrayScanNode{})]
	case ScanTypeAnalytic:
		alias = "analytic_scan"
		transformer, exists = c.scanTransformers[reflect.TypeOf(&ast.AnalyticScanNode{})]

	default:
		return nil, fmt.Errorf("unsupported scan data type: %v", scanData.Type)
	}

	if !exists {
		return nil, fmt.Errorf("no transformer registered for scan data type: %v", scanData.Type)
	}

	fromItem, err := transformer.Transform(scanData, ctx)

	if err != nil {
		return nil, fmt.Errorf("failed to transform scan data: %w", err)
	}

	alias = fmt.Sprintf("%s_%s", alias, ctx.FragmentContext().GetID())
	fromItem.Alias = alias

	// Verify output column names against extracted data and add to scope
	if err := c.validateColumnData(fromItem, scanData.ColumnList, ctx); err != nil {
		return nil, fmt.Errorf("column validation failed for %v: %w", scanData.Type, err)
	}

	// Add available column expressions
	for _, column := range scanData.ColumnList {
		ctx.FragmentContext().AddAvailableColumn(column.ID, &ColumnInfo{
			Name: column.Name,
			ID:   column.ID,
		})
	}

	// Register scope mappings for output columns
	ctx.FragmentContext().RegisterColumnScopeMapping(alias, scanData.ColumnList)

	return fromItem, err
}

// TransformWithEntryData transforms WITH entry data to WithClause
func (c *QueryCoordinator) TransformWithEntry(scanData ScanData, ctx TransformContext) (*WithClause, error) {
	if scanData.Type != ScanTypeWithEntry {
		return nil, fmt.Errorf("expected WITH entry data, got type %v", scanData.Type)
	}

	token := ctx.FragmentContext().EnterScope()
	defer ctx.FragmentContext().ExitScope(token)

	// Create a WithEntryTransformer for this transformation
	transformer := NewWithEntryTransformer(c)
	return transformer.Transform(scanData, ctx)
}

// Helper methods for data-based transformations

// TransformExpressionDataList transforms a list of expression data
func (c *QueryCoordinator) TransformExpressionDataList(exprDataList []ExpressionData, ctx TransformContext) ([]*SQLExpression, error) {
	if exprDataList == nil {
		return nil, nil
	}

	result := make([]*SQLExpression, 0, len(exprDataList))
	for i, exprData := range exprDataList {
		expr, err := c.TransformExpression(exprData, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform expression data at index %d: %w", i, err)
		}
		result = append(result, expr)
	}

	return result, nil
}

// TransformOptionalExpressionData transforms optional expression data
func (c *QueryCoordinator) TransformOptionalExpressionData(exprData *ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	if exprData == nil {
		return nil, nil
	}
	return c.TransformExpression(*exprData, ctx)
}

// GetRegisteredExpressionTypes returns the types of registered expression transformers
func (c *QueryCoordinator) GetRegisteredExpressionTypes() []string {
	types := make([]string, 0, len(c.expressionTransformers))
	for t := range c.expressionTransformers {
		types = append(types, t.Name())
	}
	return types
}

// GetRegisteredStatementTypes returns the types of registered statement transformers
func (c *QueryCoordinator) GetRegisteredStatementTypes() []string {
	types := make([]string, 0, len(c.statementTransformers))
	for t := range c.statementTransformers {
		types = append(types, t.Name())
	}
	return types
}

// GetRegisteredScanTypes returns the types of registered scan transformers
func (c *QueryCoordinator) GetRegisteredScanTypes() []string {
	types := make([]string, 0, len(c.scanTransformers))
	for t := range c.scanTransformers {
		types = append(types, t.Name())
	}
	return types
}

// validateColumnData validates that output columns in a transformed scan's SelectList
// use id-based aliases and match what are held in the ScanData.ColumnList.
// For Select Star subqueries, recursively uses the subquery's SelectList.
func (c *QueryCoordinator) validateColumnData(fromItem *FromItem, expectedColumns []*ColumnData, ctx TransformContext) error {
	if fromItem == nil || len(expectedColumns) == 0 {
		return nil
	}

	var selectList []*SelectListItem

	// Get the SelectList to validate - handle different FromItem types
	switch fromItem.Type {
	case FromItemTypeSubquery:
		if fromItem.Subquery == nil {
			return fmt.Errorf("subquery FromItem has nil Subquery")
		}
		selectList = c.getSelectListRecursive(fromItem.Subquery)
	case FromItemTypeTable:
		// Table scans don't have SelectLists to validate
		return nil
	default:
		// For other types like joins, we may not have a direct SelectList
		return nil
	}

	if selectList == nil {
		return fmt.Errorf("could not extract SelectList from FromItem")
	}

	// Validate that we have the same number of columns
	if len(selectList) != len(expectedColumns) {
		return fmt.Errorf("SelectList length mismatch: got %d items, expected %d columns",
			len(selectList), len(expectedColumns))
	}

	// Generate expected aliases using generateIDBasedAlias
	expectedAliases := make(map[string]bool)
	for _, col := range expectedColumns {
		expectedAlias := generateIDBasedAlias(col.Name, col.ID)
		expectedAliases[expectedAlias] = true
	}

	// Validate each SelectListItem
	for i, item := range selectList {
		if item.Alias == "" {
			return fmt.Errorf("SelectListItem at index %d has empty alias", i)
		}

		// Verify the alias matches one of our expected id-based aliases
		if !expectedAliases[item.Alias] {
			return fmt.Errorf("SelectListItem at index %d has unexpected alias '%s', not found in expected id-based aliases",
				i, item.Alias)
		}
	}

	return nil
}

// getSelectListRecursive extracts SelectList from a SelectStatement, handling Select Star subqueries recursively
func (c *QueryCoordinator) getSelectListRecursive(stmt *SelectStatement) []*SelectListItem {
	if stmt == nil || len(stmt.SelectList) == 0 {
		return nil
	}

	// Check if this is a Select Star query
	if len(stmt.SelectList) == 1 {
		item := stmt.SelectList[0]
		if item.IsStarExpansion || (item.Expression != nil && item.Expression.Type == ExpressionTypeStar) {
			// This is a SELECT * - we need to recurse into the FROM clause
			if stmt.FromClause != nil && stmt.FromClause.Type == FromItemTypeSubquery && stmt.FromClause.Subquery != nil {
				return c.getSelectListRecursive(stmt.FromClause.Subquery)
			}
			if stmt.FromClause != nil && stmt.FromClause.Type == FromItemTypeJoin {
				items := []*SelectListItem{}
				items = append(items, c.getSelectListRecursive(stmt.FromClause.Join.Left.Subquery)...)
				items = append(items, c.getSelectListRecursive(stmt.FromClause.Join.Right.Subquery)...)
				return items
			}
		}

	}

	// Return the current SelectList
	return stmt.SelectList
}
