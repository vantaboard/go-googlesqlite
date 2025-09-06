package internal

import (
	"context"
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
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

// VisitQuery Formats the outermost query statement that runs and produces rows of output, like a SELECT
// The node's `OutputColumnList()` gives user-visible column names that should be returned. There may be duplicate names,
// and multiple output columns may reference the same column from `Query()`
// https://github.com/google/zetasql/blob/master/docs/resolved_ast.md#ResolvedQueryStmt
func (v *SQLBuilderVisitor) VisitQuery(node *ast.QueryStmtNode) (SQLFragment, error) {
	scan, err := v.VisitScan(node.Query())
	if err != nil {
		return nil, fmt.Errorf("failed to visit query: %w", err)
	}

	selectStatement := NewSelectStatement()
	selectStatement.FromClause = scan

	for _, column := range node.OutputColumnList() {
		expr, err := v.VisitOutputColumnNode(column)
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

// VisitSQL methods for DDL statements

// VisitCreateTableStmt for CreateTableStmtNode processes CREATE TABLE statements
func (v *SQLBuilderVisitor) VisitSQL(node *ast.CreateTableStmtNode) (SQLFragment, error) {
	createTable := &CreateTableStatement{
		IfNotExists: node.CreateMode() == ast.CreateIfNotExistsMode,
		TableName:   namePathFromContext(v.context).format(node.NamePath()),
	}

	primaryKeyColumns := make(map[string]struct{})
	for _, val := range node.PrimaryKey().ColumnNameList() {
		primaryKeyColumns[val] = struct{}{} // empty struct uses zero memory
	}
	// Handle regular CREATE TABLE with columns
	for _, colDef := range node.ColumnDefinitionList() {
		_, isPrimaryKey := primaryKeyColumns[colDef.Name()]

		column := &ColumnDefinition{
			Name:         colDef.Name(),
			Type:         convertZetaSQLTypeToSQLite(colDef.Type()),
			NotNull:      colDef.Annotations().NotNull(),
			IsPrimaryKey: isPrimaryKey,
		}

		if colDef.GeneratedColumnInfo() != nil && colDef.GeneratedColumnInfo().Expression() != nil {
			defaultExpr, err := v.VisitExpression(colDef.GeneratedColumnInfo().Expression())
			if err != nil {
				return nil, fmt.Errorf("failed to visit default expression: %w", err)
			}
			column.DefaultValue = defaultExpr.(*SQLExpression)
		}

		createTable.Columns = append(createTable.Columns, column)
	}

	return createTable, nil
}

// VisitCreateTableAsSelectStmt for CreateTableAsSelectStmtNode processes CREATE TABLE AS SELECT statements
func (v *SQLBuilderVisitor) VisitCreateTableAsSelectStmt(node *ast.CreateTableAsSelectStmtNode) (SQLFragment, error) {
	createTable := &CreateTableStatement{
		IfNotExists: node.CreateMode() == ast.CreateIfNotExistsMode,
		TableName:   namePathFromContext(v.context).format(node.NamePath()),
	}

	scan, err := v.VisitScan(node.Query())
	if err != nil {
		return nil, fmt.Errorf("failed to visit query in CREATE TABLE AS SELECT: %w", err)
	}

	query, err := v.visitOutputColumnProvider(scan, node.OutputColumnList())
	if err != nil {
		return nil, err
	}

	createTable.AsSelect = query

	return createTable, nil
}

// VisitCreateFunctionStmt for CreateFunctionStmtNode processes CREATE FUNCTION statements
func (v *SQLBuilderVisitor) VisitCreateFunctionStmt(node *ast.CreateFunctionStmtNode) (SQLFragment, error) {
	createFunction := &CreateFunctionStatement{
		IfNotExists:  node.CreateMode() == ast.CreateIfNotExistsMode,
		FunctionName: node.NamePath()[len(node.NamePath())-1],
		Language:     node.Language(),
		Code:         node.Code(),
		Options:      make(map[string]*SQLExpression),
	}

	// Handle function signature
	if node.Signature() != nil {
		for _, arg := range node.Signature().Arguments() {
			param := &ParameterDefinition{
				Name: arg.ArgumentName(),
				Type: convertZetaSQLTypeToSQLite(arg.Type()),
			}
			createFunction.Parameters = append(createFunction.Parameters, param)
		}
		if node.Signature().ResultType() != nil {
			createFunction.ReturnType = convertZetaSQLTypeToSQLite(node.Signature().ResultType())
		}
	}

	// Handle options
	for _, option := range node.OptionList() {
		if option.Value() != nil {
			optionExpr, err := v.VisitExpression(option.Value())
			if err != nil {
				return nil, fmt.Errorf("failed to visit function option: %w", err)
			}
			createFunction.Options[option.Name()] = optionExpr.(*SQLExpression)
		}
	}

	return createFunction, nil
}

// VisitDropStmt for DropStmtNode processes DROP statements
func (v *SQLBuilderVisitor) VisitDropStmt(node *ast.DropStmtNode) (SQLFragment, error) {
	objectType := "TABLE"
	switch node.ObjectType() {
	case "TABLE":
		objectType = "TABLE"
	case "VIEW":
		objectType = "VIEW"
	case "INDEX":
		objectType = "INDEX"
	case "SCHEMA":
		objectType = "SCHEMA"
	default:
		objectType = node.ObjectType()
	}

	return &DropStatement{
		IfExists:   node.IsIfExists(),
		ObjectType: objectType,
		ObjectName: namePathFromContext(v.context).format(node.NamePath()),
	}, nil
}

// VisitDropFunctionStmt for DropFunctionStmtNode processes DROP FUNCTION statements
func (v *SQLBuilderVisitor) VisitDropFunctionStmt(node *ast.DropFunctionStmtNode) (SQLFragment, error) {
	return &DropStatement{
		IfExists:   node.IsIfExists(),
		ObjectType: "FUNCTION",
		ObjectName: node.NamePath()[len(node.NamePath())-1],
	}, nil
}

// VisitTruncateStmt for TruncateStmtNode processes TRUNCATE statements
func (v *SQLBuilderVisitor) VisitTruncateStmt(node *ast.TruncateStmtNode) (SQLFragment, error) {
	return &TruncateStatement{
		TableName: node.TableScan().Table().Name(),
	}, nil
}

//
//// VisitMergeStmt for MergeStmtNode processes MERGE statements
//func (v *SQLBuilderVisitor) VisitMergeStmt(node *ast.MergeStmtNode) (SQLFragment, error) {
//	mergeStmt := &MergeStatement{
//		TargetTable: node.TableScan().Table().Name(),
//	}
//
//	// Handle source table
//	if node.FromScan() != nil {
//		source, err := v.VisitScan(node.FromScan())
//		if err != nil {
//			return nil, fmt.Errorf("failed to visit source scan in MERGE: %w", err)
//		}
//		mergeStmt.SourceTable = source
//	}
//
//	// Handle merge condition
//	if node.MergeExpr() != nil {
//		mergeExpr, err := v.VisitExpression(node.MergeExpr())
//		if err != nil {
//			return nil, fmt.Errorf("failed to visit merge expression: %w", err)
//		}
//		mergeStmt.MergeClause = mergeExpr.(*SQLExpression)
//	}
//
//	// Handle WHEN clauses
//	for _, whenClause := range node.WhenClauseList() {
//		when := &MergeWhenClause{}
//
//		switch whenClause.MatchType() {
//		case ast.MatchTypeMatched:
//			when.Type = "MATCHED"
//		case ast.MatchTypeNotMatchedBySource:
//			when.Type = "NOT MATCHED"
//		}
//
//		if whenClause.MatchExpr() != nil {
//			condition, err := v.VisitExpression(whenClause.MatchExpr())
//			if err != nil {
//				return nil, fmt.Errorf("failed to visit when condition: %w", err)
//			}
//			when.Condition = condition.(*SQLExpression)
//		}
//
//		switch whenClause.ActionType() {
//		case ast.ActionTypeUpdate:
//			when.Action = "UPDATE"
//		case ast.ActionTypeDelete:
//			when.Action = "DELETE"
//		case ast.ActionTypeInsert:
//			when.Action = "INSERT"
//		}
//
//		mergeStmt.WhenClauses = append(mergeStmt.WhenClauses, when)
//	}
//
//	return mergeStmt, nil
//}

func (v *SQLBuilderVisitor) VisitUpdateStatement(node *ast.UpdateStmtNode) (SQLFragment, error) {
	scan, err := v.VisitTableScan(node.TableScan(), true)
	if err != nil {
		return nil, err
	}

	var fromItem *FromItem
	if node.FromScan() != nil {
		fromItem, err = v.VisitScan(node.FromScan())
		if err != nil {
			return nil, err
		}
	}

	items := []*SetItem{}
	for _, item := range node.UpdateItemList() {
		sql, err := v.VisitUpdateItem(item)
		if err != nil {
			return nil, err
		}
		items = append(items, sql)
	}
	where, err := v.VisitExpression(node.WhereExpr())
	if err != nil {
		return nil, err
	}
	return &UpdateStatement{
		Table:       scan.(*FromItem),
		FromClause:  fromItem,
		SetItems:    items,
		WhereClause: where.(*SQLExpression),
	}, nil
}

func (v *SQLBuilderVisitor) VisitUpdateItem(node *ast.UpdateItemNode) (*SetItem, error) {
	target, err := v.VisitExpression(node.Target())
	if err != nil {
		return nil, err
	}
	setValue, err := v.VisitExpression(node.SetValue())
	if err != nil {
		return nil, err
	}
	return &SetItem{
		// SQLite does not allow for updating multiple tables as once
		// as such, it does not accept table-qualified column names
		// so we use only the column name here
		Column: NewLiteralExpression(strings.Split(target.String(), ".")[1]),
		Value:  setValue.(*SQLExpression),
	}, nil
}

func (v *SQLBuilderVisitor) VisitDeleteStatement(node *ast.DeleteStmtNode) (SQLFragment, error) {
	from, err := v.VisitTableScan(node.TableScan(), true)
	if err != nil {
		return nil, err
	}

	where, err := v.VisitExpression(node.WhereExpr())
	if err != nil {
		return nil, err
	}
	return &DeleteStatement{
		Table:     from,
		WhereExpr: where,
	}, nil
}

func (v *SQLBuilderVisitor) VisitDMLValueNode(node *ast.DMLValueNode) (SQLFragment, error) {
	return v.VisitExpression(node.Value())
}

func (v *SQLBuilderVisitor) VisitInsertRowNode(node *ast.InsertRowNode) (SQLFragment, error) {
	values := []string{}
	for _, value := range node.ValueList() {
		sql, err := v.VisitDMLValueNode(value)
		if err != nil {
			return nil, err
		}
		values = append(values, sql.String())
	}
	return &SQLExpression{
		Type:  ExpressionTypeLiteral,
		Value: strings.Join(values, ","),
	}, nil
}

func (v *SQLBuilderVisitor) VisitInsertStatement(node *ast.InsertStmtNode) (SQLFragment, error) {
	table := node.TableScan().Table().Name()
	columns := []string{}
	for _, col := range node.InsertColumnList() {
		columns = append(columns, fmt.Sprintf("`%s`", col.Name()))
	}

	query := node.Query()
	if query != nil {
		query, err := v.VisitScan(query)
		if err != nil {
			return nil, err
		}

		return &InsertStatement{
			TableName: table,
			Columns:   columns,
			Query:     NewSelectStarStatement(query),
		}, nil
	}

	rows := []SQLFragment{}
	for _, row := range node.RowList() {
		sql, err := v.VisitInsertRowNode(row)
		if err != nil {
			return nil, err
		}
		rows = append(rows, sql.(SQLFragment))
	}
	return &InsertStatement{
		TableName: namePathFromContext(v.context).format([]string{table}),
		Columns:   columns,
		Rows:      rows,
	}, nil
}

func (v *SQLBuilderVisitor) VisitDMLStatement(node ast.Node) (SQLFragment, error) {
	switch node.(type) {
	case *ast.DeleteStmtNode:
		return v.VisitDeleteStatement(node.(*ast.DeleteStmtNode))
	case *ast.InsertStmtNode:
		return v.VisitInsertStatement(node.(*ast.InsertStmtNode))
	case *ast.UpdateStmtNode:
		return v.VisitUpdateStatement(node.(*ast.UpdateStmtNode))
	default:
		return nil, fmt.Errorf("unsupported DML statement: %T", node)
	}
}

func (v *SQLBuilderVisitor) VisitDMLDefaultNode(node *ast.DMLDefaultNode) (SQLFragment, error) {
	return &SQLExpression{
		Type:  ExpressionTypeLiteral,
		Value: "",
	}, nil
}

func (v *SQLBuilderVisitor) VisitCreateViewStatement(node *ast.CreateViewStmtNode) (SQLFragment, error) {
	// Visit scan first to produce column scope
	scan, err := v.VisitScan(node.Query())
	if err != nil {
		return nil, err
	}

	query, err := v.visitOutputColumnProvider(scan, node.OutputColumnList())
	if err != nil {
		return nil, err
	}

	return &CreateViewStatement{
		Query:    query,
		ViewName: namePathFromContext(v.context).format(node.NamePath()),
	}, nil

}

type OutputColumnListProvider interface {
	OutputColumnList() []ast.OutputColumnNode
}

func (v *SQLBuilderVisitor) visitOutputColumnProvider(query *FromItem, columns []*ast.OutputColumnNode) (*SelectStatement, error) {
	selectStatement := NewSelectStatement()
	selectStatement.FromClause = query
	for _, col := range columns {
		expr, err := v.VisitExpression(col)
		if err != nil {
			return nil, err
		}

		selectStatement.SelectList = append(selectStatement.SelectList, &SelectListItem{
			Expression: expr.(*SQLExpression),
			Alias:      col.Name(),
		})
	}

	return selectStatement, nil
}

const MERGED_TABLE = "zetasqlite_merged_table"

/*
MergeStmt
+-table_scan=
| +-TableScan(parse_location=728-737, column_list=Inventory.[product#1, quantity#2, supply_constrained#3], table=Inventory, column_index_list=[0, 1, 2], alias="I")
+-column_access_list=READ_WRITE,READ_WRITE,WRITE
+-from_scan=
| +-TableScan(parse_location=749-752, column_list=tmp.[product#4, quantity#5, warehouse#6], table=tmp, column_index_list=[0, 1, 2], alias="T")
+-merge_expr=
| +-FunctionCall(ZetaSQL:$equal(STRING, STRING) -> BOOL)
|   +-parse_location=761-782
|   +-ColumnRef(parse_location=761-770, type=STRING, column=Inventory.product#1)
|   +-ColumnRef(parse_location=773-782, type=STRING, column=tmp.product#4)
+-when_clause_list=
  +-MergeWhen
  | +-match_type=NOT_MATCHED_BY_TARGET
  | +-action_type=INSERT
  | +-insert_column_list=Inventory.[product#1, quantity#2, supply_constrained#3]
  | +-insert_row=
  |   +-InsertRow
  |     +-value_list=
  |       +-DMLValue
  |       | +-value=
  |       |   +-ColumnRef(parse_location=860-867, type=STRING, column=tmp.product#4)
  |       +-DMLValue
  |       | +-value=
  |       |   +-ColumnRef(parse_location=869-877, type=INT64, column=tmp.quantity#5)
  |       +-DMLValue
  |         +-value=
  |           +-Literal(parse_location=879-884, type=BOOL, value=false)
  +-MergeWhen
    +-match_type=MATCHED
    +-action_type=UPDATE
    +-update_item_list=
      +-UpdateItem
        +-target=
        | +-ColumnRef(parse_location=916-924, type=INT64, column=Inventory.quantity#2)
        +-set_value=
          +-DMLValue
            +-value=
              +-FunctionCall(ZetaSQL:$add(INT64, INT64) -> INT64)
                +-parse_location=927-950
                +-ColumnRef(parse_location=927-937, type=INT64, column=Inventory.quantity#2)
                +-ColumnRef(parse_location=940-950, type=INT64, column=tmp.quantity#5)
*/

func (v *SQLBuilderVisitor) VisitMergeStatement(node *ast.MergeStmtNode) ([]*SQLExpression, error) {
	return nil, fmt.Errorf("not implemented")
	//targetTable, err := v.VisitTableScan(node.TableScan(), false)
	//if err != nil {
	//	return nil, err
	//}
	//fromScan, ok := node.FromScan().(*ast.TableScanNode)
	//if !ok {
	//	return nil, fmt.Errorf("unexpected FROM scan. expected TableScanNode but got %T", node.FromScan())
	//}
	//sourceTable, err := v.VisitTableScan(fromScan, false)
	//if err != nil {
	//	return nil, err
	//}
	//expr, err := v.VisitExpression(node.MergeExpr())
	//if err != nil {
	//	return nil, err
	//}
	//fn, ok := node.MergeExpr().(*ast.FunctionCallNode)
	//if !ok {
	//	return nil, fmt.Errorf("currently MERGE expression is supported equal expression only")
	//}
	//if fn.Function().FullName(false) != "$equal" {
	//	return nil, fmt.Errorf("currently MERGE expression is supported equal expression only")
	//}
	//argList := fn.ArgumentList()
	//if len(argList) != 2 {
	//	return nil, fmt.Errorf("unexpected MERGE expression column num. expected 2 column but specified %d column", len(args))
	//}

}

// convertZetaSQLTypeToSQLite converts ZetaSQL types to SQLite types
func convertZetaSQLTypeToSQLite(zetaType interface{}) string {
	// This is a simplified type conversion - may need to be expanded
	if zetaType == nil {
		return "TEXT"
	}

	// For now, return a basic type mapping
	// This would need to be expanded based on actual ZetaSQL type system
	return "TEXT"
}
