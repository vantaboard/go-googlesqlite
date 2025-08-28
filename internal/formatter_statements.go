package internal

import (
	"context"
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
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
