package internal

import (
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"strings"
)

func (v *SQLBuilderVisitor) getFuncNameAndArgs(node *ast.BaseFunctionCallNode, isWindowFunc bool) (string, []*SQLExpression, error) {
	ctx := v.context
	args := make([]*SQLExpression, 0, len(node.ArgumentList()))
	for _, a := range node.ArgumentList() {
		arg, err := v.VisitExpression(a)
		if err != nil {
			return "", nil, err
		}
		args = append(args, arg.(*SQLExpression))
	}
	funcName := node.Function().FullName(false)
	funcName = strings.Replace(funcName, ".", "_", -1)

	_, existsCurrentTimeFunc := currentTimeFuncMap[funcName]
	_, existsNormalFunc := normalFuncMap[funcName]
	_, existsAggregateFunc := aggregateFuncMap[funcName]
	_, existsWindowFunc := windowFuncMap[funcName]
	currentTime := CurrentTime(ctx)

	funcPrefix := "zetasqlite"
	if node.ErrorMode() == ast.SafeErrorMode {
		if !existsNormalFunc {
			return "", nil, fmt.Errorf("SAFE is not supported for function %s", funcName)
		}
		funcPrefix = "zetasqlite_safe"
	}

	if strings.HasPrefix(funcName, "$") {
		if isWindowFunc {
			funcName = fmt.Sprintf("%s_window_%s", funcPrefix, funcName[1:])
		} else {
			funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName[1:])
		}
	} else if existsCurrentTimeFunc {
		if currentTime != nil {
			args = append(
				args,
				NewLiteralExpressionFromGoValue(types.Int64Type(), currentTime.UnixNano()),
			)
		}
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if existsNormalFunc {
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if !isWindowFunc && existsAggregateFunc {
		funcName = fmt.Sprintf("%s_%s", funcPrefix, funcName)
	} else if isWindowFunc && existsWindowFunc {
		funcName = fmt.Sprintf("%s_window_%s", funcPrefix, funcName)
	} else {
		if node.Function().IsZetaSQLBuiltin() {
			return "", nil, fmt.Errorf("%s function is unimplemented", funcName)
		}
		fname, err := getFuncName(ctx, node)
		if err != nil {
			return "", nil, err
		}
		funcName = fname
	}
	return funcName, args, nil
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
			TableAlias: call.Column().Name(),
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
