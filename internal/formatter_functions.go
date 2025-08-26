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
