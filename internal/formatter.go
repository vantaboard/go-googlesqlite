package internal

import (
	"fmt"
	parsed_ast "github.com/goccy/go-zetasql/ast"
)

import (
	"context"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

type Formatter interface {
	FormatSQL(context.Context) (string, error)
}

func getFuncName(ctx context.Context, n ast.Node) (string, error) {
	nodeMap := nodeMapFromContext(ctx)
	found := nodeMap.FindNodeFromResolvedNode(n)
	if len(found) == 0 {
		return "", fmt.Errorf("failed to find path node from function node %T", n)
	}
	var foundCallNode *parsed_ast.FunctionCallNode
	for _, node := range found {
		fcallNode, ok := node.(*parsed_ast.FunctionCallNode)
		if !ok {
			continue
		}
		foundCallNode = fcallNode
		break
	}
	if foundCallNode == nil {
		return "", fmt.Errorf("failed to find function call node from %T", n)
	}
	path, err := getPathFromNode(foundCallNode.Function())
	if err != nil {
		return "", fmt.Errorf("failed to find path: %w", err)
	}
	namePath := namePathFromContext(ctx)
	return namePath.format(path), nil
}

func getPathFromNode(n parsed_ast.Node) ([]string, error) {
	var path []string
	switch node := n.(type) {
	case *parsed_ast.IdentifierNode:
		path = append(path, node.Name())
	case *parsed_ast.PathExpressionNode:
		for _, name := range node.Names() {
			path = append(path, name.Name())
		}
	case *parsed_ast.TablePathExpressionNode:
		switch {
		case node.PathExpr() != nil:
			for _, name := range node.PathExpr().Names() {
				path = append(path, name.Name())
			}
		}
	default:
		return nil, fmt.Errorf("found unknown path node: %T", node)
	}
	return path, nil
}

var windowFunctionsIgnoreNullsByDefault = map[string]bool{
	"zetasqlite_window_percentile_disc": true,
}

func getWindowBoundarySQL(boundaryType ast.BoundaryType, literal string) string {
	switch boundaryType {
	case ast.UnboundedPrecedingType:
		return "UNBOUNDED PRECEDING"
	case ast.OffsetPrecedingType:
		return fmt.Sprintf("%s PRECEDING", literal)
	case ast.CurrentRowType:
		return "CURRENT ROW"
	case ast.OffsetFollowingType:
		return fmt.Sprintf("%s FOLLOWING", literal)
	case ast.UnboundedFollowingType:
		return "UNBOUNDED FOLLOWING"
	}
	return ""
}
