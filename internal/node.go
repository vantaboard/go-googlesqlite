package internal

import (
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

func newNode(node ast.Node) Formatter {
	if node == nil {
		return nil
	}

	return nil
}
