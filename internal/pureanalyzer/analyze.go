package pureanalyzer

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/vantaboard/go-googlesql/types"
)

// AnalyzeSelect parses and resolves a narrow SELECT subset against catalog.
func AnalyzeSelect(sql string, catalog types.Catalog) (*AnalyzedSelect, error) {
	if catalog == nil {
		return nil, fmt.Errorf("pureanalyzer: nil catalog")
	}
	ps, err := parseSelectQuery(sql)
	if err != nil {
		return nil, err
	}
	tab, err := catalog.FindTable([]string{ps.table})
	if err != nil {
		return nil, err
	}
	if tab == nil {
		return nil, fmt.Errorf("pureanalyzer: table not found %q", ps.table)
	}

	colTypes := map[string]TypeKind{}
	for i := 0; i < tab.NumColumns(); i++ {
		c := tab.Column(i)
		colTypes[strings.ToLower(c.Name())] = typeKindFromTypes(c.Type())
	}

	out := &AnalyzedSelect{
		Table:  tab.Name(),
		RawSQL: ps.source,
	}

	if ps.star {
		out.SelectStar = true
		for i := 0; i < tab.NumColumns(); i++ {
			c := tab.Column(i)
			out.OutputCols = append(out.OutputCols, OutputColumn{
				Name: c.Name(),
				Type: typeKindFromTypes(c.Type()),
			})
		}
	} else {
		for _, cn := range ps.cols {
			col := tab.FindColumnByName(cn)
			if col == nil {
				return nil, fmt.Errorf("pureanalyzer: unknown column %q", cn)
			}
			out.OutputCols = append(out.OutputCols, OutputColumn{
				Name: col.Name(),
				Type: typeKindFromTypes(col.Type()),
			})
		}
	}

	if ps.where != nil {
		w, err := resolveExpr(ps.where, colTypes, nil)
		if err != nil {
			return nil, err
		}
		out.Where = w
	}

	return out, nil
}

func typeKindFromTypes(t types.Type) TypeKind {
	if t == nil {
		return TypeUnknown
	}
	switch t.Kind() {
	case types.INT64:
		return TypeInt64
	case types.STRING:
		return TypeString
	case types.BOOL:
		return TypeBool
	case types.DOUBLE, types.FLOAT:
		return TypeDouble
	case types.BYTES:
		return TypeBytes
	case types.DATE:
		return TypeDate
	case types.TIMESTAMP:
		return TypeTimestamp
	default:
		return TypeUnknown
	}
}

func resolveExpr(pe *parseExpr, cols map[string]TypeKind, hint *TypeKind) (*Expr, error) {
	if pe == nil {
		return nil, nil
	}
	if pe.fn != "" {
		return resolveCall(pe, cols)
	}
	if pe.op != "" {
		return resolveBinary(pe, cols)
	}
	switch pe.litKind {
	case "ident":
		if strings.EqualFold(pe.lit, "true") {
			return &Expr{Kind: ExprLiteral, Type: TypeBool, Literal: "true"}, nil
		}
		if strings.EqualFold(pe.lit, "false") {
			return &Expr{Kind: ExprLiteral, Type: TypeBool, Literal: "false"}, nil
		}
		tk, ok := cols[strings.ToLower(pe.lit)]
		if !ok {
			return nil, fmt.Errorf("pureanalyzer: unknown column %q", pe.lit)
		}
		return &Expr{Kind: ExprColumn, Type: tk, Name: pe.lit}, nil
	case "int":
		return &Expr{Kind: ExprLiteral, Type: TypeInt64, Literal: pe.lit}, nil
	case "string":
		return &Expr{Kind: ExprLiteral, Type: TypeString, Literal: strconv.Quote(pe.lit)}, nil
	case "param_named":
		pt := TypeInt64
		if hint != nil {
			pt = *hint
		}
		return &Expr{Kind: ExprParam, Type: pt, Param: &ParamRef{Name: pe.lit}}, nil
	case "param_pos":
		pt := TypeInt64
		if hint != nil {
			pt = *hint
		}
		return &Expr{Kind: ExprParam, Type: pt, Param: &ParamRef{Position: 1}}, nil
	default:
		return nil, fmt.Errorf("%w: leaf %q", ErrUnsupportedFeature, pe.litKind)
	}
}

func resolveBinary(pe *parseExpr, cols map[string]TypeKind) (*Expr, error) {
	if strings.EqualFold(pe.op, "AND") || strings.EqualFold(pe.op, "OR") {
		l, err := resolveExpr(pe.left, cols, nil)
		if err != nil {
			return nil, err
		}
		r, err := resolveExpr(pe.right, cols, nil)
		if err != nil {
			return nil, err
		}
		op := strings.ToUpper(pe.op)
		return &Expr{
			Kind:     ExprCall,
			Type:     TypeBool,
			Name:     mapBinaryToFuncName(op),
			Children: []*Expr{l, r},
		}, nil
	}
	// comparison or arithmetic
	lh, rh, err := inferComparisonTypes(pe.left, pe.right, cols)
	if err != nil {
		return nil, err
	}
	left, err := resolveExpr(pe.left, cols, lh)
	if err != nil {
		return nil, err
	}
	right, err := resolveExpr(pe.right, cols, rh)
	if err != nil {
		return nil, err
	}
	fn := mapBinaryToFuncName(pe.op)
	return &Expr{
		Kind:     ExprBinaryOp,
		Type:     resultTypeForBinary(pe.op, left.Type, right.Type),
		Op:       pe.op,
		Name:     fn,
		Children: []*Expr{left, right},
	}, nil
}

func inferComparisonTypes(a, b *parseExpr, cols map[string]TypeKind) (leftHint, rightHint *TypeKind, err error) {
	// Parameter type from peer column
	la := columnTypeIfIdent(a, cols)
	lb := columnTypeIfIdent(b, cols)
	if a.litKind == "param_named" || a.litKind == "param_pos" {
		return lb, nil, nil
	}
	if b.litKind == "param_named" || b.litKind == "param_pos" {
		return nil, la, nil
	}
	return nil, nil, nil
}

func columnTypeIfIdent(pe *parseExpr, cols map[string]TypeKind) *TypeKind {
	if pe == nil || pe.litKind != "ident" {
		return nil
	}
	if t, ok := cols[strings.ToLower(pe.lit)]; ok {
		return &t
	}
	return nil
}

func resultTypeForBinary(op string, a, b TypeKind) TypeKind {
	switch op {
	case "=", "!=", "<", ">", "<=", ">=", "AND", "OR":
		return TypeBool
	default:
		if a == b {
			return a
		}
		return TypeUnknown
	}
}

func resolveCall(pe *parseExpr, cols map[string]TypeKind) (*Expr, error) {
	args := make([]*Expr, 0, len(pe.args))
	for _, a := range pe.args {
		e, err := resolveExpr(a, cols, nil)
		if err != nil {
			return nil, err
		}
		args = append(args, e)
	}
	// Minimal builtins: treat as opaque call with STRING/INT64 heuristic
	outType := TypeUnknown
	switch strings.ToUpper(pe.fn) {
	case "ABS", "CEIL", "FLOOR":
		if len(args) > 0 {
			outType = args[0].Type
		}
	case "CONCAT":
		outType = TypeString
	case "LOWER", "UPPER", "TRIM":
		outType = TypeString
	case "LENGTH", "CHAR_LENGTH":
		outType = TypeInt64
	default:
		return nil, fmt.Errorf("function %s: %w", pe.fn, ErrUnsupportedFeature)
	}
	return &Expr{Kind: ExprCall, Type: outType, Name: strings.ToUpper(pe.fn), Children: args}, nil
}
