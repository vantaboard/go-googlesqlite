package pureanalyzer

import (
	"fmt"
	"strings"

	ast "github.com/vantaboard/go-googlesql/resolved_ast"
	"github.com/vantaboard/go-googlesql/types"
)

// ResolvedQuerySummary builds a stable, line-oriented summary of a resolved
// QueryStmt for differential testing. It only covers shapes used in oracle fixtures.
func ResolvedQuerySummary(stmt ast.StatementNode) (string, error) {
	if stmt == nil {
		return "", fmt.Errorf("pureanalyzer: nil statement")
	}
	if stmt.Kind() != ast.QueryStmt {
		return "", fmt.Errorf("pureanalyzer: want QueryStmt got kind %d", stmt.Kind())
	}
	q := stmt.(*ast.QueryStmtNode)
	var b strings.Builder
	outs := q.OutputColumnList()
	fmt.Fprintf(&b, "stmt:QueryStmt\n")
	fmt.Fprintf(&b, "output_columns:%d\n", len(outs))
	for i, oc := range outs {
		col := oc.Column()
		fmt.Fprintf(&b, "col%d:name=%s type=%s\n", i, oc.Name(), col.Type().TypeName(types.ProductInternal))
	}
	if err := writeScan(&b, q.Query(), 0); err != nil {
		return "", err
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

func writeScan(b *strings.Builder, scan ast.ScanNode, depth int) error {
	if scan == nil {
		return fmt.Errorf("pureanalyzer: nil scan")
	}
	pad := strings.Repeat("  ", depth)
	switch s := scan.(type) {
	case *ast.ProjectScanNode:
		fmt.Fprintf(b, "%sProjectScan\n", pad)
		if err := writeScan(b, s.InputScan(), depth+1); err != nil {
			return err
		}
		for _, cc := range s.ExprList() {
			col := cc.Column()
			fmt.Fprintf(b, "%s  expr_col name=%s type=%s\n", pad, col.Name(), col.Type().TypeName(types.ProductInternal))
		}
		return nil
	case *ast.FilterScanNode:
		fmt.Fprintf(b, "%sFilterScan\n", pad)
		if err := writeScan(b, s.InputScan(), depth+1); err != nil {
			return err
		}
		fmt.Fprintf(b, "%sfilter:", pad)
		if err := writeExpr(b, s.FilterExpr()); err != nil {
			return err
		}
		fmt.Fprintf(b, "\n")
		return nil
	case *ast.TableScanNode:
		tn := s.Table().Name()
		fmt.Fprintf(b, "%sTableScan table=%s\n", pad, tn)
		return nil
	default:
		return fmt.Errorf("%w: scan %T", ErrUnsupportedFeature, scan)
	}
}

func writeExpr(b *strings.Builder, e ast.ExprNode) error {
	if e == nil {
		fmt.Fprintf(b, "<nil>")
		return nil
	}
	switch x := e.(type) {
	case *ast.ColumnRefNode:
		fmt.Fprintf(b, "ColumnRef(%s)", x.Column().Name())
		return nil
	case *ast.LiteralNode:
		v := x.Value()
		if v.IsNull() {
			fmt.Fprintf(b, "Literal(NULL)")
			return nil
		}
		switch v.TypeKind() {
		case types.INT64:
			fmt.Fprintf(b, "Literal(INT64=%d)", v.Int64Value())
		case types.STRING:
			fmt.Fprintf(b, "Literal(STRING=%q)", v.StringValue())
		case types.BOOL:
			fmt.Fprintf(b, "Literal(BOOL=%v)", v.BoolValue())
		case types.DOUBLE:
			fmt.Fprintf(b, "Literal(DOUBLE=%v)", v.DoubleValue())
		default:
			fmt.Fprintf(b, "Literal(%s)", v.TypeKind().String())
		}
		return nil
	case *ast.ParameterNode:
		fmt.Fprintf(b, "Parameter(%s)", x.Name())
		return nil
	case *ast.FunctionCallNode:
		fn := x.Function()
		name := fn.Name()
		fmt.Fprintf(b, "FunctionCall(%s)", name)
		args := x.ArgumentList()
		fmt.Fprintf(b, "(")
		for i, arg := range args {
			if i > 0 {
				fmt.Fprintf(b, ",")
			}
			if err := writeExpr(b, arg); err != nil {
				return err
			}
		}
		fmt.Fprintf(b, ")")
		return nil
	default:
		return fmt.Errorf("%w: expr %T", ErrUnsupportedFeature, e)
	}
}

// PureSelectSummary formats AnalyzedSelect in the same style as ResolvedQuerySummary
// for simple SELECT * / column list + FilterScan shapes.
func PureSelectSummary(a *AnalyzedSelect) (string, error) {
	if a == nil {
		return "", fmt.Errorf("pureanalyzer: nil AnalyzedSelect")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "stmt:QueryStmt\n")
	fmt.Fprintf(&b, "output_columns:%d\n", len(a.OutputCols))
	for i, oc := range a.OutputCols {
		fmt.Fprintf(&b, "col%d:name=%s type=%s\n", i, oc.Name, oc.Type)
	}
	fmt.Fprintf(&b, "ProjectScan\n")
	fmt.Fprintf(&b, "  FilterScan\n")
	fmt.Fprintf(&b, "    TableScan table=%s\n", a.Table)
	if a.Where != nil {
		fmt.Fprintf(&b, "  filter:")
		if err := writePureExpr(&b, a.Where); err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "\n")
	} else {
		fmt.Fprintf(&b, "  filter:Literal(BOOL=true)\n")
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

func writePureExpr(b *strings.Builder, e *Expr) error {
	if e == nil {
		fmt.Fprintf(b, "<nil>")
		return nil
	}
	switch e.Kind {
	case ExprColumn:
		fmt.Fprintf(b, "ColumnRef(%s)", e.Name)
	case ExprLiteral:
		if e.Type == TypeString {
			fmt.Fprintf(b, "Literal(STRING=%s)", e.Literal)
		} else {
			fmt.Fprintf(b, "Literal(%s=%s)", e.Type, e.Literal)
		}
	case ExprParam:
		if e.Param != nil && e.Param.Name != "" {
			fmt.Fprintf(b, "Parameter(%s)", e.Param.Name)
		} else if e.Param != nil {
			fmt.Fprintf(b, "Parameter(p%d)", e.Param.Position)
		} else {
			fmt.Fprintf(b, "Parameter(?)")
		}
	case ExprCall:
		fmt.Fprintf(b, "FunctionCall(%s)", e.Name)
		fmt.Fprintf(b, "(")
		for i, c := range e.Children {
			if i > 0 {
				fmt.Fprintf(b, ",")
			}
			if err := writePureExpr(b, c); err != nil {
				return err
			}
		}
		fmt.Fprintf(b, ")")
	case ExprBinaryOp:
		fmt.Fprintf(b, "FunctionCall(%s)", mapBinaryToFuncName(e.Op))
		fmt.Fprintf(b, "(")
		for i, c := range e.Children {
			if i > 0 {
				fmt.Fprintf(b, ",")
			}
			if err := writePureExpr(b, c); err != nil {
				return err
			}
		}
		fmt.Fprintf(b, ")")
	default:
		return fmt.Errorf("%w: pure expr kind %s", ErrUnsupportedFeature, e.Kind)
	}
	return nil
}

func mapBinaryToFuncName(op string) string {
	switch op {
	case "=", "==":
		return "$equal"
	case "!=":
		return "$not_equal"
	case "<":
		return "$less"
	case "<=":
		return "$less_or_equal"
	case ">":
		return "$greater"
	case ">=":
		return "$greater_or_equal"
	case "+":
		return "$add"
	case "-":
		return "$subtract"
	case "*":
		return "$multiply"
	case "/":
		return "$divide"
	case "AND":
		return "$and"
	case "OR":
		return "$or"
	default:
		return "$" + op
	}
}
