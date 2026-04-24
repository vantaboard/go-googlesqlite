package internal

import (
	"fmt"
	"os"

	"github.com/vantaboard/go-googlesql"
	ast "github.com/vantaboard/go-googlesql/resolved_ast"
	"github.com/vantaboard/go-googlesql/types"

	"github.com/vantaboard/go-googlesql-engine/internal/pureanalyzer"
)

// StatementAnalysisDriver is the seam for GoogleSQL statement analysis. The
// default implementation delegates to CGO [googlesql.AnalyzeStatement].
// See also [AnalyzerBackend] in analyzer_backend.go for the future out-of-process boundary.
type StatementAnalysisDriver interface {
	AnalyzeStatement(stmtQuery string, catalog types.Catalog, opt *googlesql.AnalyzerOptions) (*googlesql.AnalyzerOutput, error)
}

// CGOStatementAnalysisDriver uses the linked GoogleSQL analyzer (production default).
type CGOStatementAnalysisDriver struct{}

// AnalyzeStatement implements [StatementAnalysisDriver].
func (CGOStatementAnalysisDriver) AnalyzeStatement(stmtQuery string, catalog types.Catalog, opt *googlesql.AnalyzerOptions) (*googlesql.AnalyzerOutput, error) {
	return googlesql.AnalyzeStatement(stmtQuery, catalog, opt)
}

// validatingStatementDriver runs the pure-Go subset analyzer after a successful CGO
// analyze when GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE=1 and the statement is a QueryStmt
// that the pure package can parse. On summary mismatch it returns an error.
type validatingStatementDriver struct {
	inner StatementAnalysisDriver
}

// NewValidatingStatementDriver wraps inner (use nil for [CGOStatementAnalysisDriver]).
func NewValidatingStatementDriver(inner StatementAnalysisDriver) StatementAnalysisDriver {
	if inner == nil {
		inner = CGOStatementAnalysisDriver{}
	}
	return validatingStatementDriver{inner: inner}
}

// AnalyzeStatement implements [StatementAnalysisDriver].
func (v validatingStatementDriver) AnalyzeStatement(stmtQuery string, catalog types.Catalog, opt *googlesql.AnalyzerOptions) (*googlesql.AnalyzerOutput, error) {
	out, err := v.inner.AnalyzeStatement(stmtQuery, catalog, opt)
	if err != nil {
		return out, err
	}
	if os.Getenv("GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE") != "1" {
		return out, nil
	}
	stmt := out.Statement()
	if stmt == nil || stmt.Kind() != ast.QueryStmt {
		return out, nil
	}
	aq, perr := pureanalyzer.AnalyzeSelect(stmtQuery, catalog)
	if perr != nil {
		return out, nil
	}
	cgoSum, err := pureanalyzer.ResolvedQuerySummary(stmt)
	if err != nil {
		return out, nil
	}
	pureSum, err := pureanalyzer.PureSelectSummary(aq)
	if err != nil {
		return out, nil
	}
	if cgoSum != pureSum {
		return nil, fmt.Errorf("GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE: pure/cgo summary mismatch for %q", stmtQuery)
	}
	return out, nil
}

func newDefaultStatementAnalysisDriver() StatementAnalysisDriver {
	if os.Getenv("GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE") == "1" {
		return NewValidatingStatementDriver(CGOStatementAnalysisDriver{})
	}
	return CGOStatementAnalysisDriver{}
}
