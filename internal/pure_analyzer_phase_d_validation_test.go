// Phase D scalar subquery in WHERE exercised under GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE=1.
// Run from the go-googlesql repo: task test:go-googlesql-engine-pure-validate.
package internal

import (
	"testing"

	"github.com/vantaboard/go-googlesql/engineopts"
)

func TestPhaseDPureAnalyzerValidation(t *testing.T) {
	t.Setenv("GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE", "1")

	opt, err := engineopts.NewAnalyzerOptions()
	if err != nil {
		t.Fatal(err)
	}
	cat := phaseCOracleCatalog()
	drv := NewValidatingStatementDriver(nil)

	queries := []string{
		"SELECT col1 FROM z_table WHERE col1 = (SELECT MAX(col1) FROM z_table)",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			out, err := drv.AnalyzeStatement(q, cat, opt)
			if err != nil {
				t.Fatal(err)
			}
			if out == nil || out.Statement() == nil {
				t.Fatal("nil analyzer output")
			}
		})
	}
}
