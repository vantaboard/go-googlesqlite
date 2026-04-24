// Phase C aggregate queries exercised under GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE=1.
// Run from the go-googlesql repo: task test:go-googlesql-engine-pure-validate (prebuilt GOWORK + sibling engine).
package internal

import (
	"testing"

	"github.com/vantaboard/go-googlesql/engineopts"
	"github.com/vantaboard/go-googlesql/types"
)

// phaseCOracleCatalog matches pure/oracle helpers_test.go z_catalog shape for summary parity.
func phaseCOracleCatalog() *types.SimpleCatalog {
	catalog := types.NewSimpleCatalog("z_catalog")
	catalog.AddTable(
		types.NewSimpleTable("z_table", []types.Column{
			types.NewSimpleColumn("z_table", "col1", types.Int64Type()),
			types.NewSimpleColumn("z_table", "col2", types.StringType()),
			types.NewSimpleColumn("z_table", "jid", types.Int64Type()),
		}),
	)
	catalog.AddTable(
		types.NewSimpleTable("other_table", []types.Column{
			types.NewSimpleColumn("other_table", "o1", types.Int64Type()),
			types.NewSimpleColumn("other_table", "o2", types.StringType()),
			types.NewSimpleColumn("other_table", "jid", types.Int64Type()),
		}),
	)
	catalog.AddGoogleSQLBuiltinFunctions(nil)
	return catalog
}

// TestPhaseCPureAnalyzerValidation runs the same Phase C aggregate shapes as pure/oracle CGO goldens
// through NewValidatingStatementDriver when GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE=1.
// Run from go-googlesql via task (prebuilt tags + GOWORK), not bare go test without env.
func TestPhaseCPureAnalyzerValidation(t *testing.T) {
	t.Setenv("GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE", "1")

	opt, err := engineopts.NewAnalyzerOptions()
	if err != nil {
		t.Fatal(err)
	}
	cat := phaseCOracleCatalog()
	drv := NewValidatingStatementDriver(nil)

	queries := []string{
		"SELECT COUNT(*) FROM z_table",
		"SELECT col1, COUNT(*) FROM z_table GROUP BY col1",
		"SELECT col1, COUNT(*) FROM z_table GROUP BY col1 HAVING col1 > 0",
		"SELECT col1, COUNT(*) FROM z_table GROUP BY col1 HAVING COUNT(*) > 0",
		"SELECT col1, COUNT(*) FROM z_table GROUP BY col1 ORDER BY col1",
		"SELECT col1, COUNT(*) FROM z_table GROUP BY col1 ORDER BY col1 LIMIT 10",
		"SELECT COUNT(*) FROM z_table LIMIT 5",
		"SELECT col1, COUNT(col2) FROM z_table GROUP BY col1",
		"SELECT col1, SUM(col1) FROM z_table GROUP BY col1",
		"SELECT col1, MIN(col1), MAX(col1), AVG(col1) FROM z_table GROUP BY col1",
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
