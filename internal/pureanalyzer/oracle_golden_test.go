package pureanalyzer

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vantaboard/go-googlesql"
	"github.com/vantaboard/go-googlesql/types"

	"github.com/vantaboard/go-googlesql-engine/engineopts"
)

var updateOracle = flag.Bool("update-oracle", false, "rewrite testdata/oracle/*.golden from CGO analyzer")

func testOracleCatalog(t *testing.T) *types.SimpleCatalog {
	t.Helper()
	catalog := types.NewSimpleCatalog("z_catalog")
	catalog.AddTable(
		types.NewSimpleTable("z_table", []types.Column{
			types.NewSimpleColumn("z_table", "col1", types.Int64Type()),
			types.NewSimpleColumn("z_table", "col2", types.StringType()),
		}),
	)
	catalog.AddGoogleSQLBuiltinFunctions(nil)
	return catalog
}

func TestOracleGoldensMatchCGO(t *testing.T) {
	catalog := testOracleCatalog(t)
	opt, err := engineopts.NewAnalyzerOptions()
	if err != nil {
		t.Fatal(err)
	}

	cases := []string{
		"select_star_where",
		"select_named_param",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join("testdata", "oracle")
			sqlBytes, err := os.ReadFile(filepath.Join(dir, name+".sql"))
			if err != nil {
				t.Fatal(err)
			}
			sql := strings.TrimSpace(string(sqlBytes))
			out, err := googlesql.AnalyzeStatement(sql, catalog, opt)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ResolvedQuerySummary(out.Statement())
			if err != nil {
				t.Fatal(err)
			}
			goldenPath := filepath.Join(dir, name+".golden")
			if *updateOracle {
				if err := os.WriteFile(goldenPath, []byte(got+"\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				t.Logf("wrote %s", goldenPath)
				return
			}
			wantBytes, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("read golden: %v (run with -update-oracle)", err)
			}
			want := strings.TrimSpace(string(wantBytes))
			if got != want {
				t.Fatalf("golden mismatch\n--- want ---\n%s\n--- got ---\n%s", want, got)
			}
		})
	}
}
