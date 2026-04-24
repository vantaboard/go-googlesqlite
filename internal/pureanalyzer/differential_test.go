package pureanalyzer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vantaboard/go-googlesql"

	"github.com/vantaboard/go-googlesql-engine/engineopts"
)

func TestPureSelectMatchesOracleGoldens(t *testing.T) {
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
			oracleSum, err := ResolvedQuerySummary(out.Statement())
			if err != nil {
				t.Fatal(err)
			}

			aq, err := AnalyzeSelect(sql, catalog)
			if err != nil {
				t.Fatalf("pure AnalyzeSelect: %v", err)
			}
			pureSum, err := PureSelectSummary(aq)
			if err != nil {
				t.Fatal(err)
			}
			if pureSum != oracleSum {
				t.Fatalf("pure vs oracle mismatch\n--- oracle ---\n%s\n--- pure ---\n%s", oracleSum, pureSum)
			}
		})
	}
}
