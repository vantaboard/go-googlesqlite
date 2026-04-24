package pureanalyzer

import (
	"errors"
	"testing"
)

func TestAnalyzeSelectUnknownBuiltinIsUnsupportedFeature(t *testing.T) {
	catalog := testOracleCatalog(t)
	_, err := AnalyzeSelect("SELECT * FROM z_table WHERE unknown_fn(col1) = 1", catalog)
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("want ErrUnsupportedFeature got %v", err)
	}
}

func TestAnalyzeSelectJoinRejectsParse(t *testing.T) {
	catalog := testOracleCatalog(t)
	_, err := AnalyzeSelect("SELECT * FROM z_table JOIN other ON true", catalog)
	if err == nil {
		t.Fatal("expected error for JOIN")
	}
}

func TestAnalyzeSelectWithClauseRejectsParse(t *testing.T) {
	catalog := testOracleCatalog(t)
	_, err := AnalyzeSelect("WITH x AS (SELECT 1) SELECT * FROM z_table", catalog)
	if err == nil {
		t.Fatal("expected error for WITH")
	}
}
