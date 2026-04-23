package internal

import (
	"os"
	"testing"

	engduck "github.com/vantaboard/go-googlesql-engine/internal/dialect/duckdb"
)

func TestDuckdbExplainAnalyzeMode(t *testing.T) {
	t.Parallel()
	restore := os.Getenv("GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE")
	defer func() { _ = os.Setenv("GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE", restore) }()

	_ = os.Setenv("GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE", "")
	if g := duckdbExplainAnalyzeMode(); g != "" {
		t.Fatalf("empty: got %q", g)
	}
	_ = os.Setenv("GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE", "BEFORE")
	if g := duckdbExplainAnalyzeMode(); g != "before" {
		t.Fatalf("before: got %q", g)
	}
	_ = os.Setenv("GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE", "off")
	if g := duckdbExplainAnalyzeMode(); g != "" {
		t.Fatalf("off: got %q", g)
	}
}

func TestIsDuckDBDialect(t *testing.T) {
	t.Parallel()
	if isDuckDBDialect(nil) {
		t.Fatal("nil")
	}
	if isDuckDBDialect(SQLiteDialect{}) {
		t.Fatal("sqlite")
	}
	if !isDuckDBDialect(engduck.Dialect{}) {
		t.Fatal("expected duckdb")
	}
}

func TestDuckExplainAnalyzeMaxBytes_default(t *testing.T) {
	t.Parallel()
	_ = os.Unsetenv("GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE_MAX_BYTES")
	if n := duckExplainAnalyzeMaxBytes(); n != 262144 {
		t.Fatalf("default: got %d", n)
	}
}
