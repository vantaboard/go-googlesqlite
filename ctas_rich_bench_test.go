package googlesqlengine_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	_ "github.com/vantaboard/go-googlesql-engine"
	"github.com/vantaboard/go-googlesql-engine/pprofserver"
)

func init() {
	// GOOGLESQL_ENGINE_PPROF_ADDR=127.0.0.1:6061 enables net/http/pprof during benchmarks.
	pprofserver.StartFromEnv()
}

// benchRichCTASTableSeq avoids table name reuse when the benchmark driver re-runs the loop with increasing b.N
// (same in-memory DB for the whole benchmark function).
var benchRichCTASTableSeq atomic.Uint64

// benchRichTortoiseSQL matches [bigquery-emulator/server/ctas_engine_harness_test.go] harnessTortoiseRichCTASSQL
// (single backtick-wrapped table name for an in-memory DB without project.dataset).
func benchRichTortoiseSQL(tableName string, outer, inner int) string {
	return fmt.Sprintf(
		"CREATE TABLE `%s` AS "+
			"WITH dead_cte AS ( "+
			"SELECT 999 AS never_used, 'unused' AS marker "+
			"), heavy AS ( "+
			"SELECT a, b FROM UNNEST(GENERATE_ARRAY(1, %d)) a CROSS JOIN UNNEST(GENERATE_ARRAY(1, %d)) b "+
			"), bucketed AS ( "+
			"SELECT MOD(x, 3) AS bucket, COUNT(*) AS n FROM UNNEST(GENERATE_ARRAY(1, 99)) x GROUP BY 1 HAVING COUNT(*) > 0 "+
			"), bucket_qualified AS ( "+
			"SELECT bucket, n FROM ( "+
			"SELECT bucket, n, ROW_NUMBER() OVER (ORDER BY n DESC) AS rn FROM bucketed "+
			") z WHERE z.rn <= 3 "+
			"), q_sales AS ( "+
			"SELECT 'item' AS product, 10 AS s, 'Q1' AS quarter UNION ALL SELECT 'item', 20, 'Q2' "+
			"), pivotish AS ( "+
			"SELECT product, SUM(CASE quarter WHEN 'Q1' THEN s END) AS q1, "+
			"SUM(CASE quarter WHEN 'Q2' THEN s END) AS q2 FROM q_sales GROUP BY product "+
			"), unpivish AS ( "+
			"SELECT v AS val, t AS qname FROM ( "+
			"SELECT q1 AS v, 'Q1' AS t FROM pivotish UNION ALL SELECT q2, 'Q2' FROM pivotish "+
			") "+
			") "+
			"SELECT (SELECT COUNT(*) AS cnt FROM heavy) + "+
			"0 * COALESCE((SELECT MAX(n) FROM bucket_qualified), 0) + "+
			"0 * COALESCE((SELECT MAX(val) FROM unpivish), 0) AS c",
		tableName, outer, inner,
	)
}

// BenchmarkCTAS_RichTortoiseExec engine-only CTAS (no bigquery-emulator HTTP). Dimensions default 6000x350
// (tortoise stress); override with BQ_BENCH_TORTOISE_OUTER and BQ_BENCH_TORTOISE_INNER. Example profile:
//
//	go test -tags "$GOOGLESQL_BUILD_TAGS" -run '^$' -bench BenchmarkCTAS_RichTortoiseExec -benchtime 3s -count 5 -cpuprofile=cpu.prof ./
//	go tool pprof -http=:0 cpu.prof
//
// Live heap (this package calls [pprofserver.StartFromEnv] in init):
//
//	GOOGLESQL_ENGINE_PPROF_ADDR=127.0.0.1:6061 go test -tags "$GOOGLESQL_BUILD_TAGS" -run '^$' -bench BenchmarkCTAS_RichTortoiseExec -benchtime 5s ./...
//	curl -sS 'http://127.0.0.1:6061/debug/pprof/heap' -o heap.prof && go tool pprof -http=127.0.0.1:8080 heap.prof
//
// Pair heap with DuckDB EXPLAIN ANALYZE (DuckDB path only, googlesqlengineduck):
//
//	GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE=after GOOGLESQL_ENGINE_LOG_SQL_CORRELATION=1 GOOGLESQL_ENGINE_PPROF_ADDR=127.0.0.1:6061 go test -tags "$GOOGLESQL_BUILD_TAGS" ...
//
// DML/CTAS: GOOGLESQL_ENGINE_DUCK_EXPLAIN_LOG=1 logs a non-executing EXPLAIN (logical) before Exec. For full EXPLAIN ANALYZE on writes, use GOOGLESQL_ENGINE_LOG_PHYSICAL_SQL=1 and run the physical SQL in the duckdb CLI on a copy of the DB.
func BenchmarkCTAS_RichTortoiseExec(b *testing.B) {
	outer, inner := 6000, 350
	if s := os.Getenv("BQ_BENCH_TORTOISE_OUTER"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			outer = v
		}
	}
	if s := os.Getenv("BQ_BENCH_TORTOISE_INNER"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			inner = v
		}
	}
	ctx := context.Background()
	db, err := sql.Open("googlesqlengine", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	for range b.N {
		tname := fmt.Sprintf("bench_rich_ctas_%d", benchRichCTASTableSeq.Add(1))
		_, err := db.ExecContext(ctx, benchRichTortoiseSQL(tname, outer, inner))
		if err != nil {
			b.Fatal(err)
		}
	}
}
