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
)

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
