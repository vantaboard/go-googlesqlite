package googlesqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	_ "github.com/vantaboard/go-googlesqlite"
)

// BenchmarkCTAS_ExecUnnestedRows measures a CTAS that writes many rows (not the emulator HTTP stack).
// Scale the workload with BQ_BENCH_CTAS_N (default 2000) to match [bigquery-emulator/server/ctas_engine_harness_test].
func BenchmarkCTAS_ExecUnnestedRows(b *testing.B) {
	n := 2000
	if s := os.Getenv("BQ_BENCH_CTAS_N"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			n = v
		}
	}
	ctx := context.Background()
	db, err := sql.Open("googlesqlite", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tname := fmt.Sprintf("bench_ctas_%d", i)
		_, err := db.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE `%s` AS SELECT x AS c FROM UNNEST(GENERATE_ARRAY(1, %d)) x",
			tname, n,
		))
		if err != nil {
			b.Fatal(err)
		}
	}
}
