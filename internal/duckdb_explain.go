package internal

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

// Environment (DuckDB physical backend):
//
//	GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE          off | before | after  — for read-only QueryStmtAction only; runs EXPLAIN ANALYZE (executes the query) before or after the main query; pairs with pprof / heap via correlation_id
//	GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE_MAX_BYTES max bytes of explain output in logs (default 262144)
//	GOOGLESQL_ENGINE_DUCK_EXPLAIN_LOG              1 — for DML/CTAS: log a non-executing EXPLAIN (logical plan) before Exec (no double mutation)
//	GOOGLESQL_ENGINE_LOG_SQL_CORRELATION            1 — include correlation_id on physical SQL logs even when EXPLAIN ANALYZE is off (for manual heap captures)

const (
	envDuckExplainAnalyze     = "GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE"
	envDuckExplainAnalyzeMax  = "GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE_MAX_BYTES"
	envDuckExplainLog         = "GOOGLESQL_ENGINE_DUCK_EXPLAIN_LOG"
	envKeyLogSQLCorrelation   = "GOOGLESQL_ENGINE_LOG_SQL_CORRELATION"
	duckExplainAnalyzeDefault = 262144
)

var sqlCorrelationID atomic.Uint64

func nextSQLCorrelationID() uint64 {
	return sqlCorrelationID.Add(1)
}

// duckdbExplainAnalyzeMode returns "before", "after", or "" (off).
func duckdbExplainAnalyzeMode() string {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(envDuckExplainAnalyze)))
	switch v {
	case "before", "after":
		return v
	case "off", "":
		return ""
	default:
		return ""
	}
}

func envLogSQLCorrelationEnabled() bool {
	return strings.TrimSpace(os.Getenv(envKeyLogSQLCorrelation)) == "1"
}

// shouldEmitSQLCorrelation is true when we assign correlation_id to physical SQL logs (and explain paths).
func shouldEmitSQLCorrelation() bool {
	return duckdbExplainAnalyzeMode() != "" || envLogSQLCorrelationEnabled()
}

func duckExplainAnalyzeMaxBytes() int {
	s := strings.TrimSpace(os.Getenv(envDuckExplainAnalyzeMax))
	if s == "" {
		return duckExplainAnalyzeDefault
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return duckExplainAnalyzeDefault
	}
	return n
}

func envDuckExplainLogicalLog() bool {
	return strings.TrimSpace(os.Getenv(envDuckExplainLog)) == "1"
}

func isDuckDBDialect(d Dialect) bool {
	return d != nil && d.ID() == "duckdb"
}

// logDuckExplainAnalyze runs EXPLAIN ANALYZE and logs the result. The statement is executed
// by DuckDB as part of EXPLAIN ANALYZE (in addition to the main query on the other path).
func logDuckExplainAnalyze(
	ctx context.Context,
	conn *Conn,
	sourceSQL, physicalSQL string,
	args []interface{},
	timing string, // "before" | "after"
	correlationID uint64,
	dialect Dialect,
) {
	if !isDuckDBDialect(dialect) {
		return
	}
	if physicalSQL == "" {
		return
	}
	explainSQL := "EXPLAIN ANALYZE " + physicalSQL
	text, err := readQueryRowsToString(ctx, conn, explainSQL, args, duckExplainAnalyzeMaxBytes())
	if err != nil {
		Logger(ctx).LogAttrs(ctx, slog.LevelWarn, "googlesqlengine duckdb explain analyze failed", slog.String("err", err.Error()), slog.String("timing", timing), slog.String("source_sql", truncateSQLForLog(sourceSQL, sourceSQLLogMax)))
		return
	}
	attrs := []slog.Attr{
		slog.String("duckdb_explain_analyze", text),
		slog.String("explain_timing", timing),
		slog.String("source_sql", truncateSQLForLog(sourceSQL, sourceSQLLogMax)),
		slog.String("pprof_heap_hint", "curl -sS 'http://127.0.0.1:6060/debug/pprof/heap' -o heap.prof (or your pprof addr) while this correlation_id is active"),
	}
	if correlationID != 0 {
		attrs = append(attrs, slog.Uint64("correlation_id", correlationID))
	}
	Logger(ctx).LogAttrs(ctx, slog.LevelInfo, "googlesqlengine duckdb explain analyze", attrs...)
}

// logDuckExplainLogical runs non-executing EXPLAIN for DML/CTAS (does not run the statement).
func logDuckExplainLogical(
	ctx context.Context,
	conn *Conn,
	sourceSQL, physicalSQL string,
	args []interface{},
	correlationID uint64,
	dialect Dialect,
) {
	if !isDuckDBDialect(dialect) || !envDuckExplainLogicalLog() {
		return
	}
	if physicalSQL == "" {
		return
	}
	explainSQL := "EXPLAIN " + physicalSQL
	text, err := readQueryRowsToString(ctx, conn, explainSQL, args, duckExplainAnalyzeMaxBytes())
	if err != nil {
		Logger(ctx).LogAttrs(ctx, slog.LevelDebug, "googlesqlengine duckdb explain (logical) skipped or failed", slog.String("err", err.Error()), slog.String("source_sql", truncateSQLForLog(sourceSQL, sourceSQLLogMax)))
		return
	}
	attrs := []slog.Attr{
		slog.String("duckdb_explain", text),
		slog.String("source_sql", truncateSQLForLog(sourceSQL, sourceSQLLogMax)),
	}
	if correlationID != 0 {
		attrs = append(attrs, slog.Uint64("correlation_id", correlationID))
	}
	Logger(ctx).LogAttrs(ctx, slog.LevelInfo, "googlesqlengine duckdb explain (logical plan)", attrs...)
}

func readQueryRowsToString(ctx context.Context, conn *Conn, query string, args []interface{}, maxBytes int) (string, error) {
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	n := len(cols)
	if n == 0 {
		return "", nil
	}
	var b strings.Builder
	line := 0
	for rows.Next() {
		ptrs := make([]any, n)
		for i := range ptrs {
			ptrs[i] = new(any)
		}
		if err := rows.Scan(ptrs...); err != nil {
			return b.String(), err
		}
		if line > 0 {
			b.WriteByte('\n')
		}
		parts := make([]string, n)
		for i, p := range ptrs {
			val := *(p.(*any))
			if val == nil {
				parts[i] = "NULL"
			} else {
				parts[i] = fmt.Sprint(val)
			}
		}
		b.WriteString(strings.Join(parts, " | "))
		line++
		if b.Len() >= maxBytes {
			b.WriteString("\n…(truncated)")
			break
		}
	}
	if err := rows.Err(); err != nil {
		return b.String(), err
	}
	if b.Len() > maxBytes {
		return b.String()[:maxBytes] + "…(truncated)", nil
	}
	return b.String(), nil
}
