# Pure analyzer expansion order

This document sequences feature work for `github.com/vantaboard/go-googlesql/pure/analyzer`
after the initial oracle-backed subset (single-table `SELECT`, narrow `WHERE`, parameters, small builtins).

Each slice should add SQL fixtures under `go-googlesql/pure/oracle/testdata/`, `.golden` updates
from the CGO structural oracle, differential tests in `pure/oracle`, and explicit
`ErrUnsupportedFeature` or parse errors for out-of-scope syntax until implemented.

## Phase A — Query shape

1. Qualified column names (`t.col`) (done: `pure/parser`, `pure/analyzer`, oracle `select_qualified_table` / `buckets/phase_a/qualified_table`)
2. Table aliases (`FROM z_table AS t` and `FROM z_table t`) (done: oracle `select_table_alias_param` / `buckets/phase_a/table_alias_param`)
3. `ORDER BY` / `LIMIT` / `OFFSET` (done: `OrderByScanNode` / `LimitOffsetScanNode` in `pure/oracle/summary`, fixtures `select_order_by`, `select_order_by_desc`, `select_limit`, `select_order_limit_offset` and `buckets/phase_a/order_by`, `limit_offset`)

## Phase B — Joins

4. `INNER` / `LEFT` / `RIGHT` / `CROSS` join for **a single** join between two catalog tables (done: parser + analyzer, `JoinScan` in `pure/oracle/summary`, fixtures `join_*` under `pure/oracle/testdata/cgo`, bucket `buckets/joins/inner_on`). **Multi-join** chains: explicit parse error until a later slice.
5. `ON` predicates in the current expression subset (done). `USING` is parsed, resolved to equality for analysis, with `SELECT *` `USING` coalesced column order in CGO goldens (`join_inner_using` under `go-googlesql/pure/oracle/testdata/cgo` and the joins bucket).

## Phase C — Aggregation

6. `GROUP BY`, `HAVING`, `ORDER BY` / `LIMIT` on aggregates (done: CGO goldens under `pure/oracle/testdata/cgo`, `PureSelectSummary` parity, `TestBucketAggregatesPhaseCPureMatchesCGO` in `pure/oracle/differential_test.go`).
7. Common aggregate builtins (done: `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG` in CGO goldens; rejection tests in `pure/analyzer/aggregate_rejections_test.go`).

## Phase D — Nested queries

8. **In progress (first slice):** uncorrelated **scalar** subqueries in `WHERE` (e.g. `WHERE col1 = (SELECT MAX(col1) FROM z_table)`) with CGO golden `go-googlesql/pure/oracle/testdata/cgo/subquery_scalar_where`, `TestBucketSubqueryPhaseDPureMatchesCGO`, and `internal/pure_analyzer_phase_d_validation_test.go`. Multi-column subqueries, `IN` / `EXISTS`, correlated forms, and **derived tables in `FROM`** stay rejected (`ErrUnsupportedFeature` or parse) until a later sub-slice.
9. `WITH` / `WITH RECURSIVE` (align with engine `newAnalyzerOptions` statement kinds) — not started

## Phase E — Advanced types in expressions

10. `ARRAY` literals and `STRUCT` field access where the engine transformers require them
11. `CAST` / `SAFE_CAST` parity for primitive types

## Phase F — DML and DDL (engine-driven priority)

12. `INSERT`, `UPDATE`, `DELETE` (match `go-googlesql-engine` coordinator coverage)
13. `MERGE`, `CREATE TABLE` / `CTAS` as needed for bigquery-emulator workflows

## Verification gates

- Never expand the grammar without a CGO oracle golden (`go-googlesql/pure/oracle/testdata/cgo`) and a passing differential test (`pure/oracle`).
- Keep `GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE` green on a growing corpus before relying on pure output for execution (engine: `internal/pure_analyzer_phase_c_validation_test.go`, `internal/pure_analyzer_phase_d_validation_test.go`; from `go-googlesql` run `task test:go-googlesql-engine-pure-validate` so GOWORK includes prebuilts and the sibling engine module).
- Prefer promoting features that unlock real queries in `query_test.go` and DuckDB parity tests.
