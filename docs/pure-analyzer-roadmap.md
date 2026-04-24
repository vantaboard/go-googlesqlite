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

4. `INNER` / `LEFT` / `RIGHT` / `CROSS` join (single join, then multi-join chains)
5. `USING` and `ON` clauses

## Phase C — Aggregation

6. `GROUP BY`, `HAVING`
7. Common aggregate builtins wired to resolved function names

## Phase D — Nested queries

8. Subqueries in `FROM` and `WHERE`
9. `WITH` / `WITH RECURSIVE` (align with engine `newAnalyzerOptions` statement kinds)

## Phase E — Advanced types in expressions

10. `ARRAY` literals and `STRUCT` field access where the engine transformers require them
11. `CAST` / `SAFE_CAST` parity for primitive types

## Phase F — DML and DDL (engine-driven priority)

12. `INSERT`, `UPDATE`, `DELETE` (match `go-googlesql-engine` coordinator coverage)
13. `MERGE`, `CREATE TABLE` / `CTAS` as needed for bigquery-emulator workflows

## Verification gates

- Never expand the grammar without a CGO oracle golden (`go-googlesql/pure/oracle/testdata/cgo`) and a passing differential test (`pure/oracle`).
- Keep `GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE` green on a growing corpus before relying on pure output for execution.
- Prefer promoting features that unlock real queries in `query_test.go` and DuckDB parity tests.
