# Pure analyzer expansion order

This document sequences feature work for `internal/pureanalyzer` after the initial
oracle-backed subset (single-table `SELECT`, narrow `WHERE`, parameters, small builtins).

Each slice should add SQL fixtures, `.golden` updates from CGO, differential tests,
and explicit `ErrUnsupportedFeature` or parse errors for out-of-scope syntax until implemented.

## Phase A — Query shape

1. Qualified column names (`t.col`)
2. Table aliases (`FROM z_table AS t`)
3. `ORDER BY` / `LIMIT` / `OFFSET` (resolved AST may use `OrderByScanNode` / `LimitOffsetScanNode`; extend `ResolvedQuerySummary` and `PureSelectSummary` together)

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

- Never expand the grammar without a CGO oracle golden and a passing differential test.
- Keep `GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE` green on a growing corpus before relying on pure output for execution.
- Prefer promoting features that unlock real queries in `query_test.go` and DuckDB parity tests.
