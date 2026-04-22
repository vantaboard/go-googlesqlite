# DuckDB parity roadmap (vs SQLite execution layer)

This document tracks work to make the **DuckDB** path (`googlesqlduck` driver, [`DuckDBDialect`](../internal/dialect.go)) match **SQLite** behavior for the same GoogleSQL inputs. “100% parity” here means: **the same GoogleSQL corpus produces observably equivalent results** (and passes the same conformance tests) on both engines—not necessarily byte-identical SQL text.

## Current baseline (already shipped)

- **Plumbing:** [`SQLBackend`](../internal/backend.go), [`CatalogRepository`](../internal/catalog_repository.go) (SQLite vs DuckDB DDL for metadata), [`driver_duckdb.go`](../driver_duckdb.go) (`//go:build duckdb`), optional **dynamic link** via [`task test:duckdb-lib`](../Taskfile.yml) and `duckdb_use_lib` (see [duckdb-go linking](https://github.com/duckdb/duckdb-go#linking-duckdb)).
- **Dialect:** [`WindowPartitionCollation`](../internal/dialect.go), [`RewriteEmittedFunctionName`](../internal/dialect.go), [`ApplySortCollation`](../internal/dialect.go) wired through window / ORDER BY / aggregate ordering paths.
- **Golden tests:** [`internal/dialect_golden_test.go`](../internal/dialect_golden_test.go) (collation + a few function renames).
- **Small DuckDB rename map:** `length`, `abs`, `lower`, `upper`, `substr`, `char_length` in [`dialect.go`](../internal/dialect.go).

## How to use this roadmap

- Work **top to bottom** within each phase; later phases depend on earlier ones.
- For each bullet, add **tests** (golden SQL and/or `sql.Open("googlesqlduck", …)` integration) before claiming parity.
- Track progress by copying unchecked items into issues/PRs; tick boxes when merged.

---

## Phase 0 — Definition and gates

See **[duckdb-parity-gates.md](duckdb-parity-gates.md)** for the locked parity definition, initial corpus, CI split, and failure policy.

- [x] **Parity definition:** Same GoogleSQL → same *logical* results on SQLite vs DuckDB for an agreed **test corpus** (start with existing `go test` packages + emulator integration tests, then grow).
- [x] **CI split:** Default CI stays **SQLite-only**; optional job builds with `duckdb` + `duckdb_use_lib` and a pinned `libduckdb` (or static build if linker allows).
- [x] **Failure policy:** Decide whether DuckDB returns a clear “unsupported on this backend” error vs silent wrong results for unimplemented features.

---

## Phase 1 — Codegen: structural SQLite emissions

These files still embed **SQLite-specific** function names, temp table names, or helpers. Each needs a **Dialect hook** (or shared helper) and tests.

| Area | Primary files | Notes |
|------|----------------|-------|
| Array subquery wrap | [`transformer_subquery.go`](../internal/transformer_subquery.go) (`googlesqlite_array`) | DuckDB may need `LIST` / `ARRAY` / subquery shape different from SQLite UDF. |
| UNNEST / `json_each` | [`transformer_scan_array.go`](../internal/transformer_scan_array.go) (`googlesqlite_decode_array`) | Likely native `UNNEST` or list functions in DuckDB. |
| Complex casts | [`transformer_cast.go`](../internal/transformer_cast.go) (`googlesqlite_cast`) | Map to DuckDB `CAST` / `TRY_CAST` where possible; keep SQLite UDF where not. |
| MERGE temp pattern | [`transformer_stmt_merge.go`](../internal/transformer_stmt_merge.go) (`googlesqlite_merged_table`, `googlesqlite_and`, key comparisons) | May need DuckDB `MERGE` or alternate rewrite; table name + predicate shape. |
| GROUP BY wrapper | [`transformer_scan_aggregate.go`](../internal/transformer_scan_aggregate.go) (`googlesqlite_group_by`) | GoogleSQL semantics vs DuckDB `GROUP BY`; may require expression rewrite, not only rename. |

Checklist:

- [x] Subquery array: dialect-specific wrapper or inline shape + golden tests.
- [x] Array scan / UNNEST: DuckDB-native FROM clause + tests.
- [x] Cast: split simple casts to SQL `CAST` vs retain multi-step UDF path on SQLite only.
- [ ] MERGE: one documented strategy (native vs temp-table parity with SQLite).
- [x] `googlesqlite_group_by`: semantic parity or documented divergence + tests (DuckDB: omit wrapper; see [`internal/dialect.go`](../internal/dialect.go)).

---

## Phase 2 — Function surface (`googlesqlite_*` → DuckDB)

SQLite registers a large UDF set in [`function_register.go`](../internal/function_register.go) / [`function_bind.go`](../internal/function_bind.go). DuckDB parity options, per function or family:

1. **Rename only** — extend [`duckDBNativeFunctions`](../internal/dialect.go) when DuckDB builtin matches (arity + semantics).
2. **Emit different SQL** — change transformer / `FunctionSpec.CallSQL` branches (harder; may need `Dialect` switch).
3. **DuckDB macros / extensions** — `CREATE MACRO` at `RegisterExtensions` time ([`DuckDBBackend`](../internal/backend.go)).
4. **Unsupported** — return typed error from analyzer or runtime with stable message.

Suggested order (high leverage first):

- [ ] **Comparison / logic:** Already optimized to SQL operators in many paths; audit remaining `googlesqlite_*` in [`transformer_function.go`](../internal/transformer_function.go) and window variants (`googlesqlite_window_*` from resolver).
- [ ] **Strings / bytes / regex:** map or macro where DuckDB builtins align.
- [ ] **Date/time:** many paths use UDFs; DuckDB has rich date functions—systematic mapping table + tests.
- [ ] **JSON:** align with DuckDB `json_*` where possible.
- [ ] **Aggregates / window builtins:** `FunctionSpec.CallSQL` and custom SQLite aggregates—largest gap; consider per-function issues.
- [ ] **Geography / ML / rare builtins:** lowest priority unless your corpus needs them.

Deliverable: maintain a **single table** (could live in this doc or `internal/duckdb_function_matrix.md`) listing GoogleSQL name → strategy (rename / rewrite / macro / unsupported).

---

## Phase 3 — DDL, types, and catalog

- [ ] **CREATE TABLE / CTAS / views:** column types in emitted DDL (SQLite `STRING` vs DuckDB `VARCHAR`, timestamps, decimals, arrays, structs).
- [ ] **Catalog persistence:** [`catalog_repository.go`](../internal/catalog_repository.go) already split; re-validate every migration path (constraints, indexes).
- [ ] **Temp tables / session:** semantics vs BigQuery emulator expectations.

---

## Phase 4 — Runtime and integration

- [ ] **Parameters:** confirm named / positional binding parity with DuckDB driver.
- [ ] **Transactions:** `BEGIN` / `COMMIT` paths through [`driver.go`](../driver.go) / `googlesqlduck`.
- [ ] **Connection settings:** `SetMaxIdleConns(0)` and other [duckdb-go lifecycle](https://github.com/duckdb/duckdb-go#memory-allocation) notes for long-running processes.
- [ ] **bigquery-emulator:** optional driver selection + same job SQL corpus (separate repo milestone).

---

## Phase 5 — Verification toward “100%”

- [ ] **Expand golden tests:** one file per concern or table-driven corpus under `internal/` (SQL text) + optional `testdata/`.
- [ ] **Dual-backend integration tests:** same GoogleSQL file → run on `googlesqlite` and `googlesqlduck`, compare rows (allow ordered comparison rules).
- [ ] **Performance / OOM:** large CTAS / analytics workloads (original motivation); track separately from correctness parity.

---

## Realistic note on “100%”

Full parity with **every** GoogleSQL feature and **every** SQLite UDF is a **multi-track** effort (months, team-dependent). This roadmap is ordered so you can:

1. Unlock **real workloads** early (SELECT + common functions + CTAS).
2. **Quantify** gaps with tests rather than chasing theoretical completeness first.
3. Decide **explicit unsupported** surfaces instead of silent bugs.

Update this document when phases complete or when the parity definition/corpus changes.
