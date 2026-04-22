# DuckDB parity roadmap (vs SQLite execution layer)

This document tracks work to make the **DuckDB** path (`googlesqlduck` driver, [`DuckDBDialect`](../internal/dialect.go)) match **SQLite** behavior for the same GoogleSQL inputs. “100% parity” here means: **the same GoogleSQL corpus produces observably equivalent results** (and passes the same conformance tests) on both engines—not necessarily byte-identical SQL text.

## Current baseline (already shipped)

- **Plumbing:** [`SQLBackend`](../internal/backend.go), [`CatalogRepository`](../internal/catalog_repository.go) (SQLite vs DuckDB DDL for metadata), [`driver_duckdb.go`](../driver_duckdb.go) (`//go:build duckdb`), optional **dynamic link** via [`task test:duckdb-lib`](../Taskfile.yml) and `duckdb_use_lib` (see [duckdb-go linking](https://github.com/duckdb/duckdb-go#linking-duckdb)).
- **Dialect:** [`WindowPartitionCollation`](../internal/dialect.go), [`RewriteEmittedFunctionName`](../internal/dialect.go), [`ApplySortCollation`](../internal/dialect.go) wired through window / ORDER BY / aggregate ordering paths.
- **Golden tests:** [`internal/dialect_golden_test.go`](../internal/dialect_golden_test.go) (collation + a few function renames).
- **DuckDB rename map:** `length`, `abs`, `lower`, `upper`, `substr`, `char_length`, plus common string builtins (`trim`, `concat`, `replace`, …) in [`dialect.go`](../internal/dialect.go); see [`duckdb_function_matrix.md`](../internal/duckdb_function_matrix.md).

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
| MERGE simulation | [`transformer_stmt_merge.go`](../internal/transformer_stmt_merge.go) | **Dialect:** scratch table name + `CREATE TEMP TABLE … AS` on DuckDB (session-local); SQLite keeps plain `CREATE TABLE … AS`. Same multi-statement rewrite as SQLite; native `MERGE INTO` deferred. |
| GROUP BY wrapper | [`transformer_scan_aggregate.go`](../internal/transformer_scan_aggregate.go) (`googlesqlite_group_by`) | GoogleSQL semantics vs DuckDB `GROUP BY`; may require expression rewrite, not only rename. |

Checklist:

- [x] Subquery array: dialect-specific wrapper or inline shape + golden tests.
- [x] Array scan / UNNEST: DuckDB-native FROM clause + tests.
- [x] Cast: split simple casts to SQL `CAST` vs retain multi-step UDF path on SQLite only.
- [x] MERGE: temp-table simulation aligned with SQLite; DuckDB uses `CREATE TEMP TABLE` for the scratch table via [`Dialect`](../internal/dialect.go) (`MergeTempTableName`, `MergeScratchTableIsTemporary`).
- [x] `googlesqlite_group_by`: semantic parity or documented divergence + tests (DuckDB: omit wrapper; see [`internal/dialect.go`](../internal/dialect.go)).

---

## Phase 2 — Function surface (`googlesqlite_*` → DuckDB)

SQLite registers a large UDF set in [`function_register.go`](../internal/function_register.go) / [`function_bind.go`](../internal/function_bind.go). DuckDB parity options, per function or family:

1. **Rename only** — extend [`duckDBNativeFunctions`](../internal/dialect.go) when DuckDB builtin matches (arity + semantics).
2. **Emit different SQL** — change transformer / `FunctionSpec.CallSQL` branches (harder; may need `Dialect` switch).
3. **DuckDB macros / extensions** — `CREATE MACRO` at `RegisterExtensions` time ([`DuckDBBackend`](../internal/backend.go)).
4. **Unsupported** — return typed error from analyzer or runtime with stable message.

Suggested order (high leverage first):

- [x] **Comparison / logic:** Audited (Phase 2): comparison and boolean ops lower to SQL `=`, `AND`, `IN`, … when [`canOptimizeFunction`](../internal/transformer_function.go) accepts primitive args; MERGE key paths still use `googlesqlite_*` comparators by design; window `googlesqlite_window_*` remains follow-on (see [`duckdb_function_matrix.md`](../internal/duckdb_function_matrix.md) inventory notes).
- [x] **Strings (batch 1):** low-risk renames (`trim`, `ltrim`, `rtrim`, `concat`, `replace`, `reverse`, `repeat`, `strpos`, `chr`, `ascii`) in [`dialect.go`](../internal/dialect.go); `INSTR` with extra args still uses SQLite UDF until rewritten.
- [x] **Strings / bytes / regex (batch 2):** DuckDB renames (`starts_with`, `ends_with`, `left`, `right`, `lpad`, `rpad`, `initcap`, `unicode`, `byte_length` → `octet_length`, `md5`/`sha1`/`sha256`/`sha512`); **INSTR** 2-arg → `strpos` rewrite in [`transformer_function.go`](../internal/transformer_function.go); golden + [`transformer_duckdb_rewrites_test.go`](../internal/transformer_duckdb_rewrites_test.go) + [`duckdb_integration_test.go`](../duckdb_integration_test.go) Phase 2 corpus.
- [x] **Date/time (batch 1):** `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_TIME` DuckDB rewrites (including frozen clock via [`WithCurrentTime`](../context.go) → `to_timestamp`); see matrix + rewrite tests.
- [x] **JSON (batch 1):** `googlesqlite_json_extract` → `json_extract` rename; `PARSE_JSON` first-arg-only rewrite (optional BigQuery widen mode dropped on DuckDB); dual-backend smoke on `JSON_EXTRACT` + `CAST` in [`duckdb_integration_test.go`](../duckdb_integration_test.go).
- [ ] **Aggregates / window builtins:** `FunctionSpec.CallSQL` and custom SQLite aggregates—largest gap; consider per-function issues.
- [ ] **Geography / ML / rare builtins:** lowest priority unless your corpus needs them.

Deliverable: **`internal/duckdb_function_matrix.md`** is the single strategy table (rename / rewrite / macro / unsupported); this roadmap links to it.

---

## Phase 3 — DDL, types, and catalog

Planning note: [duckdb-phase3-phase4-followon.md](duckdb-phase3-phase4-followon.md).

- [x] **CREATE TABLE / CTAS / views (starter):** dialect-aware `PhysicalDDL` on [`TableSpec`](../internal/spec.go) / [`ColumnSpec`](../internal/spec.go) — DuckDB avoids SQLite-only PK collation and `WITHOUT ROWID`; string columns use `VARCHAR`. CTAS/view body still shared; refine types as workloads require.
- [x] **Catalog persistence (smoke):** [`catalog_repository.go`](../internal/catalog_repository.go) — [`catalog_repository_test.go`](../internal/catalog_repository_test.go) (SQLite) + DuckDB-tagged integration test; re-validate constraints/indexes as DDL evolves.
- [x] **Temp tables / session (docs):** pooling + TEMP semantics in [duckdb-phase3-phase4-followon.md](duckdb-phase3-phase4-followon.md); emulator alignment still workload-specific.

---

## Phase 4 — Runtime and integration

Planning note: [duckdb-phase3-phase4-followon.md](duckdb-phase3-phase4-followon.md).

- [x] **Parameters (smoke):** dual-backend named param test in [`duckdb_integration_test.go`](../duckdb_integration_test.go) (`CAST(@p AS INT64)`).
- [x] **Transactions (smoke):** commit + rollback DDL/DML via `database/sql` `BeginTx` in [`duckdb_integration_test.go`](../duckdb_integration_test.go).
- [x] **Connection settings:** `SetMaxIdleConns(0)` in [`OpenSQLBackend`](../internal/backend.go) for DuckDB; lifecycle notes in [duckdb-phase3-phase4-followon.md](duckdb-phase3-phase4-followon.md).
- [ ] **bigquery-emulator:** optional driver selection + same job SQL corpus (separate repo milestone).

---

## Phase 5 — Verification toward “100%”

- [ ] **Expand golden tests:** one file per concern or table-driven corpus under `internal/` (SQL text) + optional `testdata/`.
- [x] **Dual-backend integration tests (starter corpus):** [`duckdb_integration_test.go`](../duckdb_integration_test.go) (`//go:build duckdb && duckdb_use_lib`) — Phase 1 surfaces + strings; comparison rules in [duckdb-parity-gates.md](duckdb-parity-gates.md).
- [ ] **Performance / OOM:** large CTAS / analytics workloads (original motivation); track separately from correctness parity.

---

## Realistic note on “100%”

Full parity with **every** GoogleSQL feature and **every** SQLite UDF is a **multi-track** effort (months, team-dependent). This roadmap is ordered so you can:

1. Unlock **real workloads** early (SELECT + common functions + CTAS).
2. **Quantify** gaps with tests rather than chasing theoretical completeness first.
3. Decide **explicit unsupported** surfaces instead of silent bugs.

Update this document when phases complete or when the parity definition/corpus changes.
