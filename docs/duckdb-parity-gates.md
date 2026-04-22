# DuckDB parity gates (Phase 0)

This document locks the **parity contract**, **minimum test corpus**, **CI shape**, and **failure policy** for DuckDB vs SQLite work tracked in [duckdb-parity-roadmap.md](duckdb-parity-roadmap.md).

## Parity definition

- **In scope:** For an agreed **GoogleSQL corpus**, the same input must yield **logically equivalent results** on `googlesqlite` (SQLite) and `googlesqlduck` (DuckDB): same row counts and column values under a defined comparison order (see dual-backend tests).

### Connection pooling (parity tests and apps)

- The `googlesqlite` / `googlesqlduck` drivers cache `*sql.DB` per DSN; reuse of `""` shares one pool. Use **private DSNs** for isolated dual-backend cases (see [duckdb-phase3-phase4-followon.md](duckdb-phase3-phase4-followon.md)).

### Comparison order

- Dual-backend tests compare row sets as **ordered sequences** after normalizing cell values to strings (see [`duckdb_integration_test.go`](../duckdb_integration_test.go)).
- **When order is not guaranteed** by the engine or the GoogleSQL text, the corpus query must end with an **`ORDER BY`** (or a deterministic key) so parity failures are not conflated with benign reordering.
- Aggregations without `ORDER BY` in the outer query are acceptable only when the expected result is a **single row**.
- **Out of scope for “parity” claims:** Byte-identical emitted SQL text between backends.
- **Initial corpus (grow over time):**
  - All `go test` packages in this module (SQLite default tags).
  - Golden / dialect tests under [`internal/`](../internal/) (including [`dialect_golden_test.go`](../internal/dialect_golden_test.go)).
  - Integration tests that run GoogleSQL through `database/sql` (e.g. [`duckdb_integration_test.go`](../duckdb_integration_test.go) when built with DuckDB tags).
  - Optional: shared cases with [bigquery-emulator](https://github.com/...) / workspace integration (separate milestone).

## CI split

- **Default CI:** Remains **SQLite-only** (existing `GOOGLESQL_BUILD_TAGS` / `task test:prebuilt` workflow). No DuckDB linker requirement for merge gates unless the team promotes an optional job.
- **Optional DuckDB lane:** Use [`Taskfile.yml`](../Taskfile.yml) task **`test:duckdb-lib`** as the single documented entry point when dynamically linking `libduckdb`:
  - Build tags: append `duckdb` and `duckdb_use_lib` to the stack tags.
  - Host env: `DUCKDB_LIB_DIR` must point at the directory containing `libduckdb.so` / `libduckdb.dylib`, plus `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` as in the Taskfile.
  - CI should pin a **DuckDB library version** when this job is enabled (exact pinning is org-specific; record the version in CI config or this doc when enabled).
  - **Pinned version (libduckdb for `duckdb-go/v2 v2.10502.0` in this repo):** `DUCKDB_VERSION=1.5.2` (see [DuckDB releases](https://github.com/duckdb/duckdb/releases)). CI should use the same when the optional job is enabled.

## Failure policy (unsupported vs wrong answers)

- **Preferred:** If a GoogleSQL feature is not implemented for DuckDB, fail **during analysis or execution** with a **stable, typed error** (or driver error wrapping a clear message), not silent wrong results.
- **Avoid:** Emitting SQLite-only artifacts on DuckDB (e.g. `googlesqlite_*` UDFs) without registration or macro shims—this tends to produce confusing runtime errors or incorrect data.
- **Casts:** DuckDB uses native `CAST` / `TRY_CAST` only for **scalar** type pairs we explicitly map; other type combinations should return an error until mapped (see [`internal/duckdb_function_matrix.md`](../internal/duckdb_function_matrix.md)).

## References

- Roadmap: [duckdb-parity-roadmap.md](duckdb-parity-roadmap.md)
- Function strategy table: [`internal/duckdb_function_matrix.md`](../internal/duckdb_function_matrix.md)
- DuckDB linking: [duckdb-go README](https://github.com/duckdb/duckdb-go#linking-duckdb)
