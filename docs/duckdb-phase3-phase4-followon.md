# DuckDB parity: Phase 3 and 4 follow-on

This note captures **what to do after Phase 2 function coverage** matches your workloads. It does not replace [duckdb-parity-roadmap.md](duckdb-parity-roadmap.md); it links the same themes for planning tickets.

## Phase 3 — DDL, types, catalog

- **Emitted DDL:** `CREATE TABLE`, CTAS, and views — align SQLite `STRING` vs DuckDB `VARCHAR`, timestamps, decimals, arrays, and structs where the analyzer emits types both backends accept.
- **Catalog persistence:** [`internal/catalog_repository.go`](../internal/catalog_repository.go) (SQLite vs DuckDB repositories) — re-run migrations and constraint/index paths after type or DDL tweaks.
- **Session / temp semantics:** Document how scratch objects (e.g. MERGE temp tables) interact with connection pooling and emulator expectations.

## Phase 4 — Runtime and integration

- **Parameters:** Named and positional binding through [`driver.go`](../driver.go) / `googlesqlduck`; confirm DuckDB driver behavior matches SQLite paths.
- **Transactions:** `BEGIN` / `COMMIT` / rollback through the same connection abstraction used by the analyzer executor.
- **Connection lifecycle:** `SetMaxIdleConns(0)` and other [duckdb-go](https://github.com/duckdb/duckdb-go#memory-allocation) guidance for long-lived processes.
- **bigquery-emulator:** Optional driver selection and shared SQL corpus remain a **separate repo** milestone.

## Suggested sequencing

1. Finish Phase 2 matrix items that appear in production queries.
2. Pick one **DDL-heavy** workload (CTAS + typed columns) and drive Phase 3 fixes until it passes dual-backend or is explicitly unsupported.
3. Add parameter and transaction smoke tests, then harden Phase 4 for the emulator or CLI.
