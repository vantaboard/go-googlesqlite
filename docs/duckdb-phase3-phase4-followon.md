# DuckDB parity: Phase 3 and 4 follow-on

This note captures **what to do after Phase 2 function coverage** matches your workloads. It does not replace [duckdb-parity-roadmap.md](duckdb-parity-roadmap.md); it links the same themes for planning tickets.

## Phase 3 — DDL, types, catalog

- **Emitted DDL:** `CREATE TABLE`, CTAS, and views — align SQLite `STRING` vs DuckDB `VARCHAR`, timestamps, decimals, arrays, and structs where the analyzer emits types both backends accept.
- **Catalog persistence:** [`internal/catalog_repository.go`](../internal/catalog_repository.go) (SQLite vs DuckDB repositories) — re-run migrations and constraint/index paths after type or DDL tweaks.
- **Session / temp semantics:** Document how scratch objects (e.g. MERGE temp tables) interact with connection pooling and emulator expectations.

## Phase 4 — Runtime and integration

- **Parameters:** Named and positional binding through [`driver.go`](../driver.go) / `googlesqlengineduck`; confirm DuckDB driver behavior matches SQLite paths.
- **Transactions:** `BEGIN` / `COMMIT` / rollback through the same connection abstraction used by the analyzer executor.
- **Connection lifecycle:** `SetMaxIdleConns(0)` and other [duckdb-go](https://github.com/duckdb/duckdb-go#memory-allocation) guidance for long-lived processes.
- **bigquery-emulator:** Optional driver selection and shared SQL corpus remain a **separate repo** milestone.

### Connection pooling and session semantics

- **Shared pools:** [`driver.go`](../driver.go) caches one `*sql.DB` (and one in-memory [`Catalog`](../internal/catalog.go)) per `(backend driver name, DSN)` key. Using the same DSN (including empty `""`) shares state across all `sql.Open` callers for that driver.
- **Isolation:** For tests or multi-tenant isolation, use **distinct DSNs**: e.g. SQLite `file:…?mode=memory&cache=private`, or a unique DuckDB file path per session.
- **TEMP tables:** GoogleSQL-emitted `CREATE TEMP TABLE` (including MERGE scratch tables on DuckDB) is visible only within the same database connection/session; pooling can return a different connection unless you hold a `sql.Conn` or use a private pool/DSN.
- **DuckDB idle conns:** [`OpenSQLBackend`](../internal/backend.go) sets `SetMaxIdleConns(0)` for `DuckDBBackend` only. Optionally tune `SetConnMaxLifetime` in the host app for long-lived processes.

## Suggested sequencing

1. Finish Phase 2 matrix items that appear in production queries.
2. Pick one **DDL-heavy** workload (CTAS + typed columns) and drive Phase 3 fixes until it passes dual-backend or is explicitly unsupported.
3. Add parameter and transaction smoke tests, then harden Phase 4 for the emulator or CLI.
