# DuckDB function / emission matrix

Single table for GoogleSQL → DuckDB parity strategy. Extend as features land; keep in sync with [`docs/duckdb-parity-roadmap.md`](../docs/duckdb-parity-roadmap.md).

Legend:

| Strategy | Meaning |
|----------|---------|
| **rename** | Map emitted `googlesqlite_*` name to DuckDB builtin via [`dialect.go`](dialect.go) `duckDBNativeFunctions` / `RewriteEmittedFunctionName`. |
| **rewrite** | Different SQL shape in transformers or `FunctionSpec.CallSQL` (may need `Dialect` switch). |
| **macro** | `CREATE MACRO` or setup in [`backend.go`](backend.go) `RegisterExtensions` (currently unused for DuckDB). |
| **native-cast** | `CAST` / `TRY_CAST` in [`dialect_cast.go`](dialect_cast.go) for scalar targets. |
| **structural** | Not a named function; handled in scan/expression transformers (e.g. UNNEST, array subquery). |
| **unsupported** | Should error with stable message until implemented. |

## Structural codegen (Phase 1)

| GoogleSQL / area | SQLite emission | DuckDB strategy | Status |
|------------------|-----------------|-----------------|--------|
| Array subquery | `googlesqlite_array(col)` | `list(col)` aggregate | **rewrite** (done) |
| UNNEST / array scan | `json_each(googlesqlite_decode_array(...))` | `unnest` + `generate_subscripts` (−1 for 0-based key), `JOIN LATERAL` when correlated | **structural** (done) |
| GROUP BY keys | `googlesqlite_group_by(expr)` | raw expression | **rewrite** (done) |
| Scalar CAST (simple types) | `googlesqlite_cast(...)` | `CAST` / `TRY_CAST` | **native-cast** (done for mapped kinds) |
| CAST complex types | `googlesqlite_cast` | **unsupported** (error until mapped) | pending |
| MERGE simulation | `CREATE TABLE` scratch + multi-statement + `googlesqlite_*` key preds | Same rewrite; scratch = **`CREATE TEMP TABLE`** on DuckDB via dialect | **structural** (done) |
| `CREATE TABLE` column / PK DDL | `TEXT`, PK `COLLATE googlesqlite_collate`, `WITHOUT ROWID` | `VARCHAR` / `BIGINT`, no PK collation, no `WITHOUT ROWID`; [`Dialect` physical DDL](dialect.go) + [`spec.go` PhysicalDDL](spec.go) | **structural** (done starter) |

## Renames (`duckDBNativeFunctions`)

| Emitted / resolver name | DuckDB builtin | Strategy |
|-------------------------|----------------|----------|
| `googlesqlite_length` | `length` | rename |
| `googlesqlite_char_length` | `length` | rename |
| `googlesqlite_abs` | `abs` | rename |
| `googlesqlite_lower` | `lower` | rename |
| `googlesqlite_upper` | `upper` | rename |
| `googlesqlite_substr` | `substr` | rename |
| `googlesqlite_trim` | `trim` | rename |
| `googlesqlite_ltrim` | `ltrim` | rename |
| `googlesqlite_rtrim` | `rtrim` | rename |
| `googlesqlite_concat` | `concat` | rename |
| `googlesqlite_replace` | `replace` | rename |
| `googlesqlite_reverse` | `reverse` | rename |
| `googlesqlite_repeat` | `repeat` | rename |
| `googlesqlite_strpos` | `strpos` | rename |
| `googlesqlite_chr` | `chr` | rename |
| `googlesqlite_ascii` | `ascii` | rename |
| `googlesqlite_instr` | — | **rewrite** / SQLite UDF (3–4 arg `INSTR` ≠ single DuckDB builtin) |

## High-priority families (Phase 2 — inventory)

| Family | Typical approach | Notes |
|--------|------------------|-------|
| Comparison / logic (`googlesqlite_equal`, `googlesqlite_and`, …) | **rewrite** (often already SQL operators via `transformer_function.go`) | Audit residuals |
| Strings / bytes / regex | **rename** or **rewrite** | Per-function semantics check |
| Date/time | **rewrite** / **macro** | Map to DuckDB date functions |
| JSON | **rewrite** | DuckDB `json_*` |
| Aggregates / windows | **rewrite** / **macro** / **unsupported** | Largest gap |
| Geography / ML | **unsupported** unless corpus needs | Lowest priority |

## How to update

1. When adding DuckDB support for a function, add a row or change status here.
2. If strategy is **unsupported**, document the stable error text or issue link in the roadmap.
