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
| Scalar CAST (simple types) | `googlesqlite_cast(...)` | `CAST` / `TRY_CAST` | **native-cast** (done for mapped kinds in [`dialect_cast.go`](dialect_cast.go)) |
| CAST complex types (ARRAY, STRUCT, …) | `googlesqlite_cast` | **unsupported** on DuckDB: `MaybeEmitNativeCast` errors for unmapped targets; falls back only on SQLite | pending (explicit error, not silent) |
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
| `googlesqlite_instr` | `strpos` (2-arg only) | **rewrite** in [`transformer_function.go`](transformer_function.go) `duckDBRewriteFunctionCall`; 3–4 arg stays SQLite UDF until mapped |
| `googlesqlite_starts_with` | `starts_with` | rename |
| `googlesqlite_ends_with` | `ends_with` | rename |
| `googlesqlite_left` | `left` | rename |
| `googlesqlite_right` | `right` | rename |
| `googlesqlite_lpad` | `lpad` | rename |
| `googlesqlite_rpad` | `rpad` | rename |
| `googlesqlite_initcap` | `initcap` | rename |
| `googlesqlite_unicode` | `unicode` | rename |
| `googlesqlite_byte_length` | `octet_length(CAST(expr AS BLOB))` | **rewrite** in [`transformer_function.go`](transformer_function.go) (`octet_length` is BLOB-only in DuckDB) |
| `googlesqlite_md5` | `md5` | rename |
| `googlesqlite_sha1` | `sha1` | rename |
| `googlesqlite_sha256` | `sha256` | rename |
| `googlesqlite_sha512` | `sha512` | rename |
| `googlesqlite_json_extract` | `json_extract` | rename (path/typing may differ; test per corpus) |
| `googlesqlite_json_value` | `json_extract` | rename ([`dialect.go`](dialect.go)) |
| `googlesqlite_parse_json` | `CAST(... AS JSON)` | **rewrite** in [`transformer_function.go`](transformer_function.go) (first arg only; optional BQ widen mode dropped) |
| `googlesqlite_current_timestamp` / `datetime` | `current_timestamp` / `to_timestamp(nanos/1e9)` | **rewrite** in [`transformer_function.go`](transformer_function.go) |
| `googlesqlite_current_date` | `current_date` / `CAST(to_timestamp(...) AS DATE)` | **rewrite** |
| `googlesqlite_current_time` | `current_time` / `CAST(to_timestamp(...) AS TIME)` | **rewrite** |

## Aggregates and window functions (`duckDBAggregateRenames`, `duckDBWindowRenames`)

Common `googlesqlite_*` / `googlesqlite_window_*` names are mapped to DuckDB builtins in [`dialect_duckdb_renames.go`](dialect_duckdb_renames.go) via [`RewriteEmittedFunctionName`](dialect.go). Examples: `sum`, `avg`, `min`, `max`, `count`, `count_if`, `bool_and` / `bool_or`, `row_number`, `rank`, `lag` / `lead`, `quantile_cont` / `quantile_disc` (from BQ `percentile_*`). **COUNT(DISTINCT …)** and similar: leading synthetic `googlesqlite_distinct()` arg is stripped in [`sqlbuilder_sqlbuilder.go`](sqlbuilder_sqlbuilder.go) `duckDBNormalizeAggregateCall`; bare `count` → `count(*)`.

Remaining gaps: `googlesqlite_having_*` / `ANY_VALUE` HAVING modifiers, `googlesqlite_order_by` / `googlesqlite_limit` aggregate options, approx/HLL aggregates—still SQLite UDF unless rewritten.

## Phase 2 inventory notes (emission)

| Area | Where emitted | DuckDB notes |
|------|---------------|--------------|
| Comparison / logic (`googlesqlite_equal`, `googlesqlite_is_not_distinct_from`, …) | Lowered to SQL when [`canOptimizeFunction`](transformer_function.go) passes; **DuckDB** also rewrites `googlesqlite_equal` / `googlesqlite_is_not_distinct_from` in [`duckDBRewriteFunctionCall`](transformer_function.go) when optimization does not apply | MERGE `ON` still validated on `ExpressionData` using `googlesqlite_*` names before transform |
| Window builtins | `googlesqlite_window_*` | **rename** via [`dialect_duckdb_renames.go`](dialect_duckdb_renames.go) |
| Grouped aggregates | `googlesqlite_*` | **rename** via [`dialect_duckdb_renames.go`](dialect_duckdb_renames.go) |
| Resolver-registered normal funcs | `googlesqlite_<name>` from [`function_register.go`](function_register.go) | Extend [`duckDBNativeFunctions`](dialect.go) / rewrites only after arity/semantics check |
| Encoded literals | `LiteralFromValue` wire strings | **rewrite** to native SQL in [`dialect_literal.go`](dialect_literal.go) + [`writeDialectLiteral`](sqlbuilder_sqlbuilder.go) |

## High-priority families (Phase 2 — inventory)

| Family | Typical approach | Notes |
|--------|------------------|-------|
| Comparison / logic (`googlesqlite_equal`, `googlesqlite_and`, …) | **rewrite** / SQL operators | DuckDB explicit rewrites for `=` / `IS NOT DISTINCT FROM` where needed |
| Strings / bytes / regex | **rename** or **rewrite** | Per-function semantics check |
| Date/time | **rewrite** / **macro** | Map to DuckDB date functions |
| JSON | **rewrite** | DuckDB `json_*`, `CAST(x AS JSON)` for parse |
| Aggregates / windows | **rename** (+ DISTINCT / `count(*)` normalization) | Option args / HAVING / ORDER BY in aggregate still gaps |
| Geography / ML | **unsupported** unless corpus needs | Lowest priority |

## How to update

1. When adding DuckDB support for a function, add a row or change status here.
2. If strategy is **unsupported**, document the stable error text or issue link in the roadmap.
