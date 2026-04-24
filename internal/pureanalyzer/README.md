# pureanalyzer

Pure-Go subset analyzer for GoogleSQL `SELECT` shapes used in roadmap work toward
removing CGO from the engine-facing path.

## Oracle fixtures

- SQL: `testdata/oracle/*.sql`
- Expected CGO summaries: `testdata/oracle/*.golden` (line-oriented; see `ResolvedQuerySummary`).
- Regenerate goldens after intentional GoogleSQL output changes:

```bash
go test ./internal/pureanalyzer/... -tags "$GOOGLESQL_BUILD_TAGS" -run TestOracleGoldensMatchCGO -count=1 -update-oracle
```

(use the same tags and `go-googlesql` prebuilts as `task test:prebuilt`).

## Differential tests

`TestPureSelectMatchesOracleGoldens` runs CGO analyze and compares `ResolvedQuerySummary`
to `PureSelectSummary(AnalyzeSelect(...))` for each fixture.

## Supported subset (initial)

- `SELECT *` or `SELECT col1, ...` from a single table
- Optional `WHERE` with comparisons, `AND`/`OR`, parentheses, named `@param` and `?`
- Literals: integers, single-quoted strings, `TRUE`/`FALSE`
- Binary ops mapped to `$equal`, `$and`, etc., matching resolved AST naming
- Small builtin set: `ABS`, `CEIL`, `FLOOR`, `CONCAT`, `LOWER`, `UPPER`, `TRIM`, `LENGTH`, `CHAR_LENGTH`

Anything else returns a parse error or `ErrUnsupportedFeature` from `AnalyzeSelect`.

## Engine integration

- `internal/analysis_driver.go`: [StatementAnalysisDriver] with default CGO implementation.
- Set `GOOGLESQL_ENGINE_PURE_ANALYZER_VALIDATE=1` to assert pure subset summaries match CGO for applicable `QueryStmt` queries after each successful analyze.

See [docs/pure-analyzer-roadmap.md](../../docs/pure-analyzer-roadmap.md) for expansion order.
