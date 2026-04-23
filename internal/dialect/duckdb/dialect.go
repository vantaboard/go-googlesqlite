package duckdb

import (
	"strings"

	"github.com/vantaboard/go-googlesql/types"

	"github.com/vantaboard/go-googlesql-engine/internal/dialect/core"
	sqlexpr "github.com/vantaboard/go-googlesql-engine/internal/sqlexpr"
)

// nativeFunctions maps googlesqlengine-prefixed runtime names to DuckDB builtins where semantics align.
var nativeFunctions = map[string]string{
	"googlesqlengine_length":      "length",
	"googlesqlengine_char_length": "length",
	"googlesqlengine_abs":         "abs",
	"googlesqlengine_lower":       "lower",
	"googlesqlengine_upper":       "upper",
	"googlesqlengine_substr":      "substr",
	"googlesqlengine_trim":        "trim",
	"googlesqlengine_ltrim":       "ltrim",
	"googlesqlengine_rtrim":       "rtrim",
	"googlesqlengine_replace":     "replace",
	"googlesqlengine_reverse":     "reverse",
	"googlesqlengine_repeat":      "repeat",
	"googlesqlengine_strpos":      "strpos",
	"googlesqlengine_chr":         "chr",
	"googlesqlengine_ascii":       "ascii",
	"googlesqlengine_starts_with": "starts_with",
	"googlesqlengine_ends_with":   "ends_with",
	"googlesqlengine_left":        "left",
	"googlesqlengine_right":       "right",
	"googlesqlengine_lpad":        "lpad",
	"googlesqlengine_rpad":        "rpad",
	"googlesqlengine_initcap":     "initcap",
	"googlesqlengine_unicode":     "unicode",
	"googlesqlengine_md5":         "md5",
	"googlesqlengine_sha1":        "sha1",
	"googlesqlengine_sha256":      "sha256",
	"googlesqlengine_sha512":      "sha512",
	"googlesqlengine_json_extract": "json_extract",
	"googlesqlengine_json_value":   "json_extract",
	"googlesqlengine_coalesce":     "coalesce",
	"googlesqlengine_error":        "error",
}

// Dialect is the GoogleSQL-to-DuckDB codegen target (incremental parity).
type Dialect struct{}

func (Dialect) ID() string { return core.IDDuckDB }

func (Dialect) WindowPartitionCollation() string { return "" }

func (Dialect) RewriteEmittedFunctionName(name string) (string, bool) {
	if alt, ok := nativeFunctions[name]; ok {
		return alt, true
	}
	if alt, ok := windowRenames[name]; ok {
		return alt, true
	}
	if alt, ok := aggregateRenames[name]; ok {
		return alt, true
	}
	return name, false
}

func (Dialect) ArraySubqueryListAggregate() string { return "list" }

func (Dialect) WrapGroupByKey(expr *sqlexpr.SQLExpression) *sqlexpr.SQLExpression { return expr }

func (Dialect) ArrayUnnestUseLateralCorrelation() bool { return true }

func (Dialect) MergeTempTableName() string { return "googlesqlengine_merged_table" }

func (Dialect) MergeScratchTableIsTemporary() bool { return true }

func (d Dialect) PhysicalPrimaryKeyColumnListEntry(columnName string) string {
	return d.QuoteIdent(columnName)
}

func (Dialect) QuoteIdent(ident string) string { return core.QuoteDuckDBIdent(ident) }

func (Dialect) PhysicalUseWithoutRowID() bool { return false }

func (Dialect) PhysicalColumnStorageType(kind types.TypeKind) string {
	switch kind {
	case types.INT32, types.INT64, types.UINT32, types.UINT64, types.ENUM:
		return "BIGINT"
	case types.BOOL:
		return "BOOLEAN"
	case types.FLOAT:
		return "FLOAT"
	case types.BYTES:
		return "BLOB"
	case types.DOUBLE:
		return "DOUBLE"
	case types.JSON:
		return "JSON"
	case types.STRING:
		return "VARCHAR"
	case types.DATE, types.TIMESTAMP, types.TIME, types.DATETIME, types.ARRAY, types.STRUCT, types.PROTO,
		types.GEOGRAPHY, types.NUMERIC, types.BIG_NUMERIC, types.EXTENDED, types.INTERVAL:
		return "VARCHAR"
	default:
		return "UNKNOWN"
	}
}

func (Dialect) NativeFunctionRewritesEnabled() bool { return true }

func (Dialect) RewritesSimpleCaseForWireBackedColumns() bool { return true }

func (Dialect) UsesDollarNamedParameters() bool { return true }

func (Dialect) StructExtractUsesFieldNameLiterals() bool { return true }

func (Dialect) DecodesGooglesqlWireLiteralsInSQL() bool { return true }

func (Dialect) FormatBoundParameter(identifier string) string {
	if identifier == "?" {
		return "?"
	}
	if strings.HasPrefix(identifier, "@") {
		return "$" + strings.TrimPrefix(identifier, "@")
	}
	return identifier
}
