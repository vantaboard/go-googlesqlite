package core

import (
	"strings"

	"github.com/vantaboard/go-googlesql/types"

	sqlexpr "github.com/vantaboard/go-googlesql-engine/internal/sqlexpr"
)

// Dialect selects SQL generation and catalog persistence behavior for a physical engine.
type Dialect interface {
	ID() string
	WindowPartitionCollation() string
	RewriteEmittedFunctionName(name string) (string, bool)
	ArraySubqueryListAggregate() string
	WrapGroupByKey(expr *sqlexpr.SQLExpression) *sqlexpr.SQLExpression
	MaybeEmitNativeCast(inner *sqlexpr.SQLExpression, cast *sqlexpr.CastMetadata) (*sqlexpr.SQLExpression, error)
	ArrayUnnestUseLateralCorrelation() bool
	MergeTempTableName() string
	MergeScratchTableIsTemporary() bool
	PhysicalPrimaryKeyColumnListEntry(columnName string) string
	PhysicalUseWithoutRowID() bool
	PhysicalColumnStorageType(kind types.TypeKind) string
	QuoteIdent(ident string) string

	// NativeFunctionRewritesEnabled gates DuckDB-specific function-call rewrites and comparator folding.
	NativeFunctionRewritesEnabled() bool
	// RewritesSimpleCaseForWireBackedColumns rewrites simple CASE for VARCHAR wire payloads.
	RewritesSimpleCaseForWireBackedColumns() bool
	// UsesDollarNamedParameters converts @name markers to $name in raw SQL paths.
	UsesDollarNamedParameters() bool
	// StructExtractUsesFieldNameLiterals emits string field keys for struct_extract where needed.
	StructExtractUsesFieldNameLiterals() bool
	// DecodesGooglesqlWireLiteralsInSQL emits native literals from googlesql wire in SQL text.
	DecodesGooglesqlWireLiteralsInSQL() bool
	// FormatBoundParameter renders a bound-parameter token (e.g. DuckDB $name).
	FormatBoundParameter(identifier string) string
}

// ApplySortCollation sets expr.Collation when the dialect uses a sort collation (SQLite only today).
func ApplySortCollation(d Dialect, expr *sqlexpr.SQLExpression) {
	if d == nil || expr == nil {
		return
	}
	if c := d.WindowPartitionCollation(); c != "" {
		expr.Collation = c
	}
}

// IDSQLite is the stable backend identifier for SQLite.
const IDSQLite = "sqlite"

// IDDuckDB is the stable backend identifier for DuckDB.
const IDDuckDB = "duckdb"

// QuoteSQLiteIdent wraps ident in SQLite-style backticks.
func QuoteSQLiteIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

// QuoteDuckDBIdent wraps ident in DuckDB-style double quotes.
func QuoteDuckDBIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
