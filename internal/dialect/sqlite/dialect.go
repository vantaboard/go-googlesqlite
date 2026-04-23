package sqlite

import (
	"github.com/vantaboard/go-googlesql/types"

	"github.com/vantaboard/go-googlesql-engine/internal/dialect/core"
	sqlexpr "github.com/vantaboard/go-googlesql-engine/internal/sqlexpr"
)

// Dialect is the default GoogleSQL-to-SQLite codegen target.
type Dialect struct{}

func (Dialect) ID() string { return core.IDSQLite }

func (Dialect) WindowPartitionCollation() string { return "googlesqlengine_collate" }

func (Dialect) RewriteEmittedFunctionName(name string) (string, bool) {
	return name, false
}

func (Dialect) ArraySubqueryListAggregate() string { return "googlesqlengine_array" }

func (Dialect) WrapGroupByKey(expr *sqlexpr.SQLExpression) *sqlexpr.SQLExpression {
	return sqlexpr.NewFunctionExpression("googlesqlengine_group_by", expr)
}

func (Dialect) MaybeEmitNativeCast(_ *sqlexpr.SQLExpression, _ *sqlexpr.CastMetadata) (*sqlexpr.SQLExpression, error) {
	return nil, nil
}

func (Dialect) ArrayUnnestUseLateralCorrelation() bool { return false }

func (Dialect) MergeTempTableName() string { return "googlesqlengine_merged_table" }

func (Dialect) MergeScratchTableIsTemporary() bool { return false }

func (d Dialect) PhysicalPrimaryKeyColumnListEntry(columnName string) string {
	return d.QuoteIdent(columnName) + " COLLATE googlesqlengine_collate"
}

func (Dialect) QuoteIdent(ident string) string { return core.QuoteSQLiteIdent(ident) }

func (Dialect) PhysicalUseWithoutRowID() bool { return true }

func (Dialect) PhysicalColumnStorageType(kind types.TypeKind) string {
	switch kind {
	case types.INT32, types.INT64, types.UINT32, types.UINT64, types.ENUM:
		return "INT"
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
		return "TEXT"
	case types.DATE, types.TIMESTAMP, types.ARRAY, types.STRUCT, types.PROTO, types.TIME,
		types.DATETIME, types.GEOGRAPHY, types.NUMERIC, types.BIG_NUMERIC, types.EXTENDED, types.INTERVAL:
		return "TEXT"
	default:
		return "UNKNOWN"
	}
}

func (Dialect) NativeFunctionRewritesEnabled() bool { return false }

func (Dialect) RewritesSimpleCaseForWireBackedColumns() bool { return false }

func (Dialect) UsesDollarNamedParameters() bool { return false }

func (Dialect) StructExtractUsesFieldNameLiterals() bool { return false }

func (Dialect) DecodesGooglesqlWireLiteralsInSQL() bool { return false }

func (Dialect) FormatBoundParameter(identifier string) string { return identifier }
