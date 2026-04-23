package internal

import (
	"fmt"

	"github.com/vantaboard/go-googlesql/types"
)

// MaybeEmitNativeCast implements Dialect for DuckDB: emit CAST / TRY_CAST for scalar types only.
func (DuckDBDialect) MaybeEmitNativeCast(inner *SQLExpression, cast *CastData) (*SQLExpression, error) {
	if inner == nil || cast == nil {
		return nil, nil
	}
	target, ok := duckDBSQLCastType(cast.ToType)
	if !ok {
		return nil, fmt.Errorf("duckdb: native CAST not implemented for target type %s", cast.ToType.DebugString(false))
	}
	try := cast.ReturnNullOnErr || cast.SafeCast
	if cast.FromType != nil && cast.FromType.Kind() == types.STRING && target != "VARCHAR" {
		inner = duckDBUnwireGooglesqlStringOperand(inner)
	}
	return NewSQLCastExpression(inner, target, try), nil
}

func duckDBSQLCastType(t types.Type) (sql string, ok bool) {
	if t == nil {
		return "", false
	}
	switch t.Kind() {
	case types.INT32:
		return "INTEGER", true
	case types.INT64:
		return "BIGINT", true
	case types.UINT32, types.UINT64:
		return "UBIGINT", true
	case types.BOOL:
		return "BOOLEAN", true
	case types.FLOAT:
		return "FLOAT", true
	case types.DOUBLE:
		return "DOUBLE", true
	case types.STRING:
		return "VARCHAR", true
	case types.BYTES:
		return "BLOB", true
	case types.DATE:
		return "DATE", true
	case types.TIMESTAMP:
		return "TIMESTAMP", true
	case types.TIME:
		return "TIME", true
	case types.DATETIME:
		return "TIMESTAMP", true
	case types.JSON:
		return "JSON", true
	case types.INTERVAL:
		return "INTERVAL", true
	case types.ENUM:
		return "VARCHAR", true
	case types.NUMERIC, types.BIG_NUMERIC:
		// Wide decimal; adjust if a workload needs exact BigQuery NUMERIC scale.
		return "DECIMAL(38, 9)", true
	default:
		return "", false
	}
}
