package internal

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/goccy/go-json"
	"github.com/vantaboard/go-googlesql/types"
)

// FunctionCallTransformer handles transformation of function calls from GoogleSQL to SQLite.
//
// BigQuery/GoogleSQL supports a rich set of built-in functions with different semantics than SQLite.
// This transformer bridges the gap by:
// - Converting GoogleSQL function calls to SQLite equivalents
// - Handling special GoogleSQL functions (IFNULL, IF, CASE) via custom googlesqlite_* functions
// - Managing window functions with proper OVER clause transformation
// - Processing function arguments recursively through the coordinator
// - Injecting current time for time-dependent functions when needed
//
// Key GoogleSQL -> SQLite transformations handled:
// - googlesqlite_ifnull -> CASE WHEN...IS NULL pattern
// - googlesqlite_if -> CASE WHEN...THEN...ELSE pattern
// - googlesqlite_case_* -> CASE expressions with proper value/condition handling
// - Window functions with PARTITION BY, ORDER BY, and frame specifications
// - Built-in function mapping through the function registry
//
// The transformer ensures function semantics are preserved across the SQL dialect boundary.
type FunctionCallTransformer struct {
	coordinator Coordinator // For recursive transformation of arguments
}

// NewFunctionCallTransformer creates a new function call transformer
func NewFunctionCallTransformer(coordinator Coordinator) *FunctionCallTransformer {
	return &FunctionCallTransformer{
		coordinator: coordinator,
	}
}

// Transform converts FunctionCallData to SQLExpression
func (t *FunctionCallTransformer) Transform(data ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	if data.Type != ExpressionTypeFunction || data.Function == nil {
		return nil, fmt.Errorf("expected function call expression data, got type %v", data.Type)
	}

	function := data.Function

	// Transform arguments recursively
	args := make([]*SQLExpression, 0, len(function.Arguments))
	for i, argData := range function.Arguments {
		arg, err := t.coordinator.TransformExpression(argData, ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to transform function argument %d: %w", i, err)
		}
		args = append(args, arg)
	}

	_, existsCurrentTime := currentTimeFuncMap[data.Function.Name]
	if existsCurrentTime {
		currentTime := CurrentTime(ctx.Context())
		if currentTime != nil {
			encodedCurrentTime, err := NewLiteralExpressionFromGoValue(types.Int64Type(), currentTime.UnixNano())
			if err != nil {
				return nil, fmt.Errorf("failed to encode current time: %w", err)
			}
			args = append(args, encodedCurrentTime)
		}
	}

	// Handle special GoogleSQL functions that need transformation
	switch function.Name {
	case "googlesqlite_ifnull":
		// Convert to CASE expression: IFNULL(a, b) => CASE WHEN a IS NULL THEN b ELSE a END
		if len(args) != 2 {
			return nil, fmt.Errorf("googlesqlite_ifnull requires exactly 2 arguments")
		}
		return NewCaseExpression(
			[]*WhenClause{
				{
					Condition: NewBinaryExpression(args[0], "IS", NewLiteralExpression("NULL")),
					Result:    args[1],
				},
			},
			args[0],
		), nil

	case "googlesqlite_if":
		// Convert to CASE expression: IF(condition, then_result, else_result) => CASE WHEN condition THEN then_result ELSE else_result END
		if len(args) != 3 {
			return nil, fmt.Errorf("googlesqlite_if requires exactly 3 arguments")
		}
		return NewCaseExpression([]*WhenClause{{Condition: args[0], Result: args[1]}}, args[2]), nil

	case "googlesqlite_case_no_value":
		// Convert to CASE expression: arguments are condition, result, condition, result, ..., [else]
		whenClauses := make([]*WhenClause, 0, len(args)/2)
		for i := 0; i < len(args)-1; i += 2 {
			whenClauses = append(whenClauses, &WhenClause{
				Condition: args[i],
				Result:    args[i+1],
			})
		}
		var elseExpr *SQLExpression
		// if args length is odd number, else statement exists
		if len(args) > (len(args)/2)*2 {
			elseExpr = args[len(args)-1]
		}
		return NewCaseExpression(whenClauses, elseExpr), nil

	case "googlesqlite_case_with_value":
		// Convert to CASE expression with value: first arg is value, then condition, result, condition, result, ..., [else]
		if len(args) < 3 {
			return nil, fmt.Errorf("googlesqlite_case_with_value requires at least 3 arguments")
		}

		valueExpr := args[0]
		remainingArgs := args[1:]

		whenClauses := make([]*WhenClause, 0, len(remainingArgs)/2)
		for i := 0; i < len(remainingArgs)-1; i += 2 {
			whenClauses = append(whenClauses, &WhenClause{
				Condition: remainingArgs[i],
				Result:    remainingArgs[i+1],
			})
		}
		var elseExpr *SQLExpression
		// if remaining args length is odd number, else statement exists
		if len(remainingArgs) > (len(remainingArgs)/2)*2 {
			elseExpr = remainingArgs[len(remainingArgs)-1]
		}
		return NewSimpleCaseExpression(valueExpr, whenClauses, elseExpr), nil

	default:
		var windowSpec *WindowSpecification
		if function.WindowSpec != nil {
			// Transform PARTITION BY expressions
			partitionBy := make([]*SQLExpression, 0, len(function.WindowSpec.PartitionBy))
			for _, partData := range function.WindowSpec.PartitionBy {
				expr, err := t.coordinator.TransformExpression(*partData, ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to transform partition by expression: %w", err)
				}

				ApplySortCollation(ctx.Dialect(), expr)

				partitionBy = append(partitionBy, expr)
			}

			// Transform ORDER BY expressions
			orderBy := make([]*OrderByItem, 0, len(function.WindowSpec.OrderBy))
			for _, orderData := range function.WindowSpec.OrderBy {
				expr, err := t.coordinator.TransformExpression(orderData.Expression, ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to transform order by expression: %w", err)
				}
				orderByItems, err := createOrderByItems(expr, orderData, ctx.Dialect())
				if err != nil {
					return nil, fmt.Errorf("failed to create order by items: %w", err)
				}
				orderBy = append(orderBy, orderByItems...)
			}

			// Transform frame clause if present
			var frameClause *FrameClause
			if function.WindowSpec.FrameClause != nil {
				frameData := function.WindowSpec.FrameClause
				frameClause = &FrameClause{
					Unit: frameData.Unit,
				}

				// Transform start bound
				if frameData.Start != nil {
					var startOffset *SQLExpression
					if frameData.Start.Offset != (ExpressionData{}) {
						var err error
						startOffset, err = t.coordinator.TransformExpression(frameData.Start.Offset, ctx)
						if err != nil {
							return nil, fmt.Errorf("failed to transform frame start offset: %w", err)
						}
					}
					frameClause.Start = &FrameBound{
						Type:   frameData.Start.Type,
						Offset: startOffset,
					}
				}

				// Transform end bound
				if frameData.End != nil {
					var endOffset *SQLExpression
					if frameData.End.Offset != (ExpressionData{}) {
						var err error
						endOffset, err = t.coordinator.TransformExpression(frameData.End.Offset, ctx)
						if err != nil {
							return nil, fmt.Errorf("failed to transform frame end offset: %w", err)
						}
					}
					frameClause.End = &FrameBound{
						Type:   frameData.End.Type,
						Offset: endOffset,
					}
				}
			}

			windowSpec = &WindowSpecification{
				PartitionBy: partitionBy,
				OrderBy:     orderBy,
				FrameClause: frameClause,
			}
		}

		// DuckDB-native rewrites (arity-sensitive or frozen-clock current time). Skip when a window
		// is attached; OVER on these shapes is rare and needs per-case design.
		if windowSpec == nil {
			if rewritten, ok := duckDBRewriteFunctionCall(function.Name, args, ctx.Dialect()); ok {
				return rewritten, nil
			}
		}

		// DuckDB has no googlesqlite_* comparators/logical helpers. Fold them to native operators even
		// when arguments are column refs (metadata DELETE/UPDATE WHERE), which skip the primitive-only
		// fast path below.
		if ctx.Dialect() != nil && ctx.Dialect().ID() == "duckdb" {
			if rewritten, ok := duckDBOptimizeComparatorOrLogical(function.Name, args); ok {
				return rewritten, nil
			}
		}

		// Fast path optimization: bypass function calls for primitive type operations
		// Function calls incur huge overheads: as each call's args must be decoded/encoded, as well as
		// allocated within both the modernc.org/sqlite driver and the go-googlesqlite driver
		// This could happen potentially hundreds of thousands of times per query in the case of complex JOINs
		if canOptimizeFunction(function) {
			return optimizeFunctionToSQL(function.Name, args)
		}

		funcMap := funcMapFromContext(ctx.Context())
		if spec, exists := funcMap[function.Name]; exists {
			return spec.CallSQL(ctx.Context(), function, args)
		}
		if analyzer := analyzerFromContext(ctx.Context()); analyzer != nil && analyzer.catalog != nil {
			for _, spec := range analyzer.catalog.functions {
				if spec.FuncName() == function.Name {
					return spec.CallSQL(ctx.Context(), function, args)
				}
			}
		}
		// Default function call transformation
		emitName := function.Name
		if n, ok := ctx.Dialect().RewriteEmittedFunctionName(function.Name); ok {
			emitName = n
		}
		return &SQLExpression{
			Type: ExpressionTypeFunction,
			FunctionCall: &FunctionCall{
				Name:       emitName,
				Arguments:  args,
				WindowSpec: windowSpec,
			},
		}, nil
	}
}

// duckDBOptimizeComparatorOrLogical maps googlesqlite_* helpers to native SQL operators for DuckDB
// without requiring primitive-only arguments (see canOptimizeFunction).
func duckDBOptimizeComparatorOrLogical(name string, args []*SQLExpression) (*SQLExpression, bool) {
	switch name {
	case "googlesqlite_and", "googlesqlite_or":
		if len(args) < 2 {
			return nil, false
		}
		e, err := optimizeFunctionToSQL(name, args)
		return e, err == nil
	case "googlesqlite_not":
		if len(args) != 1 {
			return nil, false
		}
		e, err := optimizeFunctionToSQL(name, args)
		return e, err == nil
	case "googlesqlite_in":
		if len(args) < 2 {
			return nil, false
		}
		e, err := optimizeFunctionToSQL(name, args)
		return e, err == nil
	default:
		if _, found := functionToOperator[name]; !found {
			return nil, false
		}
		if len(args) != 2 {
			return nil, false
		}
		e, err := optimizeFunctionToSQL(name, args)
		return e, err == nil
	}
}

// canOptimizeFunction checks if a function can be optimized to use direct SQL operators
func canOptimizeFunction(function *FunctionCallData) bool {
	_, found := functionToOperator[function.Name]
	if !found {
		return false
	}

	// Check argument count requirements
	switch function.Name {
	case "googlesqlite_not":
		if len(function.Arguments) != 1 {
			return false
		}
	case "googlesqlite_and", "googlesqlite_or":
		if len(function.Arguments) < 2 {
			return false
		}
	default: // comparison operators
		if len(function.Arguments) != 2 {
			return false
		}
	}

	// All arguments must be primitive SQLite-compatible types or optimizable expressions
	for _, arg := range function.Arguments {
		if !isPrimitiveSQLiteType(arg) {
			return false
		}
	}

	return true
}

var functionToOperator = map[string]string{
	// Comparison operators
	"googlesqlite_equal":                "=",
	"googlesqlite_not_equal":            "!=",
	"googlesqlite_less":                 "<",
	"googlesqlite_greater":              ">",
	"googlesqlite_less_or_equal":        "<=",
	"googlesqlite_greater_or_equal":     ">=",
	"googlesqlite_in":                   "IN",
	"googlesqlite_is_not_distinct_from": "IS NOT DISTINCT FROM",
	"googlesqlite_is_distinct_from":     "IS DISTINCT FROM",
	// Logical operators
	"googlesqlite_and": "AND",
	"googlesqlite_or":  "OR",
	"googlesqlite_not": "NOT",
}

// optimizeFunctionToSQL converts functions to direct SQL operators
func optimizeFunctionToSQL(functionName string, args []*SQLExpression) (*SQLExpression, error) {
	operator, found := functionToOperator[functionName]
	if !found {
		return nil, fmt.Errorf("unknown optimizable function: %s", functionName)
	}

	switch functionName {
	case "googlesqlite_and", "googlesqlite_or":
		if len(args) < 2 {
			return nil, fmt.Errorf("%s expected at least 2 arguments, got %d", functionName, len(args))
		}
		// Chain multiple arguments with the operator
		result := args[0]
		for i := 1; i < len(args); i++ {
			result = NewBinaryExpression(result, operator, args[i])
		}
		return result, nil

	case "googlesqlite_not":
		if len(args) != 1 {
			return nil, fmt.Errorf("%s expected only 1 argument, got %d", functionName, len(args))
		}
		return NewNotExpression(args[0]), nil
	case "googlesqlite_in":
		return NewBinaryExpression(args[0], operator, NewListExpression(args[1:])), nil
	default: // comparison operators
		if len(args) != 2 {
			return nil, fmt.Errorf("%s expected 2 arguments, got %d", functionName, len(args))
		}
		return NewBinaryExpression(args[0], operator, args[1]), nil
	}
}

// isPrimitiveSQLiteType checks if an expression represents a primitive type that SQLite can handle natively
// or if it's an already-optimized expression that can be further optimized
func isPrimitiveSQLiteType(expr ExpressionData) bool {
	switch expr.Type {
	case ExpressionTypeLiteral:
		if expr.Literal == nil || expr.Literal.Value == nil {
			return false
		}
		// Check if the literal value is a primitive type
		switch expr.Literal.Value.(type) {
		case IntValue, FloatValue, BoolValue:
			return true
		case StringValue:
			// String literals can be compared directly in SQLite
			return true
		default:
			return false
		}
	case ExpressionTypeColumn:
		t := expr.Column.Type
		return t.IsInt32() ||
			t.IsInt64() ||
			t.IsUint32() ||
			t.IsUint64() ||
			t.IsBool() ||
			t.IsFloat() ||
			t.IsDouble() ||
			t.IsString()
	case ExpressionTypeFunction:
		// If this is an optimizable function, it can be treated as primitive for further optimization
		if expr.Function != nil {
			_, found := functionToOperator[expr.Function.Name]
			return found && canOptimizeFunction(expr.Function)
		}
		return false
	default:
		return false
	}
}

// duckDBRewriteFunctionCall returns a DuckDB-native expression for special cases that are not
// covered by RewriteEmittedFunctionName alone (extra args, frozen clock, or arity-specific INSTR).
func duckDBRewriteFunctionCall(name string, args []*SQLExpression, d Dialect) (*SQLExpression, bool) {
	if d == nil || d.ID() != "duckdb" {
		return nil, false
	}
	switch name {
	case "googlesqlite_equal":
		if len(args) == 2 {
			return NewBinaryExpression(args[0], "=", args[1]), true
		}
	case "googlesqlite_is_not_distinct_from":
		if len(args) == 2 {
			return NewBinaryExpression(args[0], "IS NOT DISTINCT FROM", args[1]), true
		}
	case "googlesqlite_add", "googlesqlite_safe_add":
		if len(args) == 2 {
			return NewBinaryExpression(args[0], "+", args[1]), true
		}
	case "googlesqlite_subtract", "googlesqlite_safe_subtract":
		if len(args) == 2 {
			return NewBinaryExpression(args[0], "-", args[1]), true
		}
	case "googlesqlite_multiply", "googlesqlite_safe_multiply":
		if len(args) == 2 {
			return NewBinaryExpression(args[0], "*", args[1]), true
		}
	case "googlesqlite_divide", "googlesqlite_safe_divide":
		if len(args) == 2 {
			return NewBinaryExpression(args[0], "/", args[1]), true
		}
	case "googlesqlite_extract":
		if len(args) >= 2 {
			part, ok := googlesqliteWireStringArg(args[1])
			if !ok {
				break
			}
			duckPart, ok := duckDBDatePartComponent(part)
			if !ok {
				break
			}
			srcExpr := args[0]
			if len(args) == 3 {
				if zone, zok := googlesqliteWireStringArg(args[2]); zok && zone != "" && !strings.EqualFold(zone, "UTC") {
					zoneLit := "'" + strings.ReplaceAll(zone, "'", "''") + "'"
					srcExpr = NewFunctionExpression("timezone", NewLiteralExpression(zoneLit), srcExpr)
				}
			}
			partLit := "'" + strings.ReplaceAll(duckPart, "'", "''") + "'"
			return NewFunctionExpression("date_part", NewLiteralExpression(partLit), srcExpr), true
		}
	case "googlesqlite_parse_json":
		if len(args) >= 1 {
			// DuckDB: CAST(string AS JSON); drop BigQuery optional widen mode when present.
			return NewSQLCastExpression(args[0], "JSON", false), true
		}
	case "googlesqlite_is_null":
		if len(args) == 1 {
			return NewBinaryExpression(args[0], "IS", NewLiteralExpression("NULL")), true
		}
	case "googlesqlite_not":
		if len(args) == 1 {
			return NewNotExpression(args[0]), true
		}
	case "googlesqlite_byte_length":
		if len(args) == 1 {
			blob := NewSQLCastExpression(args[0], "BLOB", false)
			return NewFunctionExpression("octet_length", blob), true
		}
	case "googlesqlite_instr":
		if len(args) == 2 {
			return NewFunctionExpression("strpos", args...), true
		}
	case "googlesqlite_between":
		if len(args) == 3 {
			// Matches runtime BETWEEN (bindBetween): inclusive bounds; any NULL arg -> NULL via SQL 3VL.
			ge := NewBinaryExpression(args[0], ">=", args[1])
			le := NewBinaryExpression(args[0], "<=", args[2])
			return NewBinaryExpression(ge, "AND", le), true
		}
	case "googlesqlite_get_struct_field":
		if len(args) == 2 {
			// Named STRUCT: second arg is a string field key (see extractGetStructFieldData for DuckDB).
			if key, ok := googlesqliteWireStringArg(args[1]); ok {
				esc := strings.ReplaceAll(key, "'", "''")
				return NewFunctionExpression("struct_extract", args[0], NewLiteralExpression("'"+esc+"'")), true
			}
			idx, ok := googlesqliteWireIntArg(args[1])
			if !ok || idx < 0 {
				break
			}
			// Anonymous tuple: GoogleSQL field index is 0-based; DuckDB struct_extract index form is 1-based.
			duck1 := idx + 1
			return NewFunctionExpression("struct_extract", args[0], NewLiteralExpression(strconv.FormatInt(duck1, 10))), true
		}
	case "googlesqlite_date":
		switch len(args) {
		case 1:
			return NewSQLCastExpression(args[0], "DATE", false), true
		case 2:
			// DATE(ts, zone) -> CAST(timezone(zone, ts) AS DATE)
			tz := NewFunctionExpression("timezone", args[1], args[0])
			return NewSQLCastExpression(tz, "DATE", false), true
		case 3:
			return NewFunctionExpression("make_date", args[0], args[1], args[2]), true
		}
	case "googlesqlite_current_timestamp", "googlesqlite_current_datetime":
		switch len(args) {
		case 0:
			return NewFunctionExpression("current_timestamp"), true
		case 1:
			return duckDBToTimestampFromUnixNano(args[0]), true
		}
	case "googlesqlite_current_date":
		switch len(args) {
		case 0:
			return NewFunctionExpression("current_date"), true
		case 1:
			return NewSQLCastExpression(duckDBToTimestampFromUnixNano(args[0]), "DATE", false), true
		}
	case "googlesqlite_current_time":
		switch len(args) {
		case 0:
			return NewFunctionExpression("current_time"), true
		case 1:
			return NewSQLCastExpression(duckDBToTimestampFromUnixNano(args[0]), "TIME", false), true
		}
	}
	return nil, false
}

// googlesqliteWireStringArg decodes a string literal produced by LiteralFromValue (double-quoted base64 JSON layout).
// googlesqliteWireIntArg parses an integer literal emitted for the resolver (plain decimal or wire JSON layout).
func googlesqliteWireIntArg(expr *SQLExpression) (int64, bool) {
	if expr == nil || expr.Type != ExpressionTypeLiteral {
		return 0, false
	}
	s := strings.TrimSpace(expr.Value)
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, true
	}
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return 0, false
	}
	inner := s[1 : len(s)-1]
	b, err := base64.StdEncoding.DecodeString(inner)
	if err != nil {
		return 0, false
	}
	var layout ValueLayout
	if err := json.Unmarshal(b, &layout); err != nil || layout.Header != IntValueType {
		return 0, false
	}
	i, err := strconv.ParseInt(strings.TrimSpace(layout.Body), 10, 64)
	if err != nil {
		return 0, false
	}
	return i, true
}

func googlesqliteWireStringArg(expr *SQLExpression) (string, bool) {
	if expr == nil || expr.Type != ExpressionTypeLiteral {
		return "", false
	}
	s := strings.TrimSpace(expr.Value)
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return "", false
	}
	inner := s[1 : len(s)-1]
	b, err := base64.StdEncoding.DecodeString(inner)
	if err != nil {
		return "", false
	}
	var layout ValueLayout
	if err := json.Unmarshal(b, &layout); err != nil || layout.Header != StringValueType {
		return "", false
	}
	return layout.Body, true
}

var duckDBExtractPart = map[string]string{
	"YEAR":        "year",
	"MONTH":       "month",
	"DAY":         "day",
	"HOUR":        "hour",
	"MINUTE":      "minute",
	"SECOND":      "second",
	"MICROSECOND": "microseconds",
	"MILLISECOND": "milliseconds",
	"QUARTER":     "quarter",
	"DAYOFWEEK":   "dow",
	"DAYOFYEAR":   "doy",
	"WEEK":        "week",
	"ISOWEEK":     "isoweek",
	"ISOYEAR":     "isoyear",
}

func duckDBDatePartComponent(part string) (string, bool) {
	p := strings.ToUpper(strings.TrimSpace(part))
	if p == "" {
		return "", false
	}
	if mapped, ok := duckDBExtractPart[p]; ok {
		return mapped, true
	}
	return strings.ToLower(p), true
}

func duckDBToTimestampFromUnixNano(nanos *SQLExpression) *SQLExpression {
	sec := NewBinaryExpression(nanos, "/", NewLiteralExpression("1000000000.0"))
	return NewFunctionExpression("to_timestamp", sec)
}
