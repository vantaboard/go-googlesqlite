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
		if ctx.Dialect() != nil && ctx.Dialect().ID() == "duckdb" {
			// Simple CASE compares CASE expr to each WHEN value with '='. VARCHAR columns often
			// hold googlesqlite wire while literals decode to plain text — no branch matches (see
			// UNNEST vs table column). Rewrite to searched CASE on unwire(expr) = unwire(when).
			return duckDBSimpleCaseExprToSearchedCaseForWireStrings(valueExpr, whenClauses, elseExpr), nil
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
			if rewritten, ok := duckDBRewriteFunctionCall(function.Name, args, function.Arguments, ctx.Dialect()); ok {
				return rewritten, nil
			}
		}

		// DuckDB has no googlesqlite_* comparators/logical helpers. Fold them to native operators even
		// when arguments are column refs (metadata DELETE/UPDATE WHERE), which skip the primitive-only
		// fast path below.
		if ctx.Dialect() != nil && ctx.Dialect().ID() == "duckdb" {
			if rewritten, ok := duckDBOptimizeComparatorOrLogical(function.Name, args, function.Arguments); ok {
				return rewritten, nil
			}
		}

		// Fast path optimization: bypass function calls for primitive type operations
		// Function calls incur huge overheads: as each call's args must be decoded/encoded, as well as
		// allocated within both the modernc.org/sqlite driver and the go-googlesqlite driver
		// This could happen potentially hundreds of thousands of times per query in the case of complex JOINs
		if canOptimizeFunction(function) {
			e, err := optimizeFunctionToSQL(function.Name, args)
			if err != nil {
				return nil, err
			}
			if ctx.Dialect() != nil && ctx.Dialect().ID() == "duckdb" {
				return duckDBCoerceOptimizedCallForDuckDB(function.Name, args, function.Arguments, e), nil
			}
			return e, nil
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

// duckDBComparisonOps is the set of binary operators emitted from googlesqlite_* comparators
// that require homogeneous temporal types in DuckDB (physical VARCHAR vs CAST DATE, etc.).
var duckDBComparisonOps = map[string]struct{}{
	"=": {}, "!=": {}, "<": {}, ">": {}, "<=": {}, ">=": {},
	"IS NOT DISTINCT FROM": {}, "IS DISTINCT FROM": {},
}

func isDuckDBComparisonOperator(op string) bool {
	_, ok := duckDBComparisonOps[op]
	return ok
}

func duckDBExprIsList(e *SQLExpression) bool {
	return e != nil && e.Type == ExpressionTypeList && e.ListExpression != nil
}

// duckDBExprShouldUnwireBeforeTemporalCast reports whether TRY_CAST(... AS DATE/TIMESTAMP/...)
// should run on the decoded googlesqlite payload: DuckDB stores DATE/TIMESTAMP and many STRING
// values as VARCHAR wire (base64+JSON); TRY_CAST on the raw cell is always NULL.
func duckDBExprShouldUnwireBeforeTemporalCast(d ExpressionData) bool {
	if _, ok := duckDBExpressionDataTemporalTarget(d); ok {
		return true
	}
	if d.Type == ExpressionTypeColumn && d.Column != nil && d.Column.Type != nil && d.Column.Type.IsString() {
		return true
	}
	if d.Type == ExpressionTypeCast && d.Cast != nil && d.Cast.FromType != nil && d.Cast.FromType.IsString() {
		return true
	}
	return false
}

// duckDBTryCastTemporalMaybeWire applies TRY_CAST after optional wire unwrap for DuckDB VARCHAR storage.
func duckDBTryCastTemporalMaybeWire(expr *SQLExpression, meta ExpressionData, target string) *SQLExpression {
	inner := expr
	if duckDBExprShouldUnwireBeforeTemporalCast(meta) {
		inner = duckDBUnwireGooglesqlStringOperand(expr)
	}
	return NewSQLCastExpression(inner, target, true)
}

func duckDBTryCastTemporalMaybeWirePtr(expr *SQLExpression, meta *ExpressionData, target string) *SQLExpression {
	if meta == nil {
		return NewSQLCastExpression(expr, target, true)
	}
	return duckDBTryCastTemporalMaybeWire(expr, *meta, target)
}

// duckDBExpressionDataTemporalTarget maps GoogleSQL column/cast types to DuckDB temporal cast targets.
func duckDBExpressionDataTemporalTarget(d ExpressionData) (string, bool) {
	switch d.Type {
	case ExpressionTypeColumn:
		if d.Column == nil || d.Column.Type == nil {
			return "", false
		}
		t := d.Column.Type
		if t.IsDate() {
			return "DATE", true
		}
		if t.IsTimestamp() {
			return "TIMESTAMP", true
		}
		if t.IsDatetime() {
			return "TIMESTAMP", true
		}
		if t.IsTime() {
			return "TIME", true
		}
		return "", false
	case ExpressionTypeCast:
		if d.Cast == nil || d.Cast.ToType == nil {
			return "", false
		}
		t := d.Cast.ToType
		if t.IsDate() {
			return "DATE", true
		}
		if t.IsTimestamp() {
			return "TIMESTAMP", true
		}
		if t.IsDatetime() {
			return "TIMESTAMP", true
		}
		if t.IsTime() {
			return "TIME", true
		}
		return "", false
	default:
		return "", false
	}
}

// duckDBIntervalExpressionData reports whether d is logically an INTERVAL (no temporal cast for date_part).
func duckDBIntervalExpressionData(d ExpressionData) bool {
	switch d.Type {
	case ExpressionTypeColumn:
		return d.Column != nil && d.Column.Type != nil && d.Column.Type.IsInterval()
	case ExpressionTypeCast:
		return d.Cast != nil && d.Cast.ToType != nil && d.Cast.ToType.IsInterval()
	default:
		return false
	}
}

// duckDBWrapExprForDuckDBDatePart casts VARCHAR-backed GoogleSQL temporals so DuckDB date_part
// sees DATE / TIMESTAMP / TIME (DuckDB has no date_part(..., VARCHAR)).
func duckDBWrapExprForDuckDBDatePart(src *SQLExpression, srcMeta ExpressionData, duckPart string) *SQLExpression {
	if duckDBIntervalExpressionData(srcMeta) {
		return src
	}
	if castTo, ok := duckDBExpressionDataTemporalTarget(srcMeta); ok {
		return duckDBTryCastTemporalMaybeWire(src, srcMeta, castTo)
	}
	// Unknown static type (still often VARCHAR in DuckDB storage): pick a TRY_CAST target from part.
	switch strings.ToLower(strings.TrimSpace(duckPart)) {
	case "year", "month", "day", "quarter", "dow", "doy", "week", "isoweek", "isoyear":
		return NewSQLCastExpression(duckDBUnwireGooglesqlStringOperand(src), "DATE", true)
	case "hour", "minute", "second", "milliseconds", "microseconds":
		return NewSQLCastExpression(duckDBUnwireGooglesqlStringOperand(src), "TIMESTAMP", true)
	default:
		return NewSQLCastExpression(duckDBUnwireGooglesqlStringOperand(src), "TIMESTAMP", true)
	}
}

func expressionDataIsIntegralFamily(d ExpressionData) bool {
	if d.Type != ExpressionTypeColumn || d.Column == nil || d.Column.Type == nil {
		return false
	}
	t := d.Column.Type
	return t.IsInt32() || t.IsInt64() || t.IsUint32() || t.IsUint64()
}

func duckDBMergeTemporalTargets(picks []string) string {
	hasTS := false
	hasDate := false
	hasTime := false
	for _, p := range picks {
		switch p {
		case "TIMESTAMP", "TIMESTAMPTZ":
			hasTS = true
		case "DATE":
			hasDate = true
		case "TIME":
			hasTime = true
		}
	}
	if hasTS {
		return "TIMESTAMP"
	}
	if hasDate && hasTime {
		return ""
	}
	if hasDate {
		return "DATE"
	}
	if hasTime {
		return "TIME"
	}
	return ""
}

// duckDBPickTemporalComparisonTarget combines CAST targets on SQL nodes with GoogleSQL types on ExpressionData.
func duckDBPickTemporalComparisonTarget(left, right *SQLExpression, ld, rd ExpressionData) string {
	var picks []string
	if t, ok := duckDBExprTemporalCastTarget(left); ok {
		picks = append(picks, t)
	}
	if t, ok := duckDBExprTemporalCastTarget(right); ok {
		picks = append(picks, t)
	}
	if t, ok := duckDBExpressionDataTemporalTarget(ld); ok {
		picks = append(picks, t)
	}
	if t, ok := duckDBExpressionDataTemporalTarget(rd); ok {
		picks = append(picks, t)
	}
	return duckDBMergeTemporalTargets(picks)
}

// duckDBExprTemporalCastTarget reports whether expr is CAST/TRY_CAST to a temporal scalar type
// and returns the DuckDB type name to use for TRY_CAST on the other side of a comparison.
func duckDBExprTemporalCastTarget(e *SQLExpression) (target string, ok bool) {
	if e == nil || e.Type != ExpressionTypeCast || e.Cast == nil {
		return "", false
	}
	t := strings.ToUpper(strings.TrimSpace(e.Cast.TargetType))
	switch t {
	case "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "DATETIME":
		if t == "DATETIME" {
			return "TIMESTAMP", true
		}
		return t, true
	default:
		return "", false
	}
}

// duckDBApplyTemporalComparisonCoercion wraps the non-cast side with TRY_CAST when exactly one
// operand is already a temporal CAST/TRY_CAST. Skips when the other side is an IN-list.
// ld/rd are optional ExpressionData for the left/right operand (unwrap VARCHAR wire before TRY_CAST).
func duckDBApplyTemporalComparisonCoercion(left, right *SQLExpression, op string, ld, rd *ExpressionData) (*SQLExpression, *SQLExpression) {
	if !isDuckDBComparisonOperator(op) {
		return left, right
	}
	if duckDBExprIsList(left) || duckDBExprIsList(right) {
		return left, right
	}
	lt, lok := duckDBExprTemporalCastTarget(left)
	rt, rok := duckDBExprTemporalCastTarget(right)
	if lok && !rok && !duckDBExprIsList(right) {
		right = duckDBTryCastTemporalMaybeWirePtr(right, rd, lt)
	} else if rok && !lok && !duckDBExprIsList(left) {
		left = duckDBTryCastTemporalMaybeWirePtr(left, ld, rt)
	}
	return left, right
}

// duckDBApplyTemporalComparisonCoercionWithExprData uses GoogleSQL types on ExpressionData so
// projected DATE columns (plain identifiers) still coerce against VARCHAR-backed DATE fields.
func duckDBApplyTemporalComparisonCoercionWithExprData(left, right *SQLExpression, ld, rd ExpressionData, op string) (*SQLExpression, *SQLExpression) {
	if !isDuckDBComparisonOperator(op) {
		return left, right
	}
	if duckDBExprIsList(left) || duckDBExprIsList(right) {
		return left, right
	}
	target := duckDBPickTemporalComparisonTarget(left, right, ld, rd)
	if target != "" {
		if expressionDataIsIntegralFamily(ld) || expressionDataIsIntegralFamily(rd) {
			return duckDBApplyTemporalComparisonCoercion(left, right, op, &ld, &rd)
		}
		return duckDBTryCastTemporalMaybeWire(left, ld, target), duckDBTryCastTemporalMaybeWire(right, rd, target)
	}
	return duckDBApplyTemporalComparisonCoercion(left, right, op, &ld, &rd)
}

// duckDBCoerceOptimizedCallForDuckDB reapplies temporal comparison rules on the optimized primitive path.
func duckDBCoerceOptimizedCallForDuckDB(name string, args []*SQLExpression, argData []ExpressionData, optimized *SQLExpression) *SQLExpression {
	if len(args) == 2 && len(argData) == 2 {
		if op, found := functionToOperator[name]; found && isDuckDBComparisonOperator(op) {
			l, r := duckDBApplyTemporalComparisonCoercionWithExprData(args[0], args[1], argData[0], argData[1], op)
			return NewBinaryExpression(l, op, r)
		}
	}
	return duckDBCoerceTemporalComparisons(optimized)
}

// duckDBCoerceTemporalComparisons walks AND/OR/NOT and comparison nodes so DuckDB sees
// TRY_CAST on VARCHAR operands paired with CAST(... AS DATE/TIMESTAMP/...).
func duckDBCoerceTemporalComparisons(e *SQLExpression) *SQLExpression {
	if e == nil {
		return nil
	}
	switch e.Type {
	case ExpressionTypeBinary:
		if e.BinaryExpression == nil {
			return e
		}
		be := e.BinaryExpression
		switch be.Operator {
		case "AND", "OR":
			return NewBinaryExpression(
				duckDBCoerceTemporalComparisons(be.Left),
				be.Operator,
				duckDBCoerceTemporalComparisons(be.Right),
			)
		default:
			if isDuckDBComparisonOperator(be.Operator) {
				l := duckDBCoerceTemporalComparisons(be.Left)
				r := duckDBCoerceTemporalComparisons(be.Right)
				l, r = duckDBApplyTemporalComparisonCoercion(l, r, be.Operator, nil, nil)
				return NewBinaryExpression(l, be.Operator, r)
			}
			return NewBinaryExpression(
				duckDBCoerceTemporalComparisons(be.Left),
				be.Operator,
				duckDBCoerceTemporalComparisons(be.Right),
			)
		}
	case ExpressionTypeUnary:
		if e.UnaryExpression == nil {
			return e
		}
		ue := *e.UnaryExpression
		ue.Expression = duckDBCoerceTemporalComparisons(e.UnaryExpression.Expression)
		return &SQLExpression{Type: ExpressionTypeUnary, UnaryExpression: &ue}
	default:
		return e
	}
}

// duckDBOptimizeComparatorOrLogical maps googlesqlite_* helpers to native SQL operators for DuckDB
// without requiring primitive-only arguments (see canOptimizeFunction).
func duckDBOptimizeComparatorOrLogical(name string, args []*SQLExpression, argData []ExpressionData) (*SQLExpression, bool) {
	switch name {
	case "googlesqlite_and", "googlesqlite_or":
		if len(args) < 2 {
			return nil, false
		}
		e, err := optimizeFunctionToSQL(name, args)
		if err != nil {
			return nil, false
		}
		return duckDBCoerceTemporalComparisons(e), true
	case "googlesqlite_not":
		if len(args) != 1 {
			return nil, false
		}
		e, err := optimizeFunctionToSQL(name, args)
		if err != nil {
			return nil, false
		}
		return duckDBCoerceTemporalComparisons(e), true
	case "googlesqlite_in":
		if len(args) < 2 {
			return nil, false
		}
		e, err := optimizeFunctionToSQL(name, args)
		if err != nil {
			return nil, false
		}
		return duckDBCoerceTemporalComparisons(e), true
	default:
		if _, found := functionToOperator[name]; !found {
			return nil, false
		}
		if len(args) != 2 {
			return nil, false
		}
		op := functionToOperator[name]
		if isDuckDBComparisonOperator(op) && len(argData) == 2 {
			l, r := duckDBApplyTemporalComparisonCoercionWithExprData(args[0], args[1], argData[0], argData[1], op)
			return duckDBCoerceTemporalComparisons(NewBinaryExpression(l, op, r)), true
		}
		e, err := optimizeFunctionToSQL(name, args)
		if err != nil {
			return nil, false
		}
		return duckDBCoerceTemporalComparisons(e), true
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

// duckDBUnwireGooglesqlStringOperand mirrors decodeStringOrLayout for a single SQL expression.
// VARCHAR columns often store googlesqlite base64+JSON wire; use for CONCAT operands and for
// simple-CASE equality so table values match plain WHEN literals (UNNEST literals are already plain).
func duckDBUnwireGooglesqlStringOperand(arg *SQLExpression) *SQLExpression {
	raw := NewSQLCastExpression(arg, "VARCHAR", false)
	// Trim only for base64 decode: coalesce fallback must use raw (e.g. CONCAT(a, ' ', b) — TRIM(' ') is '').
	trimmed := NewFunctionExpression("trim", raw)
	tryB64 := NewFunctionExpression("try", NewFunctionExpression("from_base64", trimmed))
	// DuckDB: from_base64 -> BLOB; decode(blob) -> UTF-8 VARCHAR (convert_from is not always available).
	utf8raw := NewFunctionExpression("decode", tryB64)
	utf8 := NewFunctionExpression("try", utf8raw)
	j := NewSQLCastExpression(utf8, "JSON", true)
	header := NewFunctionExpression("try", NewFunctionExpression("lower", NewFunctionExpression("json_extract_string", j, NewLiteralExpression(`'$.header'`))))
	body := NewFunctionExpression("try", NewFunctionExpression("json_extract_string", j, NewLiteralExpression(`'$.body'`)))
	// DATE/DATETIME/TIME use distinct headers (see codec.go ValueLayout); only "string" was handled
	// here, so TRY_CAST on DATE columns saw raw base64 and returned NULL for join predicates.
	decoded := NewCaseExpression([]*WhenClause{
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'string'`)), Result: body},
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'date'`)), Result: body},
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'datetime'`)), Result: body},
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'time'`)), Result: body},
	}, NewLiteralExpression("NULL"))
	pick := NewFunctionExpression("try", decoded)
	return NewFunctionExpression("coalesce", pick, raw)
}

func duckDBSimpleCaseExprToSearchedCaseForWireStrings(valueExpr *SQLExpression, whenClauses []*WhenClause, elseExpr *SQLExpression) *SQLExpression {
	left := duckDBUnwireGooglesqlStringOperand(valueExpr)
	searched := make([]*WhenClause, len(whenClauses))
	for i, w := range whenClauses {
		right := duckDBUnwireGooglesqlStringOperand(w.Condition)
		searched[i] = &WhenClause{
			Condition: NewBinaryExpression(left, "=", right),
			Result:    w.Result,
		}
	}
	return NewCaseExpression(searched, elseExpr)
}

// duckDBRewriteFunctionCall returns a DuckDB-native expression for special cases that are not
// covered by RewriteEmittedFunctionName alone (extra args, frozen clock, or arity-specific INSTR).
func duckDBRewriteFunctionCall(name string, args []*SQLExpression, argData []ExpressionData, d Dialect) (*SQLExpression, bool) {
	if d == nil || d.ID() != "duckdb" {
		return nil, false
	}
	switch name {
	case "googlesqlite_concat":
		if len(args) < 1 {
			return nil, false
		}
		unwrapped := make([]*SQLExpression, len(args))
		for i, a := range args {
			unwrapped[i] = duckDBUnwireGooglesqlStringOperand(a)
		}
		return NewFunctionExpression("concat", unwrapped...), true
	case "googlesqlite_equal":
		if len(args) == 2 {
			if len(argData) == 2 {
				l, r := duckDBApplyTemporalComparisonCoercionWithExprData(args[0], args[1], argData[0], argData[1], "=")
				return duckDBCoerceTemporalComparisons(NewBinaryExpression(l, "=", r)), true
			}
			return duckDBCoerceTemporalComparisons(NewBinaryExpression(args[0], "=", args[1])), true
		}
	case "googlesqlite_is_not_distinct_from":
		if len(args) == 2 {
			op := "IS NOT DISTINCT FROM"
			if len(argData) == 2 {
				l, r := duckDBApplyTemporalComparisonCoercionWithExprData(args[0], args[1], argData[0], argData[1], op)
				return duckDBCoerceTemporalComparisons(NewBinaryExpression(l, op, r)), true
			}
			return duckDBCoerceTemporalComparisons(NewBinaryExpression(args[0], op, args[1])), true
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
			if len(argData) >= 1 {
				srcExpr = duckDBWrapExprForDuckDBDatePart(srcExpr, argData[0], duckPart)
			}
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
			var ge, le *SQLExpression
			if len(argData) == 3 {
				gL, gR := duckDBApplyTemporalComparisonCoercionWithExprData(args[0], args[1], argData[0], argData[1], ">=")
				ge = duckDBCoerceTemporalComparisons(NewBinaryExpression(gL, ">=", gR))
				lL, lR := duckDBApplyTemporalComparisonCoercionWithExprData(args[0], args[2], argData[0], argData[2], "<=")
				le = duckDBCoerceTemporalComparisons(NewBinaryExpression(lL, "<=", lR))
			} else {
				ge = duckDBCoerceTemporalComparisons(NewBinaryExpression(args[0], ">=", args[1]))
				le = duckDBCoerceTemporalComparisons(NewBinaryExpression(args[0], "<=", args[2]))
			}
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
	case "googlesqlite_generate_array":
		if out, ok := duckDBRewriteGenerateArrayToRange(args); ok {
			return out, true
		}
	}
	return nil, false
}

// duckDBRewriteGenerateArrayToRange maps BigQuery GENERATE_ARRAY (inclusive endpoints) to DuckDB
// range(start, stop_exclusive, step). SQLite registers googlesqlite_generate_array; DuckDB has no
// such UDF, so this rewrite is required for native execution.
func duckDBRewriteGenerateArrayToRange(args []*SQLExpression) (*SQLExpression, bool) {
	if len(args) != 2 && len(args) != 3 {
		return nil, false
	}
	start, end := args[0], args[1]
	var step *SQLExpression
	if len(args) == 3 {
		step = args[2]
	} else {
		step = NewLiteralExpression("1")
	}
	zero := NewLiteralExpression("0")
	one := NewLiteralExpression("1")
	stepNonZero := NewBinaryExpression(step, "<>", zero)
	posRun := NewBinaryExpression(
		NewBinaryExpression(step, ">", zero),
		"AND",
		NewBinaryExpression(start, "<=", end),
	)
	negRun := NewBinaryExpression(
		NewBinaryExpression(step, "<", zero),
		"AND",
		NewBinaryExpression(start, ">=", end),
	)
	validDir := NewBinaryExpression(posRun, "OR", negRun)
	valid := NewBinaryExpression(stepNonZero, "AND", validDir)

	diff := NewBinaryExpression(end, "-", start)
	quot := NewBinaryExpression(diff, "/", step)
	plusOne := NewBinaryExpression(quot, "+", one)
	inc := NewBinaryExpression(plusOne, "*", step)
	stopExcl := NewBinaryExpression(start, "+", inc)
	// DuckDB only exposes range(BIGINT, BIGINT, BIGINT); date_part and mixed
	// arithmetic otherwise produce DOUBLE and fail binding.
	castI64 := func(e *SQLExpression) *SQLExpression {
		return NewSQLCastExpression(e, "BIGINT", false)
	}
	rng := NewFunctionExpression("range", castI64(start), castI64(stopExcl), castI64(step))

	empty := NewLiteralExpression("CAST([] AS BIGINT[])")
	return NewCaseExpression([]*WhenClause{{Condition: valid, Result: rng}}, empty), true
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
