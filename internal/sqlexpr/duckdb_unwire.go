package sqlexpr

// DuckDBUnwireGooglesqlStringOperand mirrors decodeStringOrLayout for a single SQL expression.
// VARCHAR columns often store googlesqlengine base64+JSON wire; use for CONCAT operands and for
// simple-CASE equality so table values match plain WHEN literals (UNNEST literals are already plain).
func DuckDBUnwireGooglesqlStringOperand(arg *SQLExpression) *SQLExpression {
	raw := NewSQLCastExpression(arg, "VARCHAR", false)
	trimmed := NewFunctionExpression("trim", raw)
	tryB64 := NewFunctionExpression("try", NewFunctionExpression("from_base64", trimmed))
	utf8raw := NewFunctionExpression("decode", tryB64)
	utf8 := NewFunctionExpression("try", utf8raw)
	j := NewSQLCastExpression(utf8, "JSON", true)
	header := NewFunctionExpression("try", NewFunctionExpression("lower", NewFunctionExpression("json_extract_string", j, NewLiteralExpression(`'$.header'`))))
	body := NewFunctionExpression("try", NewFunctionExpression("json_extract_string", j, NewLiteralExpression(`'$.body'`)))
	decoded := NewCaseExpression([]*WhenClause{
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'string'`)), Result: body},
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'date'`)), Result: body},
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'datetime'`)), Result: body},
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'time'`)), Result: body},
	}, NewLiteralExpression("NULL"))
	pick := NewFunctionExpression("try", decoded)
	return NewFunctionExpression("coalesce", pick, raw)
}
