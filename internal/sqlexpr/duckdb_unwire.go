package sqlexpr

// duckDBGooglesqlWireAsJSON decodes a VARCHAR googlesqlengine wire column to a JSON value expression.
func duckDBGooglesqlWireAsJSON(arg *SQLExpression) *SQLExpression {
	raw := NewSQLCastExpression(arg, "VARCHAR", false)
	trimmed := NewFunctionExpression("trim", raw)
	tryB64 := NewFunctionExpression("try", NewFunctionExpression("from_base64", trimmed))
	utf8raw := NewFunctionExpression("decode", tryB64)
	utf8 := NewFunctionExpression("try", utf8raw)
	return NewSQLCastExpression(utf8, "JSON", true)
}

// DuckDBUnwireGooglesqlStringOperand mirrors decodeStringOrLayout for a single SQL expression.
// VARCHAR columns often store googlesqlengine base64+JSON wire; use for CONCAT operands and for
// simple-CASE equality so table values match plain WHEN literals (UNNEST literals are already plain).
func DuckDBUnwireGooglesqlStringOperand(arg *SQLExpression) *SQLExpression {
	raw := NewSQLCastExpression(arg, "VARCHAR", false)
	j := duckDBGooglesqlWireAsJSON(arg)
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

// DuckDBGooglesqlWireArrayJSONForJsonEach builds the JSON payload for DuckDB json_each(...) from a
// VARCHAR wire column. ARRAY columns are stored as VARCHAR on DuckDB (see DuckDBDialect.PhysicalColumnStorageType);
// UNNEST(list) requires a native LIST; json_each accepts the same JSON array text SQLite's
// googlesqlengine_decode_array produces (ValueLayout header=array, $.body is the JSON array).
func DuckDBGooglesqlWireArrayJSONForJsonEach(arg *SQLExpression) *SQLExpression {
	j := duckDBGooglesqlWireAsJSON(arg)
	header := NewFunctionExpression("try", NewFunctionExpression("lower", NewFunctionExpression("json_extract_string", j, NewLiteralExpression(`'$.header'`))))
	bodyStr := NewFunctionExpression("try", NewFunctionExpression("json_extract_string", j, NewLiteralExpression(`'$.body'`)))
	asJSON := NewSQLCastExpression(bodyStr, "JSON", true)
	emptyArr := NewSQLCastExpression(NewLiteralExpression(`'[]'`), "JSON", false)
	return NewCaseExpression([]*WhenClause{
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'array'`)), Result: asJSON},
	}, emptyArr)
}
