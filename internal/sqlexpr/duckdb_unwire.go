package sqlexpr

// duckDBGooglesqlWireAsJSON decodes a VARCHAR googlesqlengine wire column to a JSON value expression.
// Cells may be base64(JSON layout) or plain JSON text. Do not use coalesce(b64, plain): DuckDB may still
// evaluate both branches when optimizing joins, and TRY_CAST(trimmed AS JSON) on base64 text ('eyJ...')
// throws Conversion Error even inside try() in some builds. Branch explicitly: only run the plain-text JSON
// path when from_base64 did not yield a payload.
func duckDBGooglesqlWireAsJSON(arg *SQLExpression) *SQLExpression {
	raw := NewSQLCastExpression(arg, "VARCHAR", false)
	trimmed := NewFunctionExpression("trim", raw)
	b64Payload := NewFunctionExpression("try", NewFunctionExpression("from_base64", trimmed))
	decodedStr := NewFunctionExpression("try", NewFunctionExpression("decode", b64Payload))
	jsonFromDecoded := NewFunctionExpression("try", NewSQLCastExpression(decodedStr, "JSON", true))
	jsonFromTrim := NewFunctionExpression("try", NewSQLCastExpression(trimmed, "JSON", true))
	isB64 := NewBinaryExpression(b64Payload, "IS NOT", NewLiteralExpression("NULL"))
	return NewCaseExpression([]*WhenClause{
		{Condition: isB64, Result: jsonFromDecoded},
	}, jsonFromTrim)
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
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'int64'`)), Result: body},
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
