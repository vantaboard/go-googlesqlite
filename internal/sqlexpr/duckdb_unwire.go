package sqlexpr

// duckDBRegexpExtractFirstGroup wraps DuckDB regexp_extract(string, pattern, 1).
func duckDBRegexpExtractFirstGroup(str *SQLExpression, pattern string) *SQLExpression {
	patLit := NewLiteralExpression(QuoteStringSingleQuoted(pattern))
	return NewFunctionExpression("regexp_extract", str, patLit, NewLiteralExpression("1"))
}

// duckDBWireGooglesqlUtf8Payload returns VARCHAR UTF-8 text of the JSON object inside googlesqlengine wire:
// either decode(base64(trim(cell))) or plain trim(cell) when it already looks like JSON ({ or [).
// If the cell is still base64 layout and from_base64 failed, yields NULL — never passes base64 text
// through as the payload (avoids implicit/explicit JSON casts on 'eyJ...' and Conversion Error).
func duckDBWireGooglesqlUtf8Payload(arg *SQLExpression) *SQLExpression {
	raw := NewSQLCastExpression(arg, "VARCHAR", false)
	trimmed := NewFunctionExpression("trim", raw)
	b64Payload := NewFunctionExpression("try", NewFunctionExpression("from_base64", trimmed))
	decodedUtf8 := NewFunctionExpression("try", NewFunctionExpression("decode", b64Payload))
	isB64 := NewBinaryExpression(b64Payload, "IS NOT", NewLiteralExpression("NULL"))

	firstChar := NewFunctionExpression("substring", trimmed, NewLiteralExpression("1"), NewLiteralExpression("1"))
	looksLikeJSONText := NewBinaryExpression(
		NewBinaryExpression(firstChar, "=", NewLiteralExpression(`'{'`)),
		"OR",
		NewBinaryExpression(firstChar, "=", NewLiteralExpression(`'['`)),
	)
	plainOrNull := NewCaseExpression([]*WhenClause{
		{Condition: looksLikeJSONText, Result: trimmed},
	}, NewLiteralExpression("NULL"))

	return NewCaseExpression([]*WhenClause{
		{Condition: isB64, Result: decodedUtf8},
	}, plainOrNull)
}

// DuckDBUnwireGooglesqlStringOperand mirrors decodeStringOrLayout for a single SQL expression.
// VARCHAR columns often store googlesqlengine base64+JSON wire; use for CONCAT operands and for
// simple-CASE equality so table values match plain WHEN literals (UNNEST literals are already plain).
//
// json_extract_string on VARCHAR still triggers JSON casting in DuckDB that can attribute
// Conversion Error to the source column; parse header/body with regexp_extract on the UTF-8 payload only.
func DuckDBUnwireGooglesqlStringOperand(arg *SQLExpression) *SQLExpression {
	raw := NewSQLCastExpression(arg, "VARCHAR", false)
	payload := duckDBWireGooglesqlUtf8Payload(arg)
	header := NewFunctionExpression("try", NewFunctionExpression("lower", duckDBRegexpExtractFirstGroup(payload, `"header"\s*:\s*"([^"]*)"`)))
	bodyQuoted := duckDBRegexpExtractFirstGroup(payload, `"body"\s*:\s*"([^"]*)"`)
	bodyNum := duckDBRegexpExtractFirstGroup(payload, `"body"\s*:\s*([-0-9]+)`)
	body := NewFunctionExpression("try", NewFunctionExpression("coalesce", bodyQuoted, bodyNum))
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
	payload := duckDBWireGooglesqlUtf8Payload(arg)
	header := NewFunctionExpression("try", NewFunctionExpression("lower", duckDBRegexpExtractFirstGroup(payload, `"header"\s*:\s*"([^"]*)"`)))
	// Capture a flat JSON array [...] after "body": — avoids json_extract_string JSON cast on VARCHAR.
	bodyStr := NewFunctionExpression("try", duckDBRegexpExtractFirstGroup(payload, `"body"\s*:\s*(\[[^\]]*\])`))
	asJSON := NewSQLCastExpression(bodyStr, "JSON", true)
	emptyArr := NewSQLCastExpression(NewLiteralExpression(`'[]'`), "JSON", false)
	return NewCaseExpression([]*WhenClause{
		{Condition: NewBinaryExpression(header, "=", NewLiteralExpression(`'array'`)), Result: asJSON},
	}, emptyArr)
}
