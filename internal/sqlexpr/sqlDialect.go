package sqlexpr

import "strings"

// SQLDialect is the minimal surface needed to render SQL for a physical backend.
type SQLDialect interface {
	ID() string
	QuoteIdent(ident string) string
	FormatParameterPlaceholder(identifier string) string
	EmitWireLiteral(w *SQLWriter, wire string)
	NormalizeAggregate(f *FunctionCall) (name string, args []*SQLExpression, distinct bool, countStar bool, aggFilter *SQLExpression)
}

// NilSQLDialect is the SQLite-style default when no dialect is configured.
type NilSQLDialect struct{}

func (NilSQLDialect) ID() string { return "sqlite" }

func (NilSQLDialect) QuoteIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

func (NilSQLDialect) FormatParameterPlaceholder(identifier string) string { return identifier }

func (NilSQLDialect) EmitWireLiteral(w *SQLWriter, wire string) { w.Write(wire) }

func (NilSQLDialect) NormalizeAggregate(f *FunctionCall) (string, []*SQLExpression, bool, bool, *SQLExpression) {
	if f == nil {
		return "", nil, false, false, nil
	}
	return f.Name, f.Arguments, f.IsDistinct, false, nil
}

// QuoteStringSingleQuoted returns a single-quoted SQL string literal.
func QuoteStringSingleQuoted(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
