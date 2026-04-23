package internal

// ParameterTransformer handles transformation of parameters/argument identifiers from GoogleSQL to SQLite.
//
// In BigQuery/GoogleSQL, parameters represent named or positional placeholders in SQL queries
// that are substituted with actual values at execution time. These can be query parameters
// like @param_name (named) or ? (positional) that allow dynamic query execution.
//
// The transformer converts GoogleSQL Parameter nodes by:
// - Extracting the parameter identifier (name or position)
// - Creating a literal SQLite expression with the identifier
// - Preserving the parameter reference for runtime substitution
//
// This is the simplest transformer as it performs direct identifier mapping without
// complex transformation logic, but it's essential for parameterized query support.
type ParameterTransformer struct{}

func NewParameterTransformer() *ParameterTransformer {
	return &ParameterTransformer{}
}

// Transform converts ParameterData to SQLExpression
func (t *ParameterTransformer) Transform(data ExpressionData, ctx TransformContext) (*SQLExpression, error) {
	return &SQLExpression{
		Type:  ExpressionTypeParameter,
		Value: data.Parameter.Identifier,
	}, nil
}

// FormatParameterPlaceholder renders a bound-parameter token for the physical SQL dialect.
func FormatParameterPlaceholder(d Dialect, identifier string) string {
	if d == nil {
		return identifier
	}
	return d.FormatBoundParameter(identifier)
}
