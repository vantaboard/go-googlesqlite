package internal

// duckDBAggregateRenames maps googlesqlite_* aggregate names (GROUP BY path) to DuckDB builtins.
var duckDBAggregateRenames = map[string]string{
	"googlesqlite_any_value":   "any_value",
	"googlesqlite_array_agg":   "array_agg",
	"googlesqlite_avg":         "avg",
	"googlesqlite_count":       "count",
	"googlesqlite_count_star":  "count",
	"googlesqlite_countif":     "count_if",
	"googlesqlite_logical_and": "bool_and",
	"googlesqlite_logical_or":  "bool_or",
	"googlesqlite_max":         "max",
	"googlesqlite_min":         "min",
	"googlesqlite_string_agg":  "string_agg",
	"googlesqlite_sum":         "sum",
	"googlesqlite_bit_and":     "bit_and",
	"googlesqlite_bit_or":      "bit_or",
	"googlesqlite_bit_xor":     "bit_xor",
	"googlesqlite_corr":        "corr",
	"googlesqlite_covar_pop":   "covar_pop",
	"googlesqlite_covar_samp":  "covar_samp",
	"googlesqlite_stddev_pop":  "stddev_pop",
	"googlesqlite_stddev_samp": "stddev_samp",
	"googlesqlite_stddev":      "stddev_samp",
	"googlesqlite_var_pop":     "var_pop",
	"googlesqlite_var_samp":    "var_samp",
	"googlesqlite_variance":    "var_samp",
}

// duckDBWindowRenames maps googlesqlite_window_* analytic names to DuckDB window functions.
var duckDBWindowRenames map[string]string

func init() {
	duckDBWindowRenames = make(map[string]string, 64)
	// Names match [function_register.go] windowFuncs entries.
	base := []struct{ googlesql, duck string }{
		{"any_value", "any_value"},
		{"array_agg", "array_agg"},
		{"avg", "avg"},
		{"count", "count"},
		{"count_star", "count"},
		{"countif", "count_if"},
		{"logical_and", "bool_and"},
		{"logical_or", "bool_or"},
		{"max", "max"},
		{"min", "min"},
		{"string_agg", "string_agg"},
		{"sum", "sum"},
		{"corr", "corr"},
		{"covar_pop", "covar_pop"},
		{"covar_samp", "covar_samp"},
		{"stddev_pop", "stddev_pop"},
		{"stddev_samp", "stddev_samp"},
		{"stddev", "stddev_samp"},
		{"var_pop", "var_pop"},
		{"var_samp", "var_samp"},
		{"variance", "var_samp"},
		{"first_value", "first_value"},
		{"last_value", "last_value"},
		{"nth_value", "nth_value"},
		{"lead", "lead"},
		{"lag", "lag"},
		{"percentile_cont", "quantile_cont"},
		{"percentile_disc", "quantile_disc"},
		{"rank", "rank"},
		{"dense_rank", "dense_rank"},
		{"percent_rank", "percent_rank"},
		{"cume_dist", "cume_dist"},
		{"ntile", "ntile"},
		{"row_number", "row_number"},
	}
	for _, p := range base {
		duckDBWindowRenames["googlesqlite_window_"+p.googlesql] = p.duck
	}
}
