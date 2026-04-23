package duckdb

// aggregateRenames maps googlesqlengine_* aggregate names (GROUP BY path) to DuckDB builtins.
var aggregateRenames = map[string]string{
	"googlesqlengine_any_value":   "any_value",
	"googlesqlengine_array_agg":   "array_agg",
	"googlesqlengine_avg":         "avg",
	"googlesqlengine_count":       "count",
	"googlesqlengine_count_star":  "count",
	"googlesqlengine_countif":     "count_if",
	"googlesqlengine_logical_and": "bool_and",
	"googlesqlengine_logical_or":  "bool_or",
	"googlesqlengine_max":         "max",
	"googlesqlengine_min":         "min",
	"googlesqlengine_string_agg":  "string_agg",
	"googlesqlengine_sum":         "sum",
	"googlesqlengine_bit_and":     "bit_and",
	"googlesqlengine_bit_or":      "bit_or",
	"googlesqlengine_bit_xor":     "bit_xor",
	"googlesqlengine_corr":        "corr",
	"googlesqlengine_covar_pop":   "covar_pop",
	"googlesqlengine_covar_samp":  "covar_samp",
	"googlesqlengine_stddev_pop":  "stddev_pop",
	"googlesqlengine_stddev_samp": "stddev_samp",
	"googlesqlengine_stddev":      "stddev_samp",
	"googlesqlengine_var_pop":     "var_pop",
	"googlesqlengine_var_samp":    "var_samp",
	"googlesqlengine_variance":    "var_samp",
}

// windowRenames maps googlesqlengine_window_* analytic names to DuckDB window functions.
var windowRenames map[string]string

func init() {
	windowRenames = make(map[string]string, 64)
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
		windowRenames["googlesqlengine_window_"+p.googlesql] = p.duck
	}
}
