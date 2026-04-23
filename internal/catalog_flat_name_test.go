package internal

import (
	"testing"
)

func TestTableFlatNamesEquivalent(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"my-project_foo_bar_bux", "my_project_foo_bar_bux", true},
		{"my-project_foo_bar_bux", "my-project_foo_bar_bux", true},
		{"a_b_c", "a-b-c", true},
		{"proj_ds_tbl", "proj_ds_tbl", true},
		{"proj_ds_tbl", "other_ds_tbl", false},
	}
	for _, tt := range tests {
		if got := tableFlatNamesEquivalent(tt.a, tt.b); got != tt.want {
			t.Fatalf("tableFlatNamesEquivalent(%q,%q) = %v want %v", tt.a, tt.b, got, tt.want)
		}
	}
}
