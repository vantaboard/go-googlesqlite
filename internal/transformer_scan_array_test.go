package internal

import (
	"testing"

	"github.com/vantaboard/go-googlesql/types"
)

func TestDuckDBUnnestElementExprForStructPayload_mergesArrayElementType(t *testing.T) {
	st, err := types.NewStructType([]*types.StructField{
		types.NewStructField("a", types.Int64Type()),
	})
	if err != nil {
		t.Fatal(err)
	}
	el := &ExpressionData{
		Type: ExpressionTypeColumn,
		Column: &ColumnRefData{
			ColumnName: "y",
			ColumnID:   42,
			Type:       types.StringType(),
		},
	}
	merged := duckDBUnnestElementExprForStructPayload(el, st)
	if merged == nil {
		t.Fatal("expected merged")
	}
	got := duckDBStructTypeFromExpressionData(*merged)
	if got == nil || !got.IsStruct() {
		t.Fatalf("expected struct type, got %v", got)
	}
	// original unchanged
	if duckDBStructTypeFromExpressionData(*el) != nil {
		t.Fatalf("source column should stay string-typed for this test")
	}
}

func TestDuckDBUnnestElementExprForStructPayload_noOpWhenElementAlreadyStruct(t *testing.T) {
	st, err := types.NewStructType([]*types.StructField{
		types.NewStructField("a", types.Int64Type()),
	})
	if err != nil {
		t.Fatal(err)
	}
	el := &ExpressionData{
		Type: ExpressionTypeColumn,
		Column: &ColumnRefData{
			ColumnName: "y",
			ColumnID:   1,
			Type:       st,
		},
	}
	otherSt, err := types.NewStructType([]*types.StructField{
		types.NewStructField("b", types.StringType()),
	})
	if err != nil {
		t.Fatal(err)
	}
	merged := duckDBUnnestElementExprForStructPayload(el, otherSt)
	if merged != el {
		t.Fatalf("expected same pointer when element already struct-typed, got %p vs %p", merged, el)
	}
}
