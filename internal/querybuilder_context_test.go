package internal

import (
	"testing"
)

func TestAddAvailableColumn_preservesExpressionWhenCoordinatorReAdds(t *testing.T) {
	fc := NewDefaultFragmentContext()
	elemID := 7
	gs := generateIDBasedAlias("gs", elemID)
	inner := NewColumnExpression(gs, "$array_1")
	fc.AddAvailableColumn(elemID, &ColumnInfo{
		ID:         elemID,
		Name:       gs,
		Expression: inner,
	})
	// Coordinator path: plain name/ID without Expression
	fc.AddAvailableColumn(elemID, &ColumnInfo{Name: "y", ID: elemID})
	got := fc.availableColumns[elemID]
	if got == nil || got.Expression == nil {
		t.Fatalf("expected preserved Expression, got %+v", got)
	}
	if got.Expression.Value != gs {
		t.Fatalf("inner value: got %q want %q", got.Expression.Value, gs)
	}
}

func TestRegisterColumnScopeMapping_mergesPublicAliasWithArrayScanExpression(t *testing.T) {
	fc := NewDefaultFragmentContext()
	elemID := 42
	gs := generateIDBasedAlias("gs", elemID)
	inner := NewColumnExpression(gs, "$array_9")
	fc.AddAvailableColumn(elemID, &ColumnInfo{
		ID:         elemID,
		Name:       gs,
		Expression: inner,
	})
	fc.RegisterColumnScopeMapping("array_scan_2", []*ColumnData{{Name: "y", ID: elemID}})
	info := fc.availableColumns[elemID]
	if info == nil || info.Expression == nil {
		t.Fatalf("expected merged ColumnInfo with Expression, got %+v", info)
	}
	wantName := generateIDBasedAlias("y", elemID)
	if info.Name != wantName {
		t.Fatalf("public name: got %q want %q", info.Name, wantName)
	}
	out := fc.GetQualifiedColumnExpression(elemID)
	if out == nil {
		t.Fatal("nil expression")
	}
	if out.TableAlias != "array_scan_2" {
		t.Fatalf("TableAlias: got %q want array_scan_2", out.TableAlias)
	}
	if out.Value != wantName {
		t.Fatalf("Value: got %q want %q (outer subquery output alias)", out.Value, wantName)
	}
}
