package internal

import (
	"strings"
	"testing"
)

func TestMergeDuplicateSourceCheckSQL(t *testing.T) {
	sql := mergeDuplicateSourceCheckSQL([]mergeKeyPair{
		{mergedSourceName: "merged_source_id", mergedTargetName: "merged_target_id"},
	}, "googlesqlite_merged_table", SQLiteDialect{})
	if !strings.Contains(sql, "GROUP BY `merged_target_id`") || !strings.Contains(sql, "HAVING COUNT(*) > 1") {
		t.Fatalf("unexpected SQL: %s", sql)
	}
}

func TestIsMergeExprConstantFalse(t *testing.T) {
	if !isMergeExprConstantFalse(ExpressionData{
		Type: ExpressionTypeLiteral,
		Literal: &LiteralData{
			Value: BoolValue(false),
		},
	}) {
		t.Fatal("expected false literal")
	}
	if isMergeExprConstantFalse(ExpressionData{
		Type: ExpressionTypeLiteral,
		Literal: &LiteralData{
			Value: BoolValue(true),
		},
	}) {
		t.Fatal("did not expect true literal")
	}
}
