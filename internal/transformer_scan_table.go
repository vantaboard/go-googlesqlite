package internal

import (
	"fmt"
)

// TableScanTransformer handles table scan transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a TableScan represents the foundational scan operation that reads
// directly from a table. This is the base case in the recursive scan transformation tree -
// it has no input scans and corresponds to a table reference in the FROM clause.
//
// The transformer converts ZetaSQL TableScan nodes into SQLite table references with:
// - Direct table name mapping
// - Optional table aliasing for disambiguation
// - Proper FROM clause item generation
//
// This is the simplest transformer as it performs direct mapping without complex logic,
// but it's crucial as the leaf node in the scan transformation tree.
type TableScanTransformer struct {
}

// NewTableScanTransformer creates a new table scan transformer
func NewTableScanTransformer() *TableScanTransformer {
	return &TableScanTransformer{}
}

// Transform converts TableScanData to FromItem
func (t *TableScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeTable || data.TableScan == nil {
		return nil, fmt.Errorf("expected table scan data, got type %v", data.Type)
	}

	tableData := data.TableScan

	// This is the base case - no inner scan to transform recursively
	return &FromItem{
		Type:      FromItemTypeTable,
		TableName: tableData.TableName,
		Alias:     tableData.Alias,
	}, nil
}
