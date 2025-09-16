package internal

import (
	"fmt"
)

// WithRefScanTransformer handles WITH reference scan transformations from ZetaSQL to SQLite.
//
// In BigQuery/ZetaSQL, a WithRefScan represents a reference to a previously defined
// Common Table Expression (CTE) within a WITH clause. This allows queries to reference
// named temporary result sets by name, following SQL's lexical scoping rules.
//
// The transformer converts ZetaSQL WithRefScan nodes by:
// - Creating a table reference to the CTE by its name
// - Retrieving stored column mappings from the transform context
// - Building a SELECT statement that maps CTE columns to output columns with proper aliases
// - Ensuring column names match the CTE definition through mapping resolution
//
// The fragment context maintains the mapping between CTE names and their column
// definitions, enabling proper name resolution when the CTE is referenced.
type WithRefScanTransformer struct {
	coordinator Coordinator
}

// NewWithRefScanTransformer creates a new WITH reference scan transformer
func NewWithRefScanTransformer(coordinator Coordinator) *WithRefScanTransformer {
	return &WithRefScanTransformer{
		coordinator: coordinator,
	}
}

// Transform converts WithRefScanData to FromItem that references a CTE
func (t *WithRefScanTransformer) Transform(data ScanData, ctx TransformContext) (*FromItem, error) {
	if data.Type != ScanTypeWithRef || data.WithRefScan == nil {
		return nil, fmt.Errorf("expected with ref scan data, got type %v", data.Type)
	}

	withRefScanData := data.WithRefScan

	// Create a SELECT statement that references the WITH query by name
	selectStatement := NewSelectStatement()
	selectStatement.FromClause = &FromItem{
		Type:      FromItemTypeTable,
		TableName: withRefScanData.WithQueryName,
	}
	selectStatement.SelectList = []*SelectListItem{}

	// Get stored column mappings from context for this WITH query
	mapping := ctx.GetWithEntryMapping(withRefScanData.WithQueryName)

	// Add SELECT items for each column
	for _, column := range withRefScanData.ColumnList {
		alias := generateIDBasedAlias(column.Name, column.ID)
		columnName := column.Name
		if mapping != nil {
			if mappedName, exists := mapping[columnName]; exists {
				columnName = mappedName
			}
		}

		selectStatement.SelectList = append(selectStatement.SelectList,
			&SelectListItem{
				Expression: NewColumnExpression(columnName, withRefScanData.WithQueryName),
				Alias:      alias,
			},
		)
	}

	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: selectStatement,
	}, nil
}
