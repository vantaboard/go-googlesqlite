package internal

import (
	"fmt"
	"github.com/goccy/go-zetasql/types"
	"reflect"
	"strings"

	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// ColumnTableMapping represents the mapping between column IDs and their table information
type ColumnTableMapping struct {
	ColumnID    string // e.g., "Items.ItemId#2"
	ColumnName  string // e.g., "ItemId" - added for convenience
	TableName   string // e.g., "Items"
	TableAlias  string // e.g., "d" (empty if no alias)
	ColumnIndex int    // Index in the column_index_list
}

func (m ColumnTableMapping) TableString() string {
	if m.TableAlias != "" {
		return m.TableAlias
	}
	return m.TableName
}

func (i ScanNodeInfo) TableString() string {
	if i.TableAlias != "" {
		return i.TableAlias
	}
	return i.TableName
}

// String provides a readable representation of the column mapping
func (m ColumnTableMapping) String() string {
	table := m.TableString()
	if table != "" {
		return fmt.Sprintf("%s.%s", table, m.ColumnName)
	}
	return m.ColumnName
}

// ScanNodeInfo contains information extracted from a ScanNode
type ScanNodeInfo struct {
	NodeType   string               // Type of scan node (e.g., "TableScan")
	TableName  string               // Table name (inferred from columns if not directly available)
	TableAlias string               // Table alias (if any)
	Columns    []ColumnTableMapping // Column mappings
	Depth      int                  // Depth in the AST tree
}

// Interface definitions for checking node capabilities
type AliasProvider interface {
	Alias() string
}

type TableProvider interface {
	Table() types.Table
}

type ColumnListProvider interface {
	ColumnList() []*ast.Column
}
type OutputColumnListProvider interface {
	OutputColumnList() []*ast.OutputColumnNode
}

type ColumnIndexListProvider interface {
	ColumnIndexList() []int
}

type WithQueryNameProvider interface {
	WithQueryName() string
}

// ScanResult holds the complete result of scanning the AST
type ScanResult struct {
	ColumnMappings map[string]ColumnTableMapping
	ScanNodes      []ScanNodeInfo
	Stats          ScanStats
}

// ScanStats provides statistics about the scan operation
type ScanStats struct {
	TotalNodes  int
	ScanNodes   int
	TablesFound []string
	ColumnCount int
	MaxDepth    int
}

// ExtractScanNodeMappings walks the ZetaSQL AST and extracts column-to-table mappings from all ScanNodes
func ExtractScanNodeMappings(resolvedStmt ast.StatementNode) (*ScanResult, error) {
	// Walk the AST and collect scan nodes
	columnMappings := make(map[string]ColumnTableMapping)
	var scanNodes []ScanNodeInfo
	stats := ScanStats{
		TablesFound: make([]string, 0),
	}

	err := walkAST(resolvedStmt, &columnMappings, &scanNodes, &stats, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to walk AST: %w", err)
	}

	// Remove duplicate table names
	stats.TablesFound = removeDuplicateStrings(stats.TablesFound)
	stats.ScanNodes = len(scanNodes)
	stats.ColumnCount = len(columnMappings)

	return &ScanResult{
		ColumnMappings: columnMappings,
		ScanNodes:      scanNodes,
		Stats:          stats,
	}, nil
}

// walkAST recursively walks the resolved AST node and extracts scan node information
func walkAST(node ast.Node, columnMappings *map[string]ColumnTableMapping, scanNodes *[]ScanNodeInfo,
	stats *ScanStats, depth int) error {

	if node == nil {
		return nil
	}

	stats.TotalNodes++
	if depth > stats.MaxDepth {
		stats.MaxDepth = depth
	}

	// Check if this is a scan node
	if scanInfo := extractScanNodeInfo(node, depth); scanInfo != nil {
		*scanNodes = append(*scanNodes, *scanInfo)

		// Add column mappings to the global map
		for _, col := range scanInfo.Columns {
			(*columnMappings)[col.ColumnID] = col
		}

		// Track unique table names
		if scanInfo.TableName != "" {
			stats.TablesFound = append(stats.TablesFound, scanInfo.TableName)
		}
	}

	// Recursively walk child nodes
	children := node.ChildNodes()
	for _, child := range children {
		if err := walkAST(child, columnMappings, scanNodes, stats, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// IsScanNode checks if a node is a scan node using the built-in IsScan() method
func IsScanNode(node ast.Node) bool {
	if node == nil {
		return false
	}

	// Use the built-in IsScan() method from ast.Node
	return node.IsScan()
}

// GetScanNodeInfo dynamically extracts scan node information from any node
func GetScanNodeInfo(node ast.Node) (*ScanNodeInfo, bool) {
	//if !IsScanNode(node) {
	//	return nil, false
	//}

	scanInfo := extractScanNodeInfo(node, 0) // depth 0 for dynamic calls
	if scanInfo == nil {
		return nil, false
	}

	return scanInfo, true
}

// extractScanNodeInfo extracts information from any scan node using interface checks
func extractScanNodeInfo(node ast.Node, depth int) *ScanNodeInfo {
	// Check if this is a scan node
	//if !IsScanNode(node) {
	//	return nil
	//}

	// Create the scan node info
	info := &ScanNodeInfo{
		NodeType: getNodeTypeName(node),
		Depth:    depth,
	}

	// Extract alias if available
	if aliasProvider, ok := node.(AliasProvider); ok {
		info.TableAlias = aliasProvider.Alias()
	}

	// Extract direct table name if available
	if tableProvider, ok := node.(TableProvider); ok {
		if table := tableProvider.Table(); table != nil {
			info.TableName = table.Name()
		}
	}

	// Extract WITH query name if available (for WithRefScan)
	if withProvider, ok := node.(WithQueryNameProvider); ok {
		info.TableName = withProvider.WithQueryName()
	}

	// Extract column index list if available
	var columnIndexList []int
	if indexProvider, ok := node.(ColumnIndexListProvider); ok {
		columnIndexList = indexProvider.ColumnIndexList()
	}

	// Extract column mappings (must have ColumnList if it's a scan node)
	if columnListProvider, ok := node.(ColumnListProvider); ok {
		columnList := columnListProvider.ColumnList()
		info.Columns = extractColumnMappings(columnList, info.TableName, info.TableAlias, columnIndexList)

		// If we don't have a table name from direct sources, try to infer it from columns
		if info.TableName == "" {
			info.TableName = inferTableNameFromColumns(columnList)
		}
	}

	// Extract column mappings (must have OutputColumnList if it's a query)
	if columnListProvider, ok := node.(OutputColumnListProvider); ok {
		outputColumnList := columnListProvider.OutputColumnList()
		columnList := make([]*ast.Column, len(outputColumnList))
		for i, col := range outputColumnList {
			columnList[i] = col.Column()
		}
		info.Columns = extractColumnMappings(columnList, info.TableName, info.TableAlias, columnIndexList)

		// If we don't have a table name from direct sources, try to infer it from columns
		if info.TableName == "" {
			info.TableName = inferTableNameFromColumns(columnList)
		}
	}

	return info
}

// getNodeTypeName extracts the node type name using reflection
func getNodeTypeName(node ast.Node) string {
	nodeType := reflect.TypeOf(node)
	if nodeType.Kind() == reflect.Ptr {
		nodeType = nodeType.Elem()
	}

	typeName := nodeType.Name()

	// Remove "Resolved" prefix if present
	if strings.HasPrefix(typeName, "Resolved") {
		typeName = strings.TrimPrefix(typeName, "Resolved")
	}

	return typeName
}

// inferTableNameFromColumns attempts to infer the table name from the column list
func inferTableNameFromColumns(columnList []*ast.Column) string {
	if len(columnList) == 0 {
		return ""
	}

	// Group columns by table name to find the most common one
	tableNames := make(map[string]int)
	for _, col := range columnList {
		if col != nil && col.TableName() != "" {
			tableNames[col.TableName()]++
		}
	}

	// Return the most frequent table name
	var mostFrequentTable string
	maxCount := 0
	for tableName, count := range tableNames {
		if count > maxCount {
			maxCount = count
			mostFrequentTable = tableName
		}
	}

	return mostFrequentTable
}

// extractColumnMappings extracts column mappings from a column list
func extractColumnMappings(columnList []*ast.Column, defaultTableName, tableAlias string, columnIndexList []int) []ColumnTableMapping {
	var mappings []ColumnTableMapping

	for i, col := range columnList {
		if col == nil {
			continue
		}

		// Determine the table name for this column
		colTableName := col.TableName()
		if colTableName == "" {
			colTableName = defaultTableName
		}

		columnName := col.Name()
		columnID := buildColumnID(colTableName, columnName, int64(col.ColumnID()))

		// Get column index if available
		columnIndex := -1
		if columnIndexList != nil && i < len(columnIndexList) {
			columnIndex = columnIndexList[i]
		}

		mapping := ColumnTableMapping{
			ColumnID:    columnID,
			ColumnName:  columnName,
			TableName:   colTableName,
			TableAlias:  tableAlias,
			ColumnIndex: columnIndex,
		}

		mappings = append(mappings, mapping)
	}

	return mappings
}

// buildColumnID constructs a column ID string
func buildColumnID(tableName, columnName string, columnID int64) string {
	if tableName == "" {
		return fmt.Sprintf("%s#%d", columnName, columnID)
	}
	return fmt.Sprintf("%s.%s#%d", tableName, columnName, columnID)
}

// FindColumnByName finds a column mapping by column name (case-insensitive)
func (sr *ScanResult) FindColumnByName(columnName string) []ColumnTableMapping {
	var matches []ColumnTableMapping
	lowerName := strings.ToLower(columnName)

	for _, mapping := range sr.ColumnMappings {
		if strings.ToLower(mapping.ColumnName) == lowerName {
			matches = append(matches, mapping)
		}
	}

	return matches
}

// FindColumnsByTable finds all columns for a specific table (by name or alias)
func (sr *ScanResult) FindColumnsByTable(tableIdentifier string) []ColumnTableMapping {
	var matches []ColumnTableMapping
	lowerIdentifier := strings.ToLower(tableIdentifier)

	for _, mapping := range sr.ColumnMappings {
		if strings.ToLower(mapping.TableName) == lowerIdentifier ||
			strings.ToLower(mapping.TableAlias) == lowerIdentifier {
			matches = append(matches, mapping)
		}
	}

	return matches
}

// GetScanNodesByType returns all scan nodes of a specific type
func (sr *ScanResult) GetScanNodesByType(nodeType string) []ScanNodeInfo {
	var matches []ScanNodeInfo

	for _, node := range sr.ScanNodes {
		if strings.EqualFold(node.NodeType, nodeType) {
			matches = append(matches, node)
		}
	}

	return matches
}

// removeDuplicateStrings removes duplicate strings from a slice
func removeDuplicateStrings(slice []string) []string {
	seen := make(map[string]bool)
	var result []string

	for _, item := range slice {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}

	return result
}

// PrintScanResult prints the extracted scan node information in a readable format
func PrintScanResult(result *ScanResult) {
	fmt.Println("=== Scan Statistics ===")
	fmt.Printf("Total nodes processed: %d\n", result.Stats.TotalNodes)
	fmt.Printf("Scan nodes found: %d\n", result.Stats.ScanNodes)
	fmt.Printf("Total columns: %d\n", result.Stats.ColumnCount)
	fmt.Printf("Max depth: %d\n", result.Stats.MaxDepth)
	fmt.Printf("Tables found: %v\n", result.Stats.TablesFound)

	fmt.Println("\n=== Column to Table Mappings ===")
	for columnID, mapping := range result.ColumnMappings {
		aliasStr := ""
		if mapping.TableAlias != "" {
			aliasStr = fmt.Sprintf(", alias=\"%s\"", mapping.TableAlias)
		}

		fmt.Printf("%s -> Table: %s%s, Index: %d\n",
			columnID, mapping.TableName, aliasStr, mapping.ColumnIndex)
	}

	fmt.Println("\n=== Scan Nodes ===")
	for i, scanInfo := range result.ScanNodes {
		indent := strings.Repeat("  ", scanInfo.Depth)
		fmt.Printf("Node %d: %s%s", i+1, indent, scanInfo.NodeType)

		if scanInfo.TableName != "" {
			fmt.Printf(", table=%s", scanInfo.TableName)
		}

		if scanInfo.TableAlias != "" {
			fmt.Printf(", alias=\"%s\"", scanInfo.TableAlias)
		}

		// Print column list
		if len(scanInfo.Columns) > 0 {
			fmt.Print(", column_list=[")
			columnStrs := make([]string, len(scanInfo.Columns))
			for j, col := range scanInfo.Columns {
				columnStrs[j] = col.ColumnID
			}
			fmt.Print(strings.Join(columnStrs, ", "))
			fmt.Print("]")
		}
		fmt.Println()
	}
}

// Legacy compatibility function
func PrintScanNodeInfo(columnMappings map[string]ColumnTableMapping, scanNodes []ScanNodeInfo) {
	result := &ScanResult{
		ColumnMappings: columnMappings,
		ScanNodes:      scanNodes,
		Stats: ScanStats{
			ScanNodes:   len(scanNodes),
			ColumnCount: len(columnMappings),
		},
	}
	PrintScanResult(result)
}
