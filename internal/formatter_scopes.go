package internal

import (
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

// ZetaSQL Scan Nodes - Column Scope Classification
//
// This document describes how different ResolvedScan node types in ZetaSQL
// handle column scopes during AST traversal. Each scan node has specific
// behavior regarding which columns it produces, consumes, or transforms.
//
// Key Principle: Each Scan has a `column_list` that defines exactly what
// columns are produced. The Scan logically produces a stream of output rows,
// where each row has exactly these columns.
//
// Column scope behavior falls into five main categories:

// =============================================================================
// SCOPE BEHAVIOR MAPPING
// =============================================================================

// ScopeBehavior represents the different ways scan nodes handle column scopes
type ScopeBehavior int

const (
	ScopeOpener      ScopeBehavior = iota // Creates/produces new columns
	ScopeFilter                           // Removes/transforms available columns
	ScopePassthrough                      // Preserves input columns exactly
	ScopeMerger                           // Combines columns from multiple sources
	ScopeTransformer                      // Special column handling (CTEs, subqueries, etc.)
	ScopeOther                            // Unique/complex behavior
)

// String returns the string representation of ScopeBehavior
func (sb ScopeBehavior) String() string {
	switch sb {
	case ScopeOpener:
		return "OPENER"
	case ScopeFilter:
		return "FILTER"
	case ScopePassthrough:
		return "PASSTHROUGH"
	case ScopeMerger:
		return "MERGER"
	case ScopeTransformer:
		return "TRANSFORMER"
	case ScopeOther:
		return "OTHER"
	default:
		return "UNKNOWN"
	}
}

// NodeKindToScopeBehavior maps ZetaSQL resolved node kinds to their column scope behavior.
//
// This map provides a quick lookup for determining how any scan node handles
// column scopes during AST traversal. Use this for:
//   - Validating column resolution during visitor implementation
//   - Planning SQL generation strategies for different node types
//   - Debugging column availability issues in complex queries
//   - Understanding data flow through querybuilder trees
//
// Usage:
//
//	behavior := NodeKindToScopeBehavior[RESOLVED_PROJECT_SCAN]
//	if behavior == ScopeFilter {
//	    // Handle column restriction logic
//	}
var NodeKindToScopeBehavior = map[ast.Kind]ScopeBehavior{
	// SCOPE OPENERS - Create/Produce Columns
	ast.TableScan:            ScopeOpener, // Base table access
	ast.ArrayScan:            ScopeOpener, // UNNEST arrays
	ast.TVFScan:              ScopeOpener, // Table-valued functions
	ast.RelationArgumentScan: ScopeOpener, // FunctionCall relation arguments

	// SCOPE FILTERS - Remove/Transform Columns
	ast.ProjectScan:      ScopeFilter, // SELECT list projection
	ast.AggregateScan:    ScopeFilter, // GROUP BY aggregation
	ast.AnalyticScan:     ScopeFilter, // Window functions
	ast.SetOperationScan: ScopeFilter, // UNION/INTERSECT/EXCEPT

	// SCOPE PASSTHROUGH - Preserve Columns
	ast.FilterScan:      ScopePassthrough, // WHERE clause
	ast.OrderByScan:     ScopePassthrough, // ORDER BY clause
	ast.LimitOffsetScan: ScopePassthrough, // LIMIT/OFFSET
	ast.SampleScan:      ScopePassthrough, // TABLESAMPLE
	ast.SingleRowScan:   ScopePassthrough, // Single row generator (SELECT 1)

	// SCOPE MERGERS - Combine Columns
	ast.JoinScan:      ScopeMerger, // JOIN operations
	ast.RecursiveScan: ScopeMerger, // Recursive CTEs

	// SCOPE TRANSFORMERS - Special Column Handling
	ast.WithScan:     ScopeTransformer, // CTE definitions
	ast.WithRefScan:  ScopeTransformer, // CTE references
	ast.SubqueryExpr: ScopeTransformer, // Subquery wrapper
	ast.PivotScan:    ScopeTransformer, // PIVOT operation
	ast.UnpivotScan:  ScopeTransformer, // UNPIVOT operation

	// SCOPE OTHERS - Special Cases
	//"RESOLVED_BARRIER_SCAN":         ScopeOther, // Optimization barrier
	//"RESOLVED_MATCH_RECOGNIZE_SCAN": ScopeOther, // MATCH_RECOGNIZE pattern matching
	//"RESOLVED_GROUP_ROWS_SCAN":      ScopeOther, // GROUP_ROWS() aggregation
	//"RESOLVED_CLONE_SCAN":           ScopeOther, // Table cloning
}

// GetScopeBehavior returns the scope behavior for a given node kind string.
// Returns ScopeOther and false if the node kind is not found.
func GetScopeBehavior(nodeKind ast.Kind) (ScopeBehavior, bool) {
	behavior, exists := NodeKindToScopeBehavior[nodeKind]
	return behavior, exists
}

// IsScopeOpener returns true if the node kind creates/produces new columns
func IsScopeOpener(nodeKind ast.Kind) bool {
	behavior, exists := NodeKindToScopeBehavior[nodeKind]
	return exists && behavior == ScopeOpener
}

// IsScopeFilter returns true if the node kind removes/transforms columns
func IsScopeFilter(nodeKind ast.Kind) bool {
	behavior, exists := NodeKindToScopeBehavior[nodeKind]
	return exists && behavior == ScopeFilter
}

// IsScopePassthrough returns true if the node kind preserves input columns exactly
func IsScopePassthrough(nodeKind ast.Kind) bool {
	behavior, exists := NodeKindToScopeBehavior[nodeKind]
	return exists && behavior == ScopePassthrough
}

// IsScopeMerger returns true if the node kind combines columns from multiple sources
func IsScopeMerger(nodeKind ast.Kind) bool {
	behavior, exists := NodeKindToScopeBehavior[nodeKind]
	return exists && behavior == ScopeMerger
}

// IsScopeTransformer returns true if the node kind has special column handling
func IsScopeTransformer(nodeKind ast.Kind) bool {
	behavior, exists := NodeKindToScopeBehavior[nodeKind]
	return exists && behavior == ScopeTransformer
}

// GetNodesByBehavior returns all node kinds that exhibit the specified scope behavior
func GetNodesByBehavior(behavior ScopeBehavior) []ast.Kind {
	var nodes []ast.Kind
	for nodeKind, nodeBehavior := range NodeKindToScopeBehavior {
		if nodeBehavior == behavior {
			nodes = append(nodes, nodeKind)
		}
	}
	return nodes
}

// ValidateColumnFlow validates that column flow follows ZetaSQL scope rules.
//
// This function can be used during AST traversal to ensure that:
// - Scope openers properly introduce new columns
// - Scope filters correctly restrict available columns
// - Scope passthrough preserves column identity
// - Scope mergers properly combine column sets
// - Scope transformers handle special cases correctly
//
// Parameters:
//
//	nodeKind: The resolved node kind (e.g., "RESOLVED_PROJECT_SCAN")
//	inputColumns: Column IDs available from input scan(s)
//	outputColumns: Column IDs produced by this scan
//
// Returns error if column flow violates ZetaSQL scope rules.
func ValidateColumnFlow(nodeKind ast.Kind, inputColumns, outputColumns []string) error {
	behavior, exists := GetScopeBehavior(nodeKind)
	if !exists {
		return fmt.Errorf("unknown node kind: %v", nodeKind)
	}

	switch behavior {
	case ScopeOpener:
		// Openers can produce any columns (from tables, functions, etc.)
		// No validation needed - they define the initial scope
		return nil

	case ScopeFilter:
		// Filters must produce subset or transformation of input columns
		// Cannot validate without knowing the specific filtering logic
		return nil

	case ScopePassthrough:
		// Passthrough must preserve input columns exactly
		if len(inputColumns) != len(outputColumns) {
			return fmt.Errorf("passthrough node %v changed column count: input=%d, output=%d",
				nodeKind, len(inputColumns), len(outputColumns))
		}
		// Note: In practice, column IDs change but structure is preserved
		return nil

	case ScopeMerger:
		// Mergers combine multiple inputs - cannot validate without all inputs
		return nil

	case ScopeTransformer:
		// Transformers have complex rules - validation depends on specific node
		return nil

	case ScopeOther:
		// Special cases require node-specific validation
		return nil

	default:
		return fmt.Errorf("unknown scope behavior: %v", behavior)
	}
}

// =============================================================================
// SCOPE OPENERS (Create/Produce Columns)
// =============================================================================
//
// These nodes introduce new columns into the scope from external sources
// like tables, functions, or generated data.

// ResolvedTableScan represents base table access operations.
//
// Column Behavior: PRODUCES table columns from schema
// - Reads columns from table definition in catalog
// - column_index_list matches 1:1 with the column_list
// - Identifies ordinal of corresponding column in table's column list
// - Creates foundation columns that flow upward through AST
//
// Example:
//
//	TableScan(column_list=[users.id#1, users.name#2], table=users)
//	-> Produces: users.id#1, users.name#2
type ResolvedTableScan struct{}

// ResolvedArrayScan represents UNNEST array operations.
//
// Column Behavior: ADDS element + offset columns to input scope
// - element_column_list are new columns storing array element values
// - array_offset_column stores 0-based array position (optional)
// - column_list includes input_scan columns + element + offset columns
// - Creates CROSS JOIN UNNEST pattern in SQL generation
//
// Example:
//
//	ArrayScan(
//	  column_list=[users.name#1, tag#2, pos#3],
//	  input_scan=TableScan(users),
//	  element_column_list=[tag#2],
//	  array_offset_column=pos#3
//	)
//	-> Input: users.name#1
//	-> Adds: tag#2 (array element), pos#3 (array position)
type ResolvedArrayScan struct{}

// ResolvedTVFScan represents table-valued function calls.
//
// Column Behavior: PRODUCES function output columns
// - Output columns defined by TVF signature
// - May have parameters from outer scope (correlated)
// - Creates new column scope independent of input tables
//
// Example:
//
//	TVFScan(column_list=[result#1, count#2], function=GenerateArray(1, 10))
//	-> Produces: result#1, count#2
type ResolvedTVFScan struct{}

// ResolvedRelationArgumentScan represents function relation arguments.
//
// Column Behavior: PRODUCES argument table columns
// - Used in TVF calls that accept table arguments
// - Passes through columns from argument table
// - Maintains column identity from source relation
type ResolvedRelationArgumentScan struct{}

// ResolvedSingleRowScan represents single row generators.
//
// Column Behavior: PRODUCES empty row (no columns)
// - Used for queries without FROM clause (SELECT 1)
// - Creates single row with no columns for expression evaluation
// - Provides execution context for scalar expressions
//
// Example:
//
//	SingleRowScan(column_list=[])
//	-> Produces: (empty - single row, no columns)
type ResolvedSingleRowScan struct{}

// ResolvedValueTableScan represents value table access.
//
// Column Behavior: PRODUCES single anonymous column
// - Value tables have single unnamed column containing structured data
// - Column represents entire row value (STRUCT, PROTO, etc.)
// - Used with AS STRUCT/AS VALUE table patterns
type ResolvedValueTableScan struct{}

// =============================================================================
// SCOPE FILTERS (Remove/Transform Columns)
// =============================================================================
//
// These nodes restrict which columns are available to parent nodes,
// typically implementing SQL clauses that transform or limit column visibility.

// ResolvedProjectScan represents SELECT list projection.
//
// Column Behavior: RESTRICTS to only projected columns
// - Most important scope filter in SQL queries
// - column_list contains ONLY projected/computed columns
// - Input columns available for expressions but not passed through
// - Each expr in expr_list creates new output column
// - Implements column aliasing and computed expressions
//
// Example:
//
//	ProjectScan(
//	  column_list=[name#5, total#6],
//	  input_scan=TableScan(column_list=[id#1, name#2, salary#3, bonus#4]),
//	  expr_list=[
//	    ComputedColumn(column=name#5, expr=ColumnRef(name#2)),
//	    ComputedColumn(column=total#6, expr=Add(salary#3, bonus#4))
//	  ]
//	)
//	-> Input available: id#1, name#2, salary#3, bonus#4
//	-> Output restricted to: name#5, total#6
type ResolvedProjectScan struct{}

// ResolvedAggregateScan represents GROUP BY aggregation.
//
// Column Behavior: RESTRICTS to GROUP BY + aggregate columns
// - column_list contains grouping columns + aggregate result columns
// - Input columns not in GROUP BY become unavailable
// - Aggregate functions create new columns
// - Implements HAVING clause filtering after grouping
//
// Example:
//
//	AggregateScan(
//	  column_list=[dept#5, avg_sal#6],
//	  groupby_list=[dept#2],
//	  aggregate_list=[Avg(salary#3) AS avg_sal#6]
//	)
//	-> Groups by: dept#2
//	-> Produces: dept#5 (grouped), avg_sal#6 (aggregate)
type ResolvedAggregateScan struct{}

// ResolvedAnalyticScan represents window functions.
//
// Column Behavior: RESTRICTS to input + window function columns
// - Adds window function result columns to input column set
// - Window functions computed over partitions/ordering
// - OVER clause defines computation window
// - Input columns preserved plus analytic results
//
// Example:
//
//	AnalyticScan(
//	  column_list=[name#1, salary#2, rank#5],
//	  input_scan=TableScan(column_list=[name#1, salary#2]),
//	  function_group_list=[
//	    RowNumber() OVER (ORDER BY salary#2 DESC) AS rank#5
//	  ]
//	)
//	-> Input: name#1, salary#2
//	-> Adds: rank#5 (window function result)
type ResolvedAnalyticScan struct{}

// ResolvedSetOperationScan represents UNION/INTERSECT/EXCEPT operations.
//
// Column Behavior: RESTRICTS to common column structure
// - Combines multiple input queries with compatible schemas
// - Output columns aligned positionally across inputs
// - Column types must be compatible/coercible
// - Column names taken from first (left) input
//
// Example:
//
//	SetOperationScan(
//	  op_type=UNION_ALL,
//	  column_list=[name#7, count#8],
//	  input_list=[
//	    Query1(column_list=[emp_name#1, emp_count#2]),
//	    Query2(column_list=[cust_name#3, cust_count#4])
//	  ]
//	)
//	-> Aligns: emp_name#1 ↔ cust_name#3 → name#7
//	-> Aligns: emp_count#2 ↔ cust_count#4 → count#8
type ResolvedSetOperationScan struct{}

// =============================================================================
// SCOPE PASSTHROUGH (Preserve Columns)
// =============================================================================
//
// These nodes pass through their input's column_list unchanged.
// They modify row content or ordering but don't affect column availability.

// ResolvedFilterScan represents WHERE clause filtering.
//
// Column Behavior: PRESERVES input columns exactly
// - column_list identical to input_scan column_list
// - WHERE condition can reference any input column
// - Filters rows but doesn't change column structure
// - Most common passthrough scan type
//
// Example:
//
//	FilterScan(
//	  column_list=[id#1, name#2, salary#3],  // Same as input
//	  input_scan=TableScan(column_list=[id#1, name#2, salary#3]),
//	  filter_expr=Greater(salary#3, Literal(50000))
//	)
//	-> Input: id#1, name#2, salary#3
//	-> Output: id#1, name#2, salary#3 (same columns, fewer rows)
type ResolvedFilterScan struct{}

// ResolvedOrderByScan represents ORDER BY clause.
//
// Column Behavior: PRESERVES input columns exactly
// - column_list identical to input_scan column_list
// - ORDER BY expressions can reference any input column
// - Changes row ordering but not column structure
// - Sets is_ordered=true for parent scans
//
// Example:
//
//	OrderByScan(
//	  column_list=[name#1, salary#2],  // Same as input
//	  input_scan=TableScan(column_list=[name#1, salary#2]),
//	  order_by_list=[OrderByItem(salary#2, DESC)]
//	)
//	-> Preserves: name#1, salary#2 (same columns, sorted rows)
type ResolvedOrderByScan struct{}

// ResolvedLimitOffsetScan represents LIMIT/OFFSET clause.
//
// Column Behavior: PRESERVES input columns exactly
// - column_list identical to input_scan column_list
// - LIMIT/OFFSET values must be non-negative integer literals or parameters
// - Restricts row count but doesn't change column structure
// - Preserves ordering from input scan
//
// Example:
//
//	LimitOffsetScan(
//	  column_list=[name#1, salary#2],  // Same as input
//	  input_scan=OrderedScan(column_list=[name#1, salary#2]),
//	  limit=Literal(10),
//	  offset=Literal(5)
//	)
//	-> Preserves: name#1, salary#2 (same columns, limited rows)
type ResolvedLimitOffsetScan struct{}

// ResolvedSampleScan represents TABLESAMPLE clause.
//
// Column Behavior: PRESERVES input columns exactly
// - column_list identical to input_scan column_list
// - Adds optional weight_column for sampling weights
// - Supports BERNOULLI and RESERVOIR sampling methods
// - May include REPEATABLE clause for deterministic sampling
//
// Example:
//
//	SampleScan(
//	  column_list=[id#1, name#2],  // Same as input
//	  input_scan=TableScan(column_list=[id#1, name#2]),
//	  method="BERNOULLI",
//	  size=Literal(10.5),
//	  unit=PERCENT
//	)
//	-> Preserves: id#1, name#2 (same columns, sampled rows)
type ResolvedSampleScan struct{}

// ResolvedExecuteAsRoleScan represents role context wrapper.
//
// Column Behavior: PRESERVES input columns (with new IDs)
// - Creates new output columns that map 1:1 with input columns
// - Column types and names preserved but get new unique IDs
// - Establishes security/role context boundary
// - Makes this node a tracing boundary for rewriters
//
// Example:
//
//	ExecuteAsRoleScan(
//	  column_list=[id#5, name#6],  // New IDs, same structure
//	  input_scan=TableScan(column_list=[id#1, name#2]),
//	  delegated_user_catalog_object=Role("analyst")
//	)
//	-> Input: id#1, name#2
//	-> Output: id#5, name#6 (new IDs, same data/types)
type ResolvedExecuteAsRoleScan struct{}

// =============================================================================
// SCOPE MERGERS (Combine Columns)
// =============================================================================
//
// These nodes combine columns from multiple input sources,
// implementing SQL join operations and set operations.

// ResolvedJoinScan represents JOIN operations.
//
// Column Behavior: COMBINES left + right columns
// - column_list contains columns from both input scans
// - Left input columns appear first, then right input columns
// - JOIN condition can reference columns from both sides
// - USING clause may affect column deduplication
// - Different join types (INNER, LEFT, RIGHT, FULL, CROSS) affect row filtering
//
// Example:
//
//	JoinScan(
//	  join_type=INNER,
//	  column_list=[u.id#1, u.name#2, p.title#3, p.user_id#4],
//	  left_scan=TableScan(users, column_list=[u.id#1, u.name#2]),
//	  right_scan=TableScan(profiles, column_list=[p.title#3, p.user_id#4]),
//	  join_condition=Equal(u.id#1, p.user_id#4)
//	)
//	-> Left: u.id#1, u.name#2
//	-> Right: p.title#3, p.user_id#4
//	-> Combined: u.id#1, u.name#2, p.title#3, p.user_id#4
type ResolvedJoinScan struct{}

// ResolvedRecursiveScan represents recursive CTEs.
//
// Column Behavior: COMBINES non-recursive + recursive parts
// - Implements WITH RECURSIVE clause functionality
// - non_recursive_term establishes initial result set
// - recursive_term references the CTE being defined
// - column_list combines both parts with consistent schema
// - Requires union-compatible column types
//
// Example:
//
//	RecursiveScan(
//	  column_list=[id#5, parent_id#6, level#7],
//	  non_recursive_term=BaseQuery(column_list=[id#1, parent_id#2, level#3]),
//	  recursive_term=RecursiveQuery(column_list=[id#1, parent_id#2, level#4])
//	)
//	-> Combines recursive and non-recursive parts iteratively
type ResolvedRecursiveScan struct{}

// =============================================================================
// SCOPE TRANSFORMERS (Special Column Handling)
// =============================================================================
//
// These nodes have unique column transformation behavior that doesn't fit
// the standard categories, often implementing complex SQL features.

// ResolvedWithScan represents CTE definitions.
//
// Column Behavior: ISOLATES CTE scopes, EXPOSES CTE outputs
// - with_entry_list defines multiple CTEs with isolated scopes
// - Each CTE has independent column scope during definition
// - CTE output columns become available to main querybuilder
// - CTE aliases are unique within querybuilder scope
// - Supports both recursive and non-recursive CTEs
//
// Example:
//
//	WithScan(
//	  column_list=[name#7, total_orders#8],  // Main querybuilder output
//	  with_entry_list=[
//	    WithEntry(
//	      name="customer_stats",
//	      querybuilder=AggregateScan(column_list=[name#3, total#4])
//	    )
//	  ],
//	  querybuilder=ProjectScan(
//	    input_scan=WithRefScan("customer_stats", column_list=[name#5, total#6])
//	  )
//	)
//	-> CTE "customer_stats" isolated during definition
//	-> CTE output becomes available as WithRefScan input
type ResolvedWithScan struct{}

// ResolvedWithRefScan represents CTE references.
//
// Column Behavior: MAPS CTE columns to new IDs (1:1)
// - References previously defined CTE by name
// - column_list matches 1:1 with referenced CTE output
// - Each column gets new unique ID but preserves type/name
// - Enables CTE reuse in multiple locations
//
// Example:
//
//	WithRefScan(
//	  with_query_name="customer_stats",
//	  column_list=[name#5, total#6]  // New IDs
//	)
//	-> References CTE with column_list=[name#3, total#4]
//	-> Maps: name#3 → name#5, total#4 → total#6
type ResolvedWithRefScan struct{}

// ResolvedSubqueryScan represents subquery wrappers.
//
// Column Behavior: ISOLATES subquery scope
// - Creates scope boundary between inner and outer queries
// - Subquery has completely independent column scope
// - Only subquery output columns visible to parent
// - Used in FROM clause subqueries and table expressions
//
// Example:
//
//	SubqueryScan(
//	  column_list=[avg_sal#5],
//	  subquery=ProjectScan(
//	    column_list=[avg_sal#3],
//	    input_scan=AggregateScan(...)
//	  )
//	)
//	-> Subquery scope isolated during evaluation
//	-> Only subquery output (avg_sal#5) available to parent
type ResolvedSubqueryScan struct{}

// ResolvedPivotScan represents PIVOT operations.
//
// Column Behavior: TRANSFORMS rows to columns
// - Takes input rows and converts to columns based on pivot values
// - Grouping columns preserved in output
// - Pivot values become new column names
// - Aggregate values populate the pivoted columns
// - Complex column transformation from vertical to horizontal layout
//
// Example:
//
//	PivotScan(
//	  column_list=[product#5, Q1_sales#6, Q2_sales#7],
//	  input_scan=TableScan(column_list=[product#1, quarter#2, sales#3]),
//	  pivot_expr_list=[quarter#2],
//	  pivot_value_list=["Q1", "Q2"],
//	  aggregate_list=[Sum(sales#3)]
//	)
//	-> Input rows: (product, quarter, sales)
//	-> Output columns: (product, Q1_sales, Q2_sales)
type ResolvedPivotScan struct{}

// ResolvedUnpivotScan represents UNPIVOT operations.
//
// Column Behavior: TRANSFORMS columns to rows
// - Takes input columns and converts to rows
// - Creates value column containing unpivoted data
// - Creates name column containing original column names
// - Preserves non-unpivoted columns
// - Inverse operation of PIVOT
//
// Example:
//
//	UnpivotScan(
//	  column_list=[product#5, quarter#6, sales#7],
//	  input_scan=TableScan(column_list=[product#1, Q1_sales#2, Q2_sales#3]),
//	  unpivot_value_columns=[Q1_sales#2, Q2_sales#3],
//	  unpivot_name_column=quarter#6,
//	  unpivot_value_column=sales#7
//	)
//	-> Input columns: (product, Q1_sales, Q2_sales)
//	-> Output rows: (product, quarter, sales)
type ResolvedUnpivotScan struct{}

// =============================================================================
// SCOPE OTHERS (Special Cases)
// =============================================================================
//
// These nodes have unique or complex scope behavior that requires
// special handling during AST traversal and SQL generation.

// ResolvedBarrierScan represents optimization barriers.
//
// Column Behavior: PRESERVES with optimization boundary
// - column_list identical to input_scan column_list
// - Prevents certain querybuilder optimizations across boundary
// - Used by querybuilder optimizer to control transformation scope
// - Transparent to column scope but affects querybuilder planning
//
// Example:
//
//	BarrierScan(
//	  column_list=[id#1, name#2],  // Same as input
//	  input_scan=FilterScan(column_list=[id#1, name#2])
//	)
//	-> Preserves: id#1, name#2 (blocks optimization passes)
type ResolvedBarrierScan struct{}

// ResolvedMatchRecognizeScan represents MATCH_RECOGNIZE clause.
//
// Column Behavior: COMPLEX pattern matching columns
// - Implements SQL MATCH_RECOGNIZE for pattern detection in ordered data
// - Input columns available for pattern matching expressions
// - Adds pattern matching result columns
// - PARTITION BY and ORDER BY clauses define matching scope
// - Pattern variables create complex column dependencies
//
// Example:
//
//	MatchRecognizeScan(
//	  column_list=[symbol#1, price#2, match_id#5, start_row#6],
//	  input_scan=TableScan(column_list=[symbol#1, price#2, date#3]),
//	  partition_by=[symbol#1],
//	  pattern="STRT DOWN+ UP+",
//	  measures=[match_id#5, start_row#6]
//	)
//	-> Input: symbol#1, price#2, date#3
//	-> Adds: match_id#5, start_row#6 (pattern results)
type ResolvedMatchRecognizeScan struct{}

// ResolvedGroupRowsScan represents GROUP_ROWS() aggregation.
//
// Column Behavior: AGGREGATES into array columns
// - Special aggregation that collects entire rows into arrays
// - Similar to GROUP BY but preserves row structure
// - Creates array-valued columns containing grouped rows
// - Used in advanced analytics and data processing
//
// Example:
//
//	GroupRowsScan(
//	  column_list=[dept#5, employee_rows#6],
//	  input_scan=TableScan(column_list=[dept#1, name#2, salary#3]),
//	  groupby_list=[dept#1]
//	)
//	-> Groups by: dept#1
//	-> Produces: dept#5, employee_rows#6 (ARRAY<STRUCT<name, salary>>)
type ResolvedGroupRowsScan struct{}

// ResolvedCloneScan represents table cloning operations.
//
// Column Behavior: PRESERVES source table columns
// - Used in CREATE TABLE ... CLONE operations
// - column_list matches source table exactly
// - Preserves column names, types, and ordering
// - Creates new table with identical structure
//
// Example:
//
//	CloneScan(
//	  column_list=[id#1, name#2, created_at#3],  // Same as source
//	  source_table=Table("users")
//	)
//	-> Clones: id#1, name#2, created_at#3 (identical to source)
type ResolvedCloneScan struct{}

// =============================================================================
// COLUMN SCOPE MANAGEMENT PRINCIPLES
// =============================================================================
//
// Key principles that govern all ZetaSQL scan nodes:
//
// 1. **Column List Authority**: Each Scan has a `column_list` that defines
//    exactly what columns are produced. This is the authoritative source
//    for column availability to parent nodes.
//
// 2. **Column Identity**: Each column gets a unique ID (e.g., name#1).
//    Column names have no semantic meaning - IDs determine identity.
//    Column references must resolve to available column IDs.
//
// 3. **Scope Isolation**: Child scopes cannot see parent columns unless
//    explicitly passed as parameters. Parent scopes see only what
//    child's column_list exposes.
//
// 4. **Ordering Preservation**: Only ResolvedOrderByScan, ResolvedLimitOffsetScan,
//    ResolvedProjectScan, and ResolvedWithScan may have is_ordered=true.
//    Other scan nodes always discard ordering.
//
// 5. **Type Safety**: Column types must be preserved or properly coerced
//    through scope transformations. Type compatibility is enforced
//    at scope boundaries.
//
// These principles ensure consistent and predictable column scope behavior
// across all ZetaSQL scan node types, enabling reliable SQL generation
// and querybuilder optimization.
