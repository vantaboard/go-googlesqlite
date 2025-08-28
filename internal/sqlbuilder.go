package internal

import (
	"fmt"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"strings"
)

// SQLFragment represents any component that can generate SQL
type SQLFragment interface {
	WriteSql(writer *SQLWriter) error
	String() string
}

// SQLWriter handles SQL string generation with proper formatting
type SQLWriter struct {
	builder     strings.Builder
	indentLevel int
	useNewlines bool
}

func NewSQLWriter() *SQLWriter {
	return &SQLWriter{
		useNewlines: true,
	}
}

func (w *SQLWriter) Write(s string) {
	w.builder.WriteString(s)
}

func (w *SQLWriter) WriteLine(s string) {
	if w.useNewlines {
		w.builder.WriteString(strings.Repeat("  ", w.indentLevel))
	}
	w.builder.WriteString(s)
	if w.useNewlines {
		w.builder.WriteString("\n")
	}
}

func (w *SQLWriter) Indent() {
	w.indentLevel++
}

func (w *SQLWriter) Dedent() {
	if w.indentLevel > 0 {
		w.indentLevel--
	}
}

func (w *SQLWriter) String() string {
	return w.builder.String()
}

// SelectType represents different SELECT variants
type SelectType int

const (
	SelectTypeStandard SelectType = iota
	SelectTypeDistinct
	SelectTypeAll
	SelectTypeAsStruct
	SelectTypeAsValue
)

// ExpressionType represents different types of SQL expressions
type ExpressionType int

const (
	ExpressionTypeColumn ExpressionType = iota
	ExpressionTypeLiteral
	ExpressionTypeFunction
	ExpressionTypeBinary
	ExpressionTypeUnary
	ExpressionTypeSubquery
	ExpressionTypeStar
	ExpressionTypeCase
	ExpressionTypeExists
)

// CaseExpression represents SQL CASE expressions
type CaseExpression struct {
	CaseExpr    *SQLExpression // Optional expression after CASE (for CASE expr WHEN...)
	WhenClauses []*WhenClause  // WHEN condition THEN result pairs
	ElseExpr    *SQLExpression // Optional ELSE expression
}

// WhenClause represents a WHEN-THEN clause in a CASE expression
type WhenClause struct {
	Condition *SQLExpression
	Result    *SQLExpression
}

// ExistsExpression represents SQL EXISTS expressions
type ExistsExpression struct {
	Subquery *SelectStatement
}

// SQLExpression represents any SQL expression
type SQLExpression struct {
	Type       ExpressionType
	Value      string
	Left       *SQLExpression
	Right      *SQLExpression
	Operator   string
	Function   *FunctionCall
	Subquery   *SelectStatement
	CaseExpr   *CaseExpression
	ExistsExpr *ExistsExpression
	Alias      string
	TableAlias string
	Collation  string
}

func (e *SQLExpression) WriteSql(writer *SQLWriter) error {
	switch e.Type {
	case ExpressionTypeColumn:
		if e.TableAlias != "" {
			writer.Write(fmt.Sprintf("`%s`.`%s`", e.TableAlias, e.Value))
		} else {
			writer.Write("`" + e.Value + "`")
		}
	case ExpressionTypeLiteral:
		writer.Write(e.Value)
	case ExpressionTypeBinary:
		if e.Left != nil {
			e.Left.WriteSql(writer)
		}
		writer.Write(fmt.Sprintf(" %s ", e.Operator))
		if e.Right != nil {
			e.Right.WriteSql(writer)
		}
	case ExpressionTypeFunction:
		if e.Function != nil {
			e.Function.WriteSql(writer)
		}
	case ExpressionTypeSubquery:
		writer.Write("(")
		if e.Subquery != nil {
			e.Subquery.WriteSql(writer)
		}
		writer.Write(")")
	case ExpressionTypeStar:
		if e.TableAlias != "" {
			writer.Write(fmt.Sprintf("%s.*", e.TableAlias))
		} else {
			writer.Write("*")
		}
	case ExpressionTypeCase:
		if e.CaseExpr != nil {
			e.CaseExpr.WriteSql(writer)
		}
	case ExpressionTypeExists:
		if e.ExistsExpr != nil {
			e.ExistsExpr.WriteSql(writer)
		}
	}

	// Add collation if specified
	if e.Collation != "" {
		writer.Write(fmt.Sprintf(" COLLATE %s", e.Collation))
	}

	return nil
}

func (e *SQLExpression) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = e.Subquery != nil
	e.WriteSql(writer)
	return writer.String()
}

// WriteSql method for CaseExpression
func (c *CaseExpression) WriteSql(writer *SQLWriter) error {
	writer.Write("CASE")

	// Optional CASE expression (for CASE expr WHEN value THEN...)
	if c.CaseExpr != nil {
		writer.Write(" ")
		if err := c.CaseExpr.WriteSql(writer); err != nil {
			return err
		}
	}

	// WHEN clauses
	for _, whenClause := range c.WhenClauses {
		writer.Write(" WHEN ")
		if err := whenClause.Condition.WriteSql(writer); err != nil {
			return err
		}
		writer.Write(" THEN ")
		if err := whenClause.Result.WriteSql(writer); err != nil {
			return err
		}
	}

	// Optional ELSE clause
	if c.ElseExpr != nil {
		writer.Write(" ELSE ")
		if err := c.ElseExpr.WriteSql(writer); err != nil {
			return err
		}
	}

	writer.Write(" END")
	return nil
}

func (c *CaseExpression) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	c.WriteSql(writer)
	return writer.String()
}

// WriteSql method for ExistsExpression
func (e *ExistsExpression) WriteSql(writer *SQLWriter) error {
	writer.Write("EXISTS (")
	if e.Subquery != nil {
		if err := e.Subquery.WriteSql(writer); err != nil {
			return err
		}
	}
	writer.Write(")")
	return nil
}

func (e *ExistsExpression) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	e.WriteSql(writer)
	return writer.String()
}

// FunctionCall represents SQL function calls
type FunctionCall struct {
	Name       string
	Arguments  []*SQLExpression
	IsDistinct bool
	WindowSpec *WindowSpecification
}

func (f *FunctionCall) WriteSql(writer *SQLWriter) error {
	writer.Write(f.Name)
	writer.Write("(")
	if f.IsDistinct {
		writer.Write("DISTINCT ")
	}
	for i, arg := range f.Arguments {
		if i > 0 {
			writer.Write(", ")
		}
		arg.WriteSql(writer)
		i++
	}
	writer.Write(")")
	if f.WindowSpec != nil {
		writer.Write(" OVER (")
		f.WindowSpec.WriteSql(writer)
		writer.Write(")")
	}
	return nil
}

func (f *FunctionCall) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	f.WriteSql(writer)
	return writer.String()
}

// WindowSpecification represents OVER clause specifications
type WindowSpecification struct {
	PartitionBy []*SQLExpression
	OrderBy     []*OrderByItem
	FrameClause *FrameClause
}

func (w *WindowSpecification) WriteSql(writer *SQLWriter) error {
	if len(w.PartitionBy) > 0 {
		writer.Write("PARTITION BY ")
		for i, expr := range w.PartitionBy {
			if i > 0 {
				writer.Write(", ")
			}
			expr.WriteSql(writer)
		}
	}

	if len(w.OrderBy) > 0 {
		if len(w.PartitionBy) > 0 {
			writer.Write(" ")
		}
		writer.Write("ORDER BY ")
		for i, item := range w.OrderBy {
			if i > 0 {
				writer.Write(", ")
			}
			item.WriteSql(writer)
		}
	}

	if w.FrameClause != nil {
		if len(w.PartitionBy) > 0 || len(w.OrderBy) > 0 {
			writer.Write(" ")
		}
		w.FrameClause.WriteSql(writer)
	}

	return nil
}

// FrameClause represents window frame specifications
type FrameClause struct {
	Unit  string // ROWS, RANGE, GROUPS
	Start *FrameBound
	End   *FrameBound
}

func (f *FrameClause) WriteSql(writer *SQLWriter) error {
	writer.Write(f.Unit)
	if f.End != nil {
		writer.Write(" BETWEEN ")
		f.Start.WriteSql(writer)
		writer.Write(" AND ")
		f.End.WriteSql(writer)
	} else {
		writer.Write(" ")
		f.Start.WriteSql(writer)
	}
	return nil
}

// FrameBound represents frame boundary specifications
type FrameBound struct {
	Type   string // UNBOUNDED, CURRENT, PRECEDING, FOLLOWING
	Offset *SQLExpression
}

func (f *FrameBound) WriteSql(writer *SQLWriter) error {
	if f.Offset != nil {
		f.Offset.WriteSql(writer)
		writer.Write(" ")
	}
	writer.Write(f.Type)
	return nil
}

// SelectListItem represents an item in the SELECT clause
type SelectListItem struct {
	Expression      *SQLExpression
	Alias           string
	IsStarExpansion bool
	ExceptColumns   []string                  // For SELECT * EXCEPT
	ReplaceColumns  map[string]*SQLExpression // For SELECT * REPLACE
}

func (s *SelectListItem) WriteSql(writer *SQLWriter) error {
	if s.IsStarExpansion {
		s.Expression.WriteSql(writer)
		if len(s.ExceptColumns) > 0 {
			writer.Write(" EXCEPT (")
			for i, col := range s.ExceptColumns {
				if i > 0 {
					writer.Write(", ")
				}
				writer.Write("`" + col + "`")
			}
			writer.Write(")")
		}
		if len(s.ReplaceColumns) > 0 {
			writer.Write(" REPLACE (")
			i := 0
			for col, expr := range s.ReplaceColumns {
				if i > 0 {
					writer.Write(", ")
				}
				expr.WriteSql(writer)
				writer.Write(" AS `" + col + "`")
				i++
			}
			writer.Write(")")
		}
	} else {
		s.Expression.WriteSql(writer)
		if s.Alias != "" {
			writer.Write(" AS `" + s.Alias + "`")
		}
	}
	return nil
}

func (s *SelectListItem) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	s.WriteSql(writer)
	return writer.String()
}

// FromItemType represents different types of FROM clause items
type FromItemType int

const (
	FromItemTypeTable FromItemType = iota
	FromItemTypeSubquery
	FromItemTypeJoin
	FromItemTypeWithRef
	FromItemTypeTableFunction
	FromItemTypeUnnest
)

// JoinType represents different types of JOINs
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
	JoinTypeCross
)

// FromItem represents items in the FROM clause
type FromItem struct {
	Type          FromItemType
	TableName     string
	Alias         string
	Subquery      *SelectStatement
	Join          *JoinClause
	WithRef       string
	TableFunction *TableFunction
	UnnestExpr    *SQLExpression
	Hints         []string
}

func (f *FromItem) WriteSql(writer *SQLWriter) error {
	switch f.Type {
	case FromItemTypeTable:
		writer.Write(f.TableName)
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write(f.Alias)
		}
	case FromItemTypeSubquery:
		writer.Write("(")
		if f.Subquery != nil {
			f.Subquery.WriteSql(writer)
		}
		writer.Write(")")
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write(f.Alias)
		}
	case FromItemTypeJoin:
		if f.Join != nil {
			f.Join.WriteSql(writer)
		}
	case FromItemTypeWithRef:
		writer.Write(f.WithRef)
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write(f.Alias)
		}
	case FromItemTypeTableFunction:
		if f.TableFunction != nil {
			f.TableFunction.WriteSql(writer)
		}
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write(f.Alias)
		}
	case FromItemTypeUnnest:
		writer.Write("UNNEST(")
		if f.UnnestExpr != nil {
			f.UnnestExpr.WriteSql(writer)
		}
		writer.Write(")")
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write(f.Alias)
		}
	}
	return nil
}

func (f *FromItem) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	f.WriteSql(writer)
	return writer.String()
}

// JoinClause represents JOIN operations
type JoinClause struct {
	Type      JoinType
	Left      *FromItem
	Right     *FromItem
	Condition *SQLExpression
	Using     []string
}

func (j *JoinClause) WriteSql(writer *SQLWriter) error {
	if j.Left != nil {
		j.Left.WriteSql(writer)
	}

	switch j.Type {
	case JoinTypeInner:
		writer.Write(" INNER JOIN ")
	case JoinTypeLeft:
		writer.Write(" LEFT JOIN ")
	case JoinTypeRight:
		writer.Write(" RIGHT JOIN ")
	case JoinTypeFull:
		writer.Write(" FULL OUTER JOIN ")
	case JoinTypeCross:
		writer.Write(" CROSS JOIN ")
	}

	if j.Right != nil {
		j.Right.WriteSql(writer)
	}

	if j.Type != JoinTypeCross {
		if len(j.Using) > 0 {
			writer.Write(" USING (")
			for i, col := range j.Using {
				if i > 0 {
					writer.Write(", ")
				}
				writer.Write(col)
			}
			writer.Write(")")
		} else if j.Condition != nil {
			writer.Write(" ON ")
			j.Condition.WriteSql(writer)
		}
	}

	return nil
}

// TableFunction represents table-valued functions
type TableFunction struct {
	Name      string
	Arguments []*SQLExpression
}

func (t *TableFunction) WriteSql(writer *SQLWriter) error {
	writer.Write(t.Name)
	writer.Write("(")
	for i, arg := range t.Arguments {
		if i > 0 {
			writer.Write(", ")
		}
		arg.WriteSql(writer)
	}
	writer.Write(")")
	return nil
}

// OrderByItem represents items in ORDER BY clause
type OrderByItem struct {
	Expression *SQLExpression
	Direction  string // ASC, DESC
	NullsOrder string // NULLS FIRST, NULLS LAST
}

func (o *OrderByItem) WriteSql(writer *SQLWriter) error {
	o.Expression.WriteSql(writer)
	if o.Direction != "" {
		writer.Write(" ")
		writer.Write(o.Direction)
	}
	if o.NullsOrder != "" {
		writer.Write(" ")
		writer.Write(o.NullsOrder)
	}
	return nil
}

func (o *OrderByItem) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	o.WriteSql(writer)
	return writer.String()
}

// WithClause represents CTE (Common Table Expression) definitions
type WithClause struct {
	Name    string
	Columns []string
	Query   *SelectStatement
}

func (w *WithClause) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	w.WriteSql(writer)
	return writer.String()
}

func (w *WithClause) WriteSql(writer *SQLWriter) error {
	writer.Write(w.Name)
	if len(w.Columns) > 0 {
		writer.Write(" (")
		for i, col := range w.Columns {
			if i > 0 {
				writer.Write(", ")
			}
			writer.Write(col)
		}
		writer.Write(")")
	}
	writer.Write(" AS (")
	writer.WriteLine("")
	writer.Indent()
	if w.Query != nil {
		w.Query.WriteSql(writer)
	}
	writer.Dedent()
	writer.WriteLine(")")
	return nil
}

// SetOperation represents UNION, INTERSECT, EXCEPT operations
type SetOperation struct {
	Type     string // UNION, INTERSECT, EXCEPT
	Modifier string // ALL, DISTINCT
	Items    []*SelectStatement
}

func (s *SetOperation) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	s.WriteSql(writer)
	return writer.String()
}

func (s *SetOperation) WriteSql(writer *SQLWriter) error {
	for i := 0; i < len(s.Items); i++ {
		s.Items[i].WriteSql(writer)
		if i != len(s.Items)-1 {
			writer.WriteLine("")
			writer.Write(s.Type)
			if s.Modifier != "" {
				writer.Write(" ")
				writer.Write(s.Modifier)
			}
			writer.WriteLine("")
		}
	}
	return nil
}

// SelectStatement represents the main SELECT statement structure
type SelectStatement struct {
	// WITH clause
	WithClauses []*WithClause

	// SELECT clause
	SelectType   SelectType
	SelectList   []*SelectListItem
	AsStructType string
	AsValueType  string

	// FROM clause
	FromClause *FromItem

	// WHERE clause
	WhereClause *SQLExpression

	// GROUP BY clause
	GroupByList []*SQLExpression

	// HAVING clause
	HavingClause *SQLExpression

	// ORDER BY clause
	OrderByList []*OrderByItem

	// LIMIT clause
	LimitClause  *SQLExpression
	OffsetClause *SQLExpression

	// Set operations
	SetOperation *SetOperation

	// Hints
	Hints []string
}

func (s *SelectStatement) WriteSql(writer *SQLWriter) error {
	// WITH clause
	if len(s.WithClauses) > 0 {
		writer.Write("WITH ")
		for i, withClause := range s.WithClauses {
			if i > 0 {
				writer.Write(", ")
				writer.WriteLine("")
			}
			withClause.WriteSql(writer)
		}
		writer.WriteLine("")
	}

	// SetOperations implement their own writer for SELECT (but use WithClauses, GroupBy, OrderBy)
	if s.SetOperation != nil {
		s.SetOperation.WriteSql(writer)
	} else {
		// SELECT clause
		switch s.SelectType {
		case SelectTypeDistinct:
			writer.Write("SELECT DISTINCT")
		case SelectTypeAll:
			writer.Write("SELECT ALL")
		case SelectTypeAsStruct:
			writer.Write("SELECT AS STRUCT")
		case SelectTypeAsValue:
			writer.Write("SELECT AS VALUE")
		default:
			writer.Write("SELECT")
		}

		if s.AsStructType != "" {
			writer.Write(" AS ")
			writer.Write(s.AsStructType)
		}

		if len(s.SelectList) > 0 {
			writer.WriteLine("")
			writer.Indent()
			for i, item := range s.SelectList {
				if i > 0 {
					writer.Write(",")
					writer.WriteLine("")
				}
				item.WriteSql(writer)
			}
			writer.Dedent()
		}

		// FROM clause
		if s.FromClause != nil {
			writer.WriteLine("")
			writer.Write("FROM ")
			s.FromClause.WriteSql(writer)
		}
	}

	// WHERE clause
	if s.WhereClause != nil {
		writer.WriteLine("")
		writer.Write("WHERE ")
		s.WhereClause.WriteSql(writer)
	}

	// GROUP BY clause
	if len(s.GroupByList) > 0 {
		writer.WriteLine("")
		writer.Write("GROUP BY ")
		for i, expr := range s.GroupByList {
			if i > 0 {
				writer.Write(", ")
			}
			expr.WriteSql(writer)
		}
	}

	// HAVING clause
	if s.HavingClause != nil {
		writer.WriteLine("")
		writer.Write("HAVING ")
		s.HavingClause.WriteSql(writer)
	}

	// ORDER BY clause
	if len(s.OrderByList) > 0 {
		writer.WriteLine("")
		writer.Write("ORDER BY ")
		for i, item := range s.OrderByList {
			if i > 0 {
				writer.Write(", ")
			}
			item.WriteSql(writer)
		}
	}

	// LIMIT clause
	if s.LimitClause != nil {
		writer.WriteLine("")
		writer.Write("LIMIT ")
		s.LimitClause.WriteSql(writer)

		if s.OffsetClause != nil {
			writer.Write(" OFFSET ")
			s.OffsetClause.WriteSql(writer)
		}
	}

	return nil
}

func (s *SelectStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// Builder helper functions

// NewSelectStatement creates a new SELECT statement
func NewSelectStatement() *SelectStatement {
	return &SelectStatement{
		SelectType: SelectTypeStandard,
	}
}

func NewSelectStarStatement(from *FromItem) *SelectStatement {
	return &SelectStatement{
		SelectType: SelectTypeStandard,
		FromClause: from,
		SelectList: []*SelectListItem{
			{
				Expression: NewStarExpression(),
			},
		},
	}
}

// NewColumnExpression creates a new column reference expression
func NewColumnExpression(column string, tableAlias ...string) *SQLExpression {
	expr := &SQLExpression{
		Type:  ExpressionTypeColumn,
		Value: column,
	}
	if len(tableAlias) > 0 {
		expr.TableAlias = tableAlias[0]
	}
	return expr
}

// NewStarExpression creates a new star (*) expression for SELECT *
func NewStarExpression(tableAlias ...string) *SQLExpression {
	expr := &SQLExpression{
		Type: ExpressionTypeStar,
	}
	if len(tableAlias) > 0 {
		expr.TableAlias = tableAlias[0]
	}
	return expr
}

func getUniqueColumnName(column *ast.Column) string {
	return fmt.Sprintf("%s#%d", column.Name(), column.ColumnID())
}

// NewUniqueColumnExpression creates a new unique column reference expression
func NewUniqueColumnExpression(column *ast.Column, tableAlias ...string) *SQLExpression {
	expr := &SQLExpression{
		Type:  ExpressionTypeColumn,
		Value: getUniqueColumnName(column),
	}
	if len(tableAlias) > 0 {
		expr.TableAlias = tableAlias[0]
	}
	return expr
}

// NewLiteralExpression creates a new literal expression
func NewLiteralExpression(value string) *SQLExpression {
	return &SQLExpression{
		Type:  ExpressionTypeLiteral,
		Value: value,
	}
}

// NewLiteralExpressionFromGoValue creates a literal expression with the appropriate zetasqlite encoding
func NewLiteralExpressionFromGoValue(t types.Type, value interface{}) *SQLExpression {
	v, _ := EncodeGoValue(t, value)
	if s, _ := v.(string); s != "" {
		v = fmt.Sprintf(`'%v'`, s)
	}
	return &SQLExpression{
		Type:  ExpressionTypeLiteral,
		Value: fmt.Sprintf("%v", v),
	}
}

// NewLiteralFromGoValue creates a literal expression by automatically inferring the ZetaSQL type
func NewLiteralFromGoValue(t types.Type, value interface{}) (*SQLExpression, error) {
	encodedValue, err := EncodeGoValue(t, value)
	if err != nil {
		return nil, fmt.Errorf("failed to encode value: %w", err)
	}

	return &SQLExpression{
		Type:  ExpressionTypeLiteral,
		Value: fmt.Sprintf("%v", encodedValue),
	}, nil
}

// NewFunctionExpression creates a new function call expression
func NewFunctionExpression(name string, args ...*SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeFunction,
		Function: &FunctionCall{
			Name:      name,
			Arguments: args,
		},
	}
}

// NewBinaryExpression creates a new binary expression
func NewBinaryExpression(left *SQLExpression, operator string, right *SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type:     ExpressionTypeBinary,
		Left:     left,
		Operator: operator,
		Right:    right,
	}
}

// NewCaseExpression creates a new CASE expression (searched case)
func NewCaseExpression(whenClauses []*WhenClause, elseExpr *SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeCase,
		CaseExpr: &CaseExpression{
			WhenClauses: whenClauses,
			ElseExpr:    elseExpr,
		},
	}
}

// NewSimpleCaseExpression creates a new CASE expression with a case expression (simple case)
func NewSimpleCaseExpression(caseExpr *SQLExpression, whenClauses []*WhenClause, elseExpr *SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeCase,
		CaseExpr: &CaseExpression{
			CaseExpr:    caseExpr,
			WhenClauses: whenClauses,
			ElseExpr:    elseExpr,
		},
	}
}

// NewWhenClause creates a new WHEN clause for CASE expressions
func NewWhenClause(condition *SQLExpression, result *SQLExpression) *WhenClause {
	return &WhenClause{
		Condition: condition,
		Result:    result,
	}
}

// NewTableFromItem creates a table FROM item
func NewTableFromItem(tableName string, alias ...string) *FromItem {
	item := &FromItem{
		Type:      FromItemTypeTable,
		TableName: tableName,
	}
	if len(alias) > 0 {
		item.Alias = alias[0]
	}
	return item
}

// NewSubqueryFromItem creates a subquery FROM item
func NewSubqueryFromItem(subquery *SelectStatement, alias string) *FromItem {
	return &FromItem{
		Type:     FromItemTypeSubquery,
		Subquery: subquery,
		Alias:    alias,
	}
}

// NewExistsExpression creates a new EXISTS expression
func NewExistsExpression(subquery *SelectStatement) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeExists,
		ExistsExpr: &ExistsExpression{
			Subquery: subquery,
		},
	}
}

// NewInnerJoin creates an INNER JOIN
func NewInnerJoin(left, right *FromItem, condition *SQLExpression) *FromItem {
	return &FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:      JoinTypeInner,
			Left:      left,
			Right:     right,
			Condition: condition,
		},
	}
}

// FragmentType represents the type of SQL fragment
type FragmentType int

const (
	FragmentTypeExpression FragmentType = iota
	FragmentTypeSelectStatement
	FragmentTypeFromClause
	FragmentTypeJoinClause
	FragmentTypeOrderByClause
	FragmentTypeWhereClause
	FragmentTypeGroupByClause
	FragmentTypeHavingClause
	FragmentTypeLimitClause
	FragmentTypeColumnList
	FragmentTypeTableReference
)

// FragmentContext stores contextual information during AST traversal
type FragmentContext struct {
	// Current scope information
	AvailableColumns map[string]*ColumnInfo
	TableAliases     map[string]string
	CurrentScope     *ScopeInfo

	// Symbol management
	symbolTable    *SymbolTable
	aliasGenerator *AliasGenerator

	// Fragment storage by node ID
	fragments        map[NodeID]SQLFragment
	fragmentMetadata map[NodeID]*FragmentMetadata

	// Stack for nested contexts (subqueries, CTEs)
	scopeStack []*ScopeInfo

	// AST path tracking for generating stable NodeIDs
	astPath     *ASTPath
	WithEntries map[string]map[string]string
}

// NodeID represents a unique identifier for AST nodes based on path in the AST
type NodeID string

// ASTPath tracks the current path through the AST during traversal
type ASTPath struct {
	path []string
}

func NewASTPath() *ASTPath {
	return &ASTPath{
		path: make([]string, 0),
	}
}

func (p *ASTPath) Push(segment string) {
	p.path = append(p.path, segment)
}

func (p *ASTPath) Pop() {
	if len(p.path) > 0 {
		p.path = p.path[:len(p.path)-1]
	}
}

func (p *ASTPath) Current() NodeID {
	return NodeID(strings.Join(p.path, "/"))
}

func (p *ASTPath) Child(segment string) NodeID {
	childPath := make([]string, len(p.path)+1)
	copy(childPath, p.path)
	childPath[len(p.path)] = segment
	return NodeID(strings.Join(childPath, "/"))
}

// ColumnInfo stores metadata about available columns
type ColumnInfo struct {
	Name         string
	Type         string
	TableAlias   string
	Expression   *SQLExpression
	Node         *ast.Column
	IsAggregated bool
	Source       NodeID // Which node produced this column
}

// ScopeInfo tracks the current scope during traversal
type ScopeInfo struct {
	ScopeType     string // "query", "subquery", "cte", "window"
	OutputColumns map[string]*ColumnInfo
	InputScans    []NodeID
	ParentScope   *ScopeInfo
}

// FragmentMetadata stores additional information about fragments
type FragmentMetadata struct {
	NodeType        string
	OutputColumns   []*ColumnInfo
	RequiredColumns []*ColumnInfo
	IsOrdered       bool
	TableAliases    []string
	Dependencies    []NodeID
}

// SymbolTable manages symbol resolution and alias generation
type SymbolTable struct {
	symbols      map[string]*Symbol
	scopes       []*SymbolScope
	currentScope int
}

type Symbol struct {
	Name         string
	Type         string
	Alias        string
	NeedsAlias   bool
	ReferencedBy []NodeID
}

type SymbolScope struct {
	symbols map[string]*Symbol
	parent  *SymbolScope
	scopeID string
}

// AliasGenerator creates unique aliases for tables and columns
type AliasGenerator struct {
	usedAliases     map[string]bool
	tableCounter    int
	columnCounter   int
	subqueryCounter int
}

// FragmentStorage implements the main storage mechanism
type FragmentStorage struct {
	context *FragmentContext
}

func NewFragmentContext() *FragmentContext {
	return &FragmentContext{
		AvailableColumns: make(map[string]*ColumnInfo),
		TableAliases:     make(map[string]string),
		WithEntries:      make(map[string]map[string]string),
		fragments:        make(map[NodeID]SQLFragment),
		fragmentMetadata: make(map[NodeID]*FragmentMetadata),
		symbolTable:      NewSymbolTable(),
		aliasGenerator:   NewAliasGenerator(),
		scopeStack:       make([]*ScopeInfo, 0),
		astPath:          NewASTPath(),
	}
}

func NewSymbolTable() *SymbolTable {
	return &SymbolTable{
		symbols: make(map[string]*Symbol),
		scopes:  make([]*SymbolScope, 0),
	}
}

func NewAliasGenerator() *AliasGenerator {
	return &AliasGenerator{
		usedAliases: make(map[string]bool),
	}
}

// Core fragment storage methods

func (fc *FragmentContext) StoreFragment(nodeID NodeID, fragment SQLFragment, metadata *FragmentMetadata) {
	fc.fragments[nodeID] = fragment
	fc.fragmentMetadata[nodeID] = metadata
}

func (fc *FragmentContext) GetFragment(nodeID NodeID) (SQLFragment, *FragmentMetadata, bool) {
	fragment, exists := fc.fragments[nodeID]
	if !exists {
		return nil, nil, false
	}
	metadata := fc.fragmentMetadata[nodeID]
	return fragment, metadata, true
}

// Scope management methods

func (fc *FragmentContext) PushScope(scopeType string) {
	newScope := &ScopeInfo{
		ScopeType:     scopeType,
		OutputColumns: make(map[string]*ColumnInfo),
		InputScans:    make([]NodeID, 0),
		ParentScope:   fc.CurrentScope,
	}
	fc.scopeStack = append(fc.scopeStack, newScope)
	fc.CurrentScope = newScope
}

func (fc *FragmentContext) PopScope(alias string) *ScopeInfo {
	if len(fc.scopeStack) == 0 {
		return nil
	}

	currentScope := fc.CurrentScope
	fc.scopeStack = fc.scopeStack[:len(fc.scopeStack)-1]

	if len(fc.scopeStack) > 0 {
		fc.CurrentScope = fc.scopeStack[len(fc.scopeStack)-1]
	} else {
		fc.CurrentScope = nil
	}

	// Push referenced OutputColumns to the latest scope
	if currentScope != nil {
		for columnID, column := range currentScope.OutputColumns {
			// Once a scope has been finalized, the column is no longer available to be inlined as a direct expression
			// It must be referenced as a column
			column.Expression = nil

			// All references must use the generated table alias
			column.TableAlias = alias
			column.Name = fmt.Sprintf("col%d", column.Node.ColumnID())
			fc.AvailableColumns[columnID] = column
		}
	}

	return currentScope
}

// Column management methods

func (fc *FragmentContext) AddAvailableColumn(column *ast.Column, info *ColumnInfo) {
	name := getUniqueColumnName(column)
	info.Node = column
	info.Name = fmt.Sprintf("col%d", column.ColumnID())
	fc.AvailableColumns[name] = info
	if fc.CurrentScope != nil {
		fc.CurrentScope.OutputColumns[name] = info
	}
}

func (fc *FragmentContext) GetColumnExpression(column *ast.Column) *SQLExpression {
	columnID := getUniqueColumnName(column)
	columnInfo, exists := fc.AvailableColumns[columnID]
	if exists {
		if columnInfo.Expression != nil {
			return columnInfo.Expression
		}

		return NewColumnExpression(columnInfo.Name, columnInfo.TableAlias)
	}
	// All columns are expected to be visited before being referenced
	// Panic when we don't have a reference to this column
	panic(fmt.Sprintf("column not found in current scope: %s", columnID))
}

func (fc *FragmentContext) AddWithEntryColumnMapping(name string, columns []*ast.Column) {
	mapping := make(map[string]string)
	for _, column := range columns {
		mapping[column.Name()] = fmt.Sprintf("col%d", column.ColumnID())
	}
	fc.WithEntries[name] = mapping
}

// Alias generation methods

func (ag *AliasGenerator) GenerateTableAlias() string {
	ag.tableCounter++
	alias := fmt.Sprintf("t%d", ag.tableCounter)
	for ag.usedAliases[alias] {
		ag.tableCounter++
		alias = fmt.Sprintf("t%d", ag.tableCounter)
	}
	ag.usedAliases[alias] = true
	return alias
}

func (ag *AliasGenerator) GenerateSubqueryAlias() string {
	ag.subqueryCounter++
	alias := fmt.Sprintf("subquery%d", ag.subqueryCounter)
	for ag.usedAliases[alias] {
		ag.subqueryCounter++
		alias = fmt.Sprintf("subquery%d", ag.subqueryCounter)
	}
	ag.usedAliases[alias] = true
	return alias
}
