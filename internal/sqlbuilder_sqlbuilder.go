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

// WriteDebug writes debug information with tree structure formatting
func (w *SQLWriter) WriteDebug(prefix string, s string) {
	w.Write(prefix + s)
}

// WriteDebugLine writes debug information with tree structure formatting and newline
func (w *SQLWriter) WriteDebugLine(prefix string, s string) {
	if w.useNewlines {
		w.Write(prefix + s + "\n")
	} else {
		w.Write(prefix + s)
	}
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
	ExpressionTypeParameter
	ExpressionTypeFunction
	ExpressionTypeBinary
	ExpressionTypeUnary
	ExpressionTypeSubquery
	ExpressionTypeStar
	ExpressionTypeCase
	ExpressionTypeExists
	ExpressionTypeCast
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

type BinaryExpression struct {
	Left     *SQLExpression
	Right    *SQLExpression
	Operator string
}

func (e *BinaryExpression) WriteSql(writer *SQLWriter) error {
	if e.Left != nil {
		e.Left.WriteSql(writer)
	}
	writer.Write(fmt.Sprintf(" %s ", e.Operator))
	if e.Right != nil {
		e.Right.WriteSql(writer)
	}
	return nil
}

func (e *BinaryExpression) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = false
	e.WriteSql(writer)
	return writer.String()
}

func (e *BinaryExpression) WriteDebugString(writer *SQLWriter, prefix string) {
	writer.WriteDebugLine(prefix, fmt.Sprintf("+-BinaryExpression(operator=%s)", e.Operator))
	if e.Left != nil {
		writer.WriteDebugLine(prefix, "  +-left_operand=")
		e.Left.writeDebugString(writer, prefix+"    ")
	}
	if e.Right != nil {
		writer.WriteDebugLine(prefix, "  +-right_operand=")
		e.Right.writeDebugString(writer, prefix+"    ")
	}
}

// SQLExpression represents any SQL expression
type SQLExpression struct {
	Type             ExpressionType
	Value            string
	BinaryExpression *BinaryExpression
	FunctionCall     *FunctionCall
	Subquery         *SelectStatement
	CaseExpression   *CaseExpression
	ExistsExpr       *ExistsExpression
	Alias            string
	TableAlias       string
	Collation        string
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
		e.BinaryExpression.WriteSql(writer)
	case ExpressionTypeFunction:
		if e.FunctionCall != nil {
			e.FunctionCall.WriteSql(writer)
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
		if e.CaseExpression != nil {
			e.CaseExpression.WriteSql(writer)
		}
	case ExpressionTypeExists:
		if e.ExistsExpr != nil {
			e.ExistsExpr.WriteSql(writer)
		}
	case ExpressionTypeParameter:
		writer.Write(e.Value)
	}

	// Add collation if specified
	if e.Collation != "" {
		writer.Write(fmt.Sprintf(" COLLATE %s", e.Collation))
	}

	if e.Alias != "" {
		writer.Write(" AS ")
		writer.Write("`" + e.Alias + "`")
	}

	return nil
}

func (e *SQLExpression) String() string {
	writer := NewSQLWriter()
	writer.useNewlines = e.Subquery != nil
	e.WriteSql(writer)
	return writer.String()
}

// DebugString returns a hierarchical debug representation similar to ZetaSQL's node debug output
func (e *SQLExpression) writeDebugString(writer *SQLWriter, prefix string) {
	switch e.Type {
	case ExpressionTypeColumn:
		if e.TableAlias != "" {
			writer.WriteDebugLine(prefix, fmt.Sprintf("+-ColumnRef(table=%s, column=%s)", e.TableAlias, e.Value))
		} else {
			writer.WriteDebugLine(prefix, fmt.Sprintf("+-ColumnRef(column=%s)", e.Value))
		}
	case ExpressionTypeLiteral:
		writer.WriteDebugLine(prefix, fmt.Sprintf("+-Literal(value=%s)", e.Value))
	case ExpressionTypeBinary:
		e.BinaryExpression.WriteDebugString(writer, prefix)
	case ExpressionTypeFunction:
		if e.FunctionCall != nil {
			writer.WriteDebugLine(prefix, fmt.Sprintf("+-FunctionCall(name=%s)", e.FunctionCall.Name))
			for i, arg := range e.FunctionCall.Arguments {
				writer.WriteDebugLine(prefix, fmt.Sprintf("  +-argument[%d]=", i))
				arg.writeDebugString(writer, prefix+"    ")
			}
		}
	case ExpressionTypeSubquery:
		writer.WriteDebugLine(prefix, "+-SubqueryExpression")
		if e.Subquery != nil {
			writer.WriteDebugLine(prefix, "  +-subquery=")
			e.Subquery.writeDebugString(writer, prefix+"    ")
		}
	case ExpressionTypeStar:
		if e.TableAlias != "" {
			writer.WriteDebugLine(prefix, fmt.Sprintf("+-StarExpression(table=%s)", e.TableAlias))
		} else {
			writer.WriteDebugLine(prefix, "+-StarExpression")
		}
	case ExpressionTypeCase:
		writer.WriteDebugLine(prefix, "+-CaseExpression")
		if e.CaseExpression != nil && e.CaseExpression.CaseExpr != nil {
			writer.WriteDebugLine(prefix, "  +-case_expr=")
			e.CaseExpression.CaseExpr.writeDebugString(writer, prefix+"    ")
		}
		for i, whenClause := range e.CaseExpression.WhenClauses {
			writer.WriteDebugLine(prefix, fmt.Sprintf("  +-when_clause[%d]=", i))
			writer.WriteDebugLine(prefix+"    ", "+-condition=")
			whenClause.Condition.writeDebugString(writer, prefix+"      ")
			writer.WriteDebugLine(prefix+"    ", "+-result=")
			whenClause.Result.writeDebugString(writer, prefix+"      ")
		}
		if e.CaseExpression.ElseExpr != nil {
			writer.WriteDebugLine(prefix, "  +-else_expr=")
			e.CaseExpression.ElseExpr.writeDebugString(writer, prefix+"    ")
		}
	case ExpressionTypeExists:
		writer.WriteDebugLine(prefix, "+-ExistsExpression")
		if e.ExistsExpr != nil && e.ExistsExpr.Subquery != nil {
			writer.WriteDebugLine(prefix, "  +-subquery=")
			e.ExistsExpr.Subquery.writeDebugString(writer, prefix+"    ")
		}
	}

	if e.Alias != "" {
		writer.WriteDebugLine(prefix, fmt.Sprintf("  +-alias=%s", e.Alias))
	}
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

// TableReference represents a table reference in SQL
type TableReference struct {
	TableName string
	Alias     string
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
	FromItemTypeSingleRow
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
		writer.Write("`" + f.TableName + "`")
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write("`" + f.Alias + "`")
		}
	case FromItemTypeSubquery:
		writer.Write("(")
		if f.Subquery != nil {
			f.Subquery.WriteSql(writer)
		}
		writer.Write(")")
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write("`" + f.Alias + "`")
		}
	case FromItemTypeJoin:
		if f.Join != nil {
			f.Join.WriteSql(writer)
		}
	case FromItemTypeWithRef:
		writer.Write(f.WithRef)
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write("`" + f.Alias + "`")
		}
	case FromItemTypeTableFunction:
		if f.TableFunction != nil {
			f.TableFunction.WriteSql(writer)
		}
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write("`" + f.Alias + "`")
		}
	case FromItemTypeUnnest:
		writer.Write("UNNEST(")
		if f.UnnestExpr != nil {
			f.UnnestExpr.WriteSql(writer)
		}
		writer.Write(")")
		if f.Alias != "" {
			writer.Write(" AS ")
			writer.Write("`" + f.Alias + "`")
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

	// LIMIT OFFSET clause
	LimitClause *LimitClause

	// Set operations
	SetOperation *SetOperation

	// Hints
	Hints []string
}

type LimitClause struct {
	Count  *SQLExpression
	Offset *SQLExpression
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
		if s.FromClause != nil && s.FromClause.Type != FromItemTypeSingleRow {
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
		s.LimitClause.Count.WriteSql(writer)

		if s.LimitClause.Offset != nil {
			writer.Write(" OFFSET ")
			s.LimitClause.Offset.WriteSql(writer)
		}
	}

	return nil
}

func (s *SelectStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// DebugString returns a hierarchical debug representation similar to ZetaSQL's node debug output
func (s *SelectStatement) writeDebugString(writer *SQLWriter, prefix string) {
	selectType := "SELECT"
	switch s.SelectType {
	case SelectTypeDistinct:
		selectType = "SELECT DISTINCT"
	case SelectTypeAll:
		selectType = "SELECT ALL"
	case SelectTypeAsStruct:
		selectType = "SELECT AS STRUCT"
	case SelectTypeAsValue:
		selectType = "SELECT AS VALUE"
	}

	writer.WriteDebugLine(prefix, fmt.Sprintf("+-SelectStatement(type=%s)", selectType))

	if len(s.WithClauses) > 0 {
		writer.WriteDebugLine(prefix, "  +-with_clause_list=")
		for i, withClause := range s.WithClauses {
			writer.WriteDebugLine(prefix, fmt.Sprintf("    +-with_clause[%d]=%s", i, withClause.Name))
			if withClause.Query != nil {
				writer.WriteDebugLine(prefix, "      +-querybuilder=")
				withClause.Query.writeDebugString(writer, prefix+"        ")
			}
		}
	}

	if s.SetOperation != nil {
		writer.WriteDebugLine(prefix, fmt.Sprintf("  +-set_operation=%s %s", s.SetOperation.Type, s.SetOperation.Modifier))
		for i, item := range s.SetOperation.Items {
			writer.WriteDebugLine(prefix, fmt.Sprintf("    +-item[%d]=", i))
			item.writeDebugString(writer, prefix+"      ")
		}
	} else {
		if len(s.SelectList) > 0 {
			writer.WriteDebugLine(prefix, "  +-select_list=")
			for i, item := range s.SelectList {
				writer.WriteDebugLine(prefix, fmt.Sprintf("    +-select_item[%d]=", i))
				item.Expression.writeDebugString(writer, prefix+"      ")
				if item.Alias != "" {
					writer.WriteDebugLine(prefix, fmt.Sprintf("      +-alias=%s", item.Alias))
				}
			}
		}

		if s.FromClause != nil {
			writer.WriteDebugLine(prefix, "  +-from_clause=")
			s.writeFromItemDebug(writer, prefix+"    ", s.FromClause)
		}
	}

	if s.WhereClause != nil {
		writer.WriteDebugLine(prefix, "  +-where_clause=")
		s.WhereClause.writeDebugString(writer, prefix+"    ")
	}

	if len(s.GroupByList) > 0 {
		writer.WriteDebugLine(prefix, "  +-group_by_list=")
		for i, expr := range s.GroupByList {
			writer.WriteDebugLine(prefix, fmt.Sprintf("    +-group_by[%d]=", i))
			expr.writeDebugString(writer, prefix+"      ")
		}
	}

	if s.HavingClause != nil {
		writer.WriteDebugLine(prefix, "  +-having_clause=")
		s.HavingClause.writeDebugString(writer, prefix+"    ")
	}

	if len(s.OrderByList) > 0 {
		writer.WriteDebugLine(prefix, "  +-order_by_list=")
		for i, item := range s.OrderByList {
			writer.WriteDebugLine(prefix, fmt.Sprintf("    +-order_by[%d]=", i))
			item.Expression.writeDebugString(writer, prefix+"      ")
			if item.Direction != "" {
				writer.WriteDebugLine(prefix, fmt.Sprintf("      +-direction=%s", item.Direction))
			}
			if item.NullsOrder != "" {
				writer.WriteDebugLine(prefix, fmt.Sprintf("      +-nulls_order=%s", item.NullsOrder))
			}
		}
	}

	if s.LimitClause != nil {
		writer.WriteDebugLine(prefix, "  +-limit_clause=")
		if s.LimitClause.Count != nil {
			writer.WriteDebugLine(prefix, "    +-count=")
			s.LimitClause.Count.writeDebugString(writer, prefix+"      ")
		}
		if s.LimitClause.Offset != nil {
			writer.WriteDebugLine(prefix, "    +-offset=")
			s.LimitClause.Offset.writeDebugString(writer, prefix+"      ")
		}
	}
}

func (s *SelectStatement) writeFromItemDebug(writer *SQLWriter, prefix string, item *FromItem) {
	switch item.Type {
	case FromItemTypeTable:
		writer.WriteDebugLine(prefix, fmt.Sprintf("+-TableRef(name=%s)", item.TableName))
		if item.Alias != "" {
			writer.WriteDebugLine(prefix, fmt.Sprintf("  +-alias=%s", item.Alias))
		}
	case FromItemTypeSubquery:
		writer.WriteDebugLine(prefix, "+-Subquery")
		if item.Subquery != nil {
			writer.WriteDebugLine(prefix, "  +-querybuilder=")
			item.Subquery.writeDebugString(writer, prefix+"    ")
		}
		if item.Alias != "" {
			writer.WriteDebugLine(prefix, fmt.Sprintf("  +-alias=%s", item.Alias))
		}
	case FromItemTypeJoin:
		if item.Join != nil {
			joinType := "INNER"
			switch item.Join.Type {
			case JoinTypeLeft:
				joinType = "LEFT"
			case JoinTypeRight:
				joinType = "RIGHT"
			case JoinTypeFull:
				joinType = "FULL OUTER"
			case JoinTypeCross:
				joinType = "CROSS"
			}
			writer.WriteDebugLine(prefix, fmt.Sprintf("+-JoinScan(type=%s)", joinType))
			if item.Join.Left != nil {
				writer.WriteDebugLine(prefix, "  +-left_scan=")
				s.writeFromItemDebug(writer, prefix+"    ", item.Join.Left)
			}
			if item.Join.Right != nil {
				writer.WriteDebugLine(prefix, "  +-right_scan=")
				s.writeFromItemDebug(writer, prefix+"    ", item.Join.Right)
			}
			if item.Join.Condition != nil {
				writer.WriteDebugLine(prefix, "  +-join_condition=")
				item.Join.Condition.writeDebugString(writer, prefix+"    ")
			}
		}
	case FromItemTypeUnnest:
		writer.WriteDebugLine(prefix, "+-UnnestScan")
		if item.UnnestExpr != nil {
			writer.WriteDebugLine(prefix, "  +-expression=")
			item.UnnestExpr.writeDebugString(writer, prefix+"    ")
		}
		if item.Alias != "" {
			writer.WriteDebugLine(prefix, fmt.Sprintf("  +-alias=%s", item.Alias))
		}
	case FromItemTypeTableFunction:
		if item.TableFunction != nil {
			writer.WriteDebugLine(prefix, fmt.Sprintf("+-TableFunctionScan(name=%s)", item.TableFunction.Name))
			for i, arg := range item.TableFunction.Arguments {
				writer.WriteDebugLine(prefix, fmt.Sprintf("  +-argument[%d]=", i))
				arg.writeDebugString(writer, prefix+"    ")
			}
		}
	}
}

// CreateTableStatement WriteSql implementation
func (s *CreateTableStatement) WriteSql(writer *SQLWriter) error {
	writer.Write("CREATE TABLE")
	if s.IfNotExists {
		writer.Write(" IF NOT EXISTS")
	}
	writer.Write(" ")
	writer.Write(s.TableName)

	if s.AsSelect != nil {
		writer.Write(" AS ")
		return s.AsSelect.WriteSql(writer)
	}

	writer.Write(" (")
	writer.WriteLine("")
	writer.Indent()
	for i, col := range s.Columns {
		if i > 0 {
			writer.Write(",")
			writer.WriteLine("")
		}
		col.WriteSql(writer)
	}
	writer.Dedent()
	writer.WriteLine("")
	writer.Write(")")
	return nil
}

func (s *CreateTableStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// ColumnDefinition WriteSql implementation
func (c *ColumnDefinition) WriteSql(writer *SQLWriter) error {
	writer.Write(c.Name)
	writer.Write(" ")
	writer.Write(c.Type)
	if c.NotNull {
		writer.Write(" NOT NULL")
	}
	if c.IsPrimaryKey {
		writer.Write(" PRIMARY KEY")
	}
	if c.DefaultValue != nil {
		writer.Write(" DEFAULT ")
		c.DefaultValue.WriteSql(writer)
	}
	return nil
}

func (c *ColumnDefinition) String() string {
	writer := NewSQLWriter()
	c.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// CreateViewStatement WriteSql implementation
func (s *CreateViewStatement) WriteSql(writer *SQLWriter) error {
	writer.Write("CREATE VIEW")
	if s.IfNotExists {
		writer.Write(" IF NOT EXISTS")
	}
	writer.Write(" ")
	writer.Write(s.ViewName)
	writer.Write(" AS ")
	return s.Query.WriteSql(writer)
}

func (s *CreateViewStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// CreateFunctionStatement WriteSql implementation
func (s *CreateFunctionStatement) WriteSql(writer *SQLWriter) error {
	writer.Write("CREATE FUNCTION")
	if s.IfNotExists {
		writer.Write(" IF NOT EXISTS")
	}
	writer.Write(" ")
	writer.Write(s.FunctionName)
	writer.Write("(")
	for i, param := range s.Parameters {
		if i > 0 {
			writer.Write(", ")
		}
		param.WriteSql(writer)
	}
	writer.Write(")")
	if s.ReturnType != "" {
		writer.Write(" RETURNS ")
		writer.Write(s.ReturnType)
	}
	if s.Language != "" {
		writer.Write(" LANGUAGE ")
		writer.Write(s.Language)
	}
	if s.Code != "" {
		writer.Write(" AS ")
		writer.Write(s.Code)
	}
	return nil
}

func (s *CreateFunctionStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// ParameterDefinition WriteSql implementation
func (p *ParameterDefinition) WriteSql(writer *SQLWriter) error {
	writer.Write(p.Name)
	writer.Write(" ")
	writer.Write(p.Type)
	return nil
}

func (p *ParameterDefinition) String() string {
	writer := NewSQLWriter()
	p.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// DropStatement WriteSql implementation
func (s *DropStatement) WriteSql(writer *SQLWriter) error {
	writer.Write("DROP ")
	writer.Write(s.ObjectType)
	if s.IfExists {
		writer.Write(" IF EXISTS")
	}
	writer.Write(" ")
	writer.Write(s.ObjectName)
	return nil
}

func (s *DropStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// TruncateStatement WriteSql implementation
func (s *TruncateStatement) WriteSql(writer *SQLWriter) error {
	writer.Write("TRUNCATE TABLE ")
	writer.Write("`" + s.TableName + "`")
	return nil
}

func (s *TruncateStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// MergeStatement WriteSql implementation
func (s *MergeStatement) WriteSql(writer *SQLWriter) error {
	writer.Write("MERGE ")
	writer.Write(s.TargetTable)
	if s.SourceTable != nil {
		writer.Write(" USING ")
		s.SourceTable.WriteSql(writer)
	}
	if s.MergeClause != nil {
		writer.Write(" ON ")
		s.MergeClause.WriteSql(writer)
	}
	for _, when := range s.WhenClauses {
		writer.WriteLine("")
		when.WriteSql(writer)
	}
	return nil
}

func (s *MergeStatement) String() string {
	writer := NewSQLWriter()
	s.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// MergeWhenClause WriteSql implementation
func (c *MergeWhenClause) WriteSql(writer *SQLWriter) error {
	writer.Write("WHEN ")
	writer.Write(c.Type)
	if c.Condition != nil {
		writer.Write(" AND ")
		c.Condition.WriteSql(writer)
	}
	writer.Write(" THEN ")
	writer.Write(c.Action)
	if len(c.SetList) > 0 {
		writer.Write(" SET ")
		for i, set := range c.SetList {
			if i > 0 {
				writer.Write(", ")
			}
			set.WriteSql(writer)
		}
	}
	return nil
}

func (c *MergeWhenClause) String() string {
	writer := NewSQLWriter()
	c.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

// SetItem WriteSql implementation
func (s *SetItem) WriteSql(writer *SQLWriter) error {
	s.Column.WriteSql(writer)
	writer.Write(" = ")
	return s.Value.WriteSql(writer)
}

func (s *SetItem) String() string {
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

func GetUniqueColumnName(column *ast.Column) string {
	return fmt.Sprintf("%s__%d", column.Name(), column.ColumnID())
}

// NewUniqueColumnExpression creates a new unique column reference expression
func NewUniqueColumnExpression(column *ast.Column, tableAlias ...string) *SQLExpression {
	expr := &SQLExpression{
		Type:  ExpressionTypeColumn,
		Value: GetUniqueColumnName(column),
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

func NewLiteralExpressionFromGoValue(t types.Type, value interface{}) (*SQLExpression, error) {
	encoded, err := ValueFromGoValue(value)
	if err != nil {
		return nil, err
	}
	literal, err := LiteralFromValue(encoded)
	if err != nil {
		return nil, err
	}
	return NewLiteralExpression(literal), nil
}

// NewFunctionExpression creates a new function call expression
func NewFunctionExpression(name string, args ...*SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeFunction,
		FunctionCall: &FunctionCall{
			Name:      name,
			Arguments: args,
		},
	}
}

// NewBinaryExpression creates a new binary expression
func NewBinaryExpression(left *SQLExpression, operator string, right *SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeBinary,
		BinaryExpression: &BinaryExpression{
			Left:     left,
			Operator: operator,
			Right:    right,
		},
	}
}

// NewCaseExpression creates a new CASE expression (searched case)
func NewCaseExpression(whenClauses []*WhenClause, elseExpr *SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeCase,
		CaseExpression: &CaseExpression{
			WhenClauses: whenClauses,
			ElseExpr:    elseExpr,
		},
	}
}

// NewSimpleCaseExpression creates a new CASE expression with a case expression (simple case)
func NewSimpleCaseExpression(caseExpr *SQLExpression, whenClauses []*WhenClause, elseExpr *SQLExpression) *SQLExpression {
	return &SQLExpression{
		Type: ExpressionTypeCase,
		CaseExpression: &CaseExpression{
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

// DDL Statement types

type CreateTableStatement struct {
	IfNotExists bool
	TableName   string
	Columns     []*ColumnDefinition
	AsSelect    *SelectStatement
}

type ColumnDefinition struct {
	Name         string
	Type         string
	NotNull      bool
	DefaultValue *SQLExpression
	IsPrimaryKey bool
}

type CreateViewStatement struct {
	IfNotExists bool
	ViewName    string
	Query       SQLFragment
}

type CreateFunctionStatement struct {
	IfNotExists  bool
	FunctionName string
	Parameters   []*ParameterDefinition
	ReturnType   string
	Language     string
	Code         string
	Options      map[string]*SQLExpression
}

type ParameterDefinition struct {
	Name string
	Type string
}

type DeleteStatement struct {
	Table     SQLFragment
	WhereExpr SQLFragment
}

func (d *DeleteStatement) String() string {
	writer := NewSQLWriter()
	d.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

func (d *DeleteStatement) WriteSql(writer *SQLWriter) error {
	writer.Write("DELETE FROM ")
	d.Table.WriteSql(writer)
	if d.WhereExpr != nil {
		writer.Write(" WHERE ")
		writer.Write(d.WhereExpr.String())
	}
	return nil
}

type InsertStatement struct {
	TableName string
	Columns   []string
	Query     *SelectStatement
	Rows      []SQLFragment
}

func (d *InsertStatement) String() string {
	writer := NewSQLWriter()
	d.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

func (d *InsertStatement) WriteSql(writer *SQLWriter) error {
	writer.Write("INSERT INTO ")
	writer.WriteLine("`" + d.TableName + "`")
	writer.WriteLine(" (" + strings.Join(d.Columns, ", ") + ") ")
	if d.Query != nil {
		writer.Write(" ")
		d.Query.WriteSql(writer)
	} else if len(d.Rows) > 0 {
		writer.WriteLine("VALUES ")
		for i, value := range d.Rows {
			writer.Write("(" + value.String() + ")")
			if len(d.Rows) != 1 && i != len(d.Rows)-1 {
				writer.Write(",")
			}
		}
	} else {
		return fmt.Errorf("expected either Query or Rows in InsertStatement.WriteSql")
	}
	return nil
}

type DropStatement struct {
	IfExists   bool
	ObjectType string
	ObjectName string
}

type TruncateStatement struct {
	TableName string
}

type MergeStatement struct {
	TargetTable string
	SourceTable *FromItem
	MergeClause *SQLExpression
	WhenClauses []*MergeWhenClause
}

type MergeWhenClause struct {
	Type      string // "MATCHED", "NOT MATCHED"
	Condition *SQLExpression
	Action    string // "UPDATE", "DELETE", "INSERT"
	SetList   []*SetItem
}

type UpdateStatement struct {
	Table       *FromItem
	SetItems    []*SetItem
	FromClause  *FromItem
	WhereClause *SQLExpression
}

func (u *UpdateStatement) WriteSql(writer *SQLWriter) error {
	writer.Write("UPDATE ")
	u.Table.WriteSql(writer)
	writer.Write(" SET ")
	for i, item := range u.SetItems {
		if i > 0 {
			writer.Write(", ")
		}
		item.WriteSql(writer)
	}
	if u.FromClause != nil {
		writer.Write(" FROM ")
		u.FromClause.WriteSql(writer)
	}
	if u.WhereClause != nil {
		writer.Write(" WHERE ")
		u.WhereClause.WriteSql(writer)
	}
	return nil
}

func (u *UpdateStatement) String() string {
	writer := NewSQLWriter()
	u.WriteSql(writer)
	return strings.TrimSpace(writer.String())
}

type SetItem struct {
	Column *SQLExpression
	Value  *SQLExpression
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

type ScopeInfo struct {
	ResolvedColumns map[string]*ColumnInfo
}

// FragmentContext stores contextual information during AST traversal
type FragmentContext struct {
	// Current scope information
	TableAliases map[string]string
	CurrentScope *ScopeInfo

	// Symbol management
	AliasGenerator *AliasGenerator

	// Stack for nested contexts (subqueries, CTEs)
	scopeStack []*ScopeInfo

	WithEntries map[string]map[string]string

	// Testing instrumentation (optional)
	OnPushScope     func(scopeType string, stackDepth int)
	OnPopScope      func(alias string, stackDepth int)
	ResolvedColumns map[string]*ColumnInfo
}

// NodeID represents a unique identifier for AST nodes based on path in the AST
type NodeID string

// ColumnInfo stores metadata about available columns
type ColumnInfo struct {
	Name         string
	Type         string
	TableAlias   string
	Expression   *SQLExpression
	ID           int
	IsAggregated bool
	ColumnID     string `json:"column_id,omitempty"` // Full column identifier like "A.id#1"
}

func (i ColumnInfo) Clone() *ColumnInfo {
	return &ColumnInfo{
		Name:         i.Name,
		Type:         i.Type,
		TableAlias:   i.TableAlias,
		Expression:   i.Expression,
		ID:           i.ID,
		IsAggregated: i.IsAggregated,
	}
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
		TableAliases:    make(map[string]string),
		WithEntries:     make(map[string]map[string]string),
		ResolvedColumns: make(map[string]*ColumnInfo),
		AliasGenerator:  NewAliasGenerator(),
	}
}

func NewAliasGenerator() *AliasGenerator {
	return &AliasGenerator{
		usedAliases: make(map[string]bool),
	}
}

// Scope management methods

func (fc *FragmentContext) UseScope(scopeType string) func() {
	newScope := &ScopeInfo{
		ResolvedColumns: make(map[string]*ColumnInfo),
	}
	fc.scopeStack = append(fc.scopeStack, newScope)
	fc.CurrentScope = newScope
	return func() {
		fc.PopScope(scopeType)
	}
}

func (fc *FragmentContext) OpenScope(scopeType string, columns []*ast.Column) ScopeInfo {
	fc.PushScope(scopeType)
	for _, column := range columns {
		fc.AddAvailableColumn(column, &ColumnInfo{
			Name: column.Name(),
			Type: column.Type().Kind().String(),
		})
	}
	return *fc.CurrentScope
}

func (fc *FragmentContext) PushScope(scopeType string) {
	newScope := &ScopeInfo{
		ResolvedColumns: make(map[string]*ColumnInfo),
	}
	fc.scopeStack = append(fc.scopeStack, newScope)
	fc.CurrentScope = newScope

	if fc.OnPushScope != nil {
		fc.OnPushScope(scopeType, len(fc.scopeStack))
	}
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

	// Push resolved columns to the latest scope
	if currentScope != nil {
		for key, column := range currentScope.ResolvedColumns {
			column = column.Clone()
			// All references must use the generated table alias
			column.TableAlias = alias

			// Once a scope has been finalized, the column is no longer available to be inlined as a direct expression
			// It must be referenced as a column
			column.Expression = nil
			fc.ResolvedColumns[key] = column
		}
	}

	if fc.OnPopScope != nil {
		fc.OnPopScope(alias, len(fc.scopeStack))
	}

	return currentScope
}

// Column management methods
func (fc *FragmentContext) AddAvailableColumn(column *ast.Column, info *ColumnInfo) {
	name := GetUniqueColumnName(column)
	info.ID = column.ColumnID()
	info.Name = generateIDBasedAlias(info.Name, info.ID)
	info.TableAlias = name
	fc.ResolvedColumns[name] = info
	if fc.CurrentScope != nil {
		fc.CurrentScope.ResolvedColumns[name] = info
	}
}

func (fc *FragmentContext) GetColumnExpression(column *ast.Column) *SQLExpression {
	columnID := GetUniqueColumnName(column)
	columnInfo, exists := fc.ResolvedColumns[columnID]
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
		mapping[column.Name()] = generateIDBasedAlias(column.Name(), column.ColumnID())
	}
	fc.WithEntries[name] = mapping
}

func (fc *FragmentContext) FilterScope(scopeType string, list []*ast.Column) {
	scope := fc.PopScope(scopeType)
	previous := scope.ResolvedColumns
	scope.ResolvedColumns = make(map[string]*ColumnInfo)

	for _, column := range list {
		columnID := GetUniqueColumnName(column)
		if column, exists := previous[columnID]; exists {
			column.Expression = nil
			scope.ResolvedColumns[columnID] = column
		}
	}
	fc.PushScope(scopeType)
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

// ColumnMapping represents the mapping between original and new column names
type ColumnMapping struct {
	SourceColumnMap map[*SQLExpression]string // original column expression -> new column name for source table
	TargetColumnMap map[*SQLExpression]string // original column expression -> new column name for target table
	AllColumnMap    map[*SQLExpression]string // all original column names -> new column names
}

func (m ColumnMapping) LookupName(column *SQLExpression) (string, bool) {
	for expr, name := range m.AllColumnMap {
		if expr.String() == column.String() {
			return name, true
		}
	}
	return "", false
}

// CreateMergedTableStatement creates a CREATE TABLE AS SELECT statement using the merged table pattern
// for MERGE operations with distinct column naming. This generates the SQL pattern:
// CREATE TABLE tableName AS SELECT DISTINCT sourceCol1 AS merged_sourceCol1, targetCol1 AS merged_targetCol1, ... FROM (
//
//	SELECT * FROM sourceTable LEFT JOIN targetTable ON joinCondition
//	UNION ALL
//	SELECT * FROM targetTable LEFT JOIN sourceTable ON joinCondition
//
// )
// Returns the CreateTableStatement and a mapping of original -> new column names
func CreateMergedTableStatement(tableName string, sourceTable, targetTable *FromItem, joinCondition *SQLExpression) (*CreateTableStatement, *ColumnMapping, error) {
	// Extract column information from the fragments
	sourceColumns, err := extractColumnNames(sourceTable)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to extract source columns: %w", err)
	}

	targetColumns, err := extractColumnNames(targetTable)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to extract target columns: %w", err)
	}

	// Create distinct column mappings
	columnMapping := createColumnMapping(sourceColumns, targetColumns)

	// Create the inner subquery with LEFT JOIN and explicit column selection
	leftJoin := createJoinWithColumnMapping(&FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:      JoinTypeLeft,
			Left:      sourceTable,
			Right:     targetTable,
			Condition: joinCondition,
		},
	}, columnMapping)

	rightJoin := createJoinWithColumnMapping(&FromItem{
		Type: FromItemTypeJoin,
		Join: &JoinClause{
			Type:      JoinTypeLeft,
			Left:      targetTable,
			Right:     sourceTable,
			Condition: joinCondition,
		},
	}, columnMapping)

	// Create the UNION ALL operation
	unionOperation := &SetOperation{
		Type:     "UNION",
		Modifier: "ALL",
		Items:    []*SelectStatement{leftJoin, rightJoin},
	}

	// Create the outer subquery with UNION ALL
	unionStatement := NewSelectStatement()
	unionStatement.SetOperation = unionOperation

	// Create the final SELECT DISTINCT * from the UNION subquery
	distinctQuery := &SelectStatement{
		SelectType: SelectTypeDistinct,
		SelectList: []*SelectListItem{
			{Expression: NewStarExpression()},
		},
		FromClause: NewSubqueryFromItem(unionStatement, "merged_union"),
	}

	// Create the CREATE TABLE AS SELECT statement
	return &CreateTableStatement{
		TableName: tableName,
		AsSelect:  distinctQuery,
	}, columnMapping, nil
}

// extractColumnNames extracts column names from different SQLFragment types
func extractColumnNames(fragment SQLFragment) ([]*SQLExpression, error) {
	switch f := fragment.(type) {
	case *SelectStatement:
		var columns []*SQLExpression
		for _, item := range f.SelectList {
			columns = append(columns, item.Expression)
		}
		return columns, nil
	case *FromItem:
		// For subqueries, use the select list and re-alias to the FromItem alias
		if f.Type == FromItemTypeSubquery {
			mappings, err := extractColumnNames(f.Subquery)
			if err != nil {
				return nil, err
			}
			for key, expr := range mappings {
				mappings[key] = NewColumnExpression(expr.Value, f.Alias)
			}
			return mappings, nil
		}
		return nil, fmt.Errorf("unsupported fragment type: %T with type %T", f, f.Type)
	default:
		return nil, fmt.Errorf("unsupported fragment type: %T", f)
	}
}

// createColumnMapping creates distinct column names and mappings
func createColumnMapping(sourceColumns, targetColumns []*SQLExpression) *ColumnMapping {
	mapping := &ColumnMapping{
		SourceColumnMap: make(map[*SQLExpression]string),
		TargetColumnMap: make(map[*SQLExpression]string),
		AllColumnMap:    make(map[*SQLExpression]string),
	}

	usedNames := make(map[string]bool)

	// Process source columns
	for _, col := range sourceColumns {
		newName := fmt.Sprintf("merged_source_%s", col.Value)
		counter := 1
		originalNewName := newName

		// Ensure uniqueness
		for usedNames[newName] {
			newName = fmt.Sprintf("%s_%d", originalNewName, counter)
			counter++
		}

		usedNames[newName] = true
		mapping.SourceColumnMap[col] = newName
		mapping.AllColumnMap[col] = newName
	}

	// Process target columns
	for _, col := range targetColumns {
		newName := fmt.Sprintf("merged_target_%s", col.Value)
		counter := 1
		originalNewName := newName

		// Ensure uniqueness
		for usedNames[newName] {
			newName = fmt.Sprintf("%s_%d", originalNewName, counter)
			counter++
		}

		usedNames[newName] = true
		mapping.TargetColumnMap[col] = newName
		mapping.AllColumnMap[col] = newName
	}

	return mapping
}

// createJoinWithColumnMapping creates a SELECT statement with explicit column mapping for joins
func createJoinWithColumnMapping(joinFromItem *FromItem, mapping *ColumnMapping) *SelectStatement {
	stmt := NewSelectStatement()
	stmt.FromClause = joinFromItem

	// Build explicit SELECT list with column mappings
	stmt.SelectList = []*SelectListItem{}

	// Add source columns
	for origName, newName := range mapping.SourceColumnMap {
		stmt.SelectList = append(stmt.SelectList, &SelectListItem{
			Expression: origName,
			Alias:      newName,
		})
	}

	// Add target columns
	for origName, newName := range mapping.TargetColumnMap {
		stmt.SelectList = append(stmt.SelectList, &SelectListItem{
			Expression: origName,
			Alias:      newName,
		})
	}

	return stmt
}
