package internal

import (
	"github.com/vantaboard/go-googlesql/types"

	sqlexpr "github.com/vantaboard/go-googlesql-engine/internal/sqlexpr"
)

// SQL IR re-exports (canonical definitions in internal/sqlexpr).
type (
	SelectType              = sqlexpr.SelectType
	ExpressionType          = sqlexpr.ExpressionType
	FromItemType            = sqlexpr.FromItemType
	JoinType                = sqlexpr.JoinType
	SQLFragment             = sqlexpr.SQLFragment
	SQLWriter               = sqlexpr.SQLWriter
	DuckDBStructLiteralExpr = sqlexpr.DuckDBStructLiteralExpr
	DuckDBStructLiteralEntry = sqlexpr.DuckDBStructLiteralEntry
	CaseExpression          = sqlexpr.CaseExpression
	WhenClause              = sqlexpr.WhenClause
	ExistsExpression        = sqlexpr.ExistsExpression
	SQLCastSpec             = sqlexpr.SQLCastSpec
	ListExpression          = sqlexpr.ListExpression
	UnaryExpression         = sqlexpr.UnaryExpression
	BinaryExpression        = sqlexpr.BinaryExpression
	SQLExpression           = sqlexpr.SQLExpression
	FunctionCall            = sqlexpr.FunctionCall
	WindowSpecification     = sqlexpr.WindowSpecification
	FrameClause             = sqlexpr.FrameClause
	FrameBound              = sqlexpr.FrameBound
	SelectListItem          = sqlexpr.SelectListItem
	TableReference          = sqlexpr.TableReference
	FromItem                = sqlexpr.FromItem
	JoinClause              = sqlexpr.JoinClause
	TableFunction           = sqlexpr.TableFunction
	OrderByItem             = sqlexpr.OrderByItem
	WithClause              = sqlexpr.WithClause
	SetOperation            = sqlexpr.SetOperation
	SelectStatement         = sqlexpr.SelectStatement
	LimitClause             = sqlexpr.LimitClause
	CreateTableStatement    = sqlexpr.CreateTableStatement
	ColumnDefinition        = sqlexpr.ColumnDefinition
	CreateViewStatement     = sqlexpr.CreateViewStatement
	CreateFunctionStatement = sqlexpr.CreateFunctionStatement
	ParameterDefinition     = sqlexpr.ParameterDefinition
	DeleteStatement         = sqlexpr.DeleteStatement
	InsertStatement         = sqlexpr.InsertStatement
	DropStatement           = sqlexpr.DropStatement
	TruncateStatement       = sqlexpr.TruncateStatement
	UpdateStatement         = sqlexpr.UpdateStatement
	SetItem                 = sqlexpr.SetItem
	CompoundSQLFragment     = sqlexpr.CompoundSQLFragment
	CastMetadata            = sqlexpr.CastMetadata
)

const (
	SelectTypeStandard = sqlexpr.SelectTypeStandard
	SelectTypeDistinct = sqlexpr.SelectTypeDistinct
	SelectTypeAll      = sqlexpr.SelectTypeAll
	SelectTypeAsStruct = sqlexpr.SelectTypeAsStruct
	SelectTypeAsValue  = sqlexpr.SelectTypeAsValue
)

const (
	ExpressionTypeColumn              = sqlexpr.ExpressionTypeColumn
	ExpressionTypeLiteral             = sqlexpr.ExpressionTypeLiteral
	ExpressionTypeParameter           = sqlexpr.ExpressionTypeParameter
	ExpressionTypeFunction          = sqlexpr.ExpressionTypeFunction
	ExpressionTypeList              = sqlexpr.ExpressionTypeList
	ExpressionTypeUnary             = sqlexpr.ExpressionTypeUnary
	ExpressionTypeBinary            = sqlexpr.ExpressionTypeBinary
	ExpressionTypeSubquery          = sqlexpr.ExpressionTypeSubquery
	ExpressionTypeStar              = sqlexpr.ExpressionTypeStar
	ExpressionTypeCase              = sqlexpr.ExpressionTypeCase
	ExpressionTypeExists            = sqlexpr.ExpressionTypeExists
	ExpressionTypeCast              = sqlexpr.ExpressionTypeCast
	ExpressionTypeDuckDBStructLiteral = sqlexpr.ExpressionTypeDuckDBStructLiteral
)

const (
	FromItemTypeTable         = sqlexpr.FromItemTypeTable
	FromItemTypeSubquery      = sqlexpr.FromItemTypeSubquery
	FromItemTypeJoin          = sqlexpr.FromItemTypeJoin
	FromItemTypeWithRef       = sqlexpr.FromItemTypeWithRef
	FromItemTypeTableFunction = sqlexpr.FromItemTypeTableFunction
	FromItemTypeUnnest        = sqlexpr.FromItemTypeUnnest
	FromItemTypeSingleRow     = sqlexpr.FromItemTypeSingleRow
)

const (
	JoinTypeInner = sqlexpr.JoinTypeInner
	JoinTypeLeft  = sqlexpr.JoinTypeLeft
	JoinTypeRight = sqlexpr.JoinTypeRight
	JoinTypeFull  = sqlexpr.JoinTypeFull
	JoinTypeCross = sqlexpr.JoinTypeCross
)

var (
	NewSQLWriter                     = sqlexpr.NewSQLWriter
	NewSQLCastExpression             = sqlexpr.NewSQLCastExpression
	NewSelectStatement               = sqlexpr.NewSelectStatement
	NewSelectStarStatement           = sqlexpr.NewSelectStarStatement
	NewColumnExpression              = sqlexpr.NewColumnExpression
	NewStarExpression                = sqlexpr.NewStarExpression
	NewLiteralExpression             = sqlexpr.NewLiteralExpression
	NewFunctionExpression            = sqlexpr.NewFunctionExpression
	NewDuckDBStructLiteralExpression = sqlexpr.NewDuckDBStructLiteralExpression
	NewBinaryExpression              = sqlexpr.NewBinaryExpression
	NewCaseExpression                = sqlexpr.NewCaseExpression
	NewSimpleCaseExpression          = sqlexpr.NewSimpleCaseExpression
	NewSubqueryFromItem              = sqlexpr.NewSubqueryFromItem
	NewListExpression                = sqlexpr.NewListExpression
	NewNotExpression                 = sqlexpr.NewNotExpression
	NewExistsExpression              = sqlexpr.NewExistsExpression
	NewInnerJoin                     = sqlexpr.NewInnerJoin
	NewCompoundSQLFragment           = sqlexpr.NewCompoundSQLFragment
)

// NewLiteralExpressionFromGoValue encodes a Go value as a googlesqlengine wire literal.
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

type sqlDialectBridge struct {
	d Dialect
}

func (b sqlDialectBridge) ID() string {
	if b.d == nil {
		return "sqlite"
	}
	return b.d.ID()
}

func (b sqlDialectBridge) QuoteIdent(ident string) string {
	if b.d == nil {
		return sqlexpr.NilSQLDialect{}.QuoteIdent(ident)
	}
	return b.d.QuoteIdent(ident)
}

func (b sqlDialectBridge) FormatParameterPlaceholder(identifier string) string {
	return FormatParameterPlaceholder(b.d, identifier)
}

func (b sqlDialectBridge) EmitWireLiteral(w *sqlexpr.SQLWriter, val string) {
	if b.d != nil && b.d.DecodesGooglesqlWireLiteralsInSQL() {
		if sql, ok := duckDBNativeLiteralSQL(val); ok {
			w.Write(sql)
			return
		}
	}
	w.Write(val)
}

func (b sqlDialectBridge) NormalizeAggregate(f *sqlexpr.FunctionCall) (string, []*sqlexpr.SQLExpression, bool, bool, *sqlexpr.SQLExpression, []*sqlexpr.OrderByItem, *sqlexpr.SQLExpression) {
	return sqlexpr.DuckDBNormalizeAggregateCall(f, b)
}

// NewSQLWriterForDialect attaches internal.Dialect to a SQL writer.
func NewSQLWriterForDialect(d Dialect) *SQLWriter {
	if d == nil {
		return sqlexpr.NewSQLWriterForDialect(nil)
	}
	return sqlexpr.NewSQLWriterForDialect(sqlDialectBridge{d: d})
}

// SQLFragmentString renders a fragment with dialect-appropriate identifier quoting.
func SQLFragmentString(f SQLFragment, d Dialect) string {
	return sqlexpr.SQLFragmentString(f, sqlDialectBridge{d: d})
}
