package pureanalyzer

// TypeKind is a coarse SQL type label for the engine-focused IR (not full ZetaSQL).
type TypeKind string

const (
	TypeUnknown   TypeKind = "UNKNOWN"
	TypeInt64     TypeKind = "INT64"
	TypeString    TypeKind = "STRING"
	TypeBool      TypeKind = "BOOL"
	TypeDouble    TypeKind = "DOUBLE"
	TypeBytes     TypeKind = "BYTES"
	TypeDate      TypeKind = "DATE"
	TypeTimestamp TypeKind = "TIMESTAMP"
)

// ParamRef is a query parameter reference in the pure IR.
type ParamRef struct {
	Name     string // lowercase name without @; empty for positional
	Position int    // 1-based when positional
}

// ExprKind identifies expression shape in the pure IR.
type ExprKind string

const (
	ExprColumn   ExprKind = "COLUMN"
	ExprLiteral  ExprKind = "LITERAL"
	ExprParam    ExprKind = "PARAM"
	ExprCall     ExprKind = "CALL"
	ExprBinaryOp ExprKind = "BINARY"
	ExprUnary    ExprKind = "UNARY"
	ExprStar     ExprKind = "STAR"
)

// Expr is a resolved expression in the pure subset.
type Expr struct {
	Kind     ExprKind
	Type     TypeKind
	Name     string // column name or function name
	Literal  string // canonical literal representation
	Op       string // binary/unary operator
	Children []*Expr
	Param    *ParamRef
}

// OutputColumn describes one visible output column.
type OutputColumn struct {
	Name string
	Type TypeKind
	Expr *Expr // projection expression; nil when STAR expands to table columns
}

// AnalyzedSelect is the minimal analyzed SELECT for the pure subset.
type AnalyzedSelect struct {
	Table      string
	TableAlias string
	OutputCols []OutputColumn
	Where      *Expr
	SelectStar bool
	RawSQL     string
}
