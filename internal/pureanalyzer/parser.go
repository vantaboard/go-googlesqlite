package pureanalyzer

import (
	"fmt"
	"strings"
)

type parseSelect struct {
	star   bool
	cols   []string
	table  string
	where  *parseExpr
	source string
}

type parseExpr struct {
	// binary
	op    string // AND OR = < > <= >= != + - * /
	left  *parseExpr
	right *parseExpr
	// leaf
	litKind string // ident int string param_pos param_named
	lit     string
	// call
	fn   string
	args []*parseExpr
}

type parser struct {
	lex *lexer
	tok token
}

func parseSelectQuery(sql string) (*parseSelect, error) {
	p := &parser{lex: newLexer(sql)}
	if err := p.advance(); err != nil {
		return nil, err
	}
	if err := p.expectIdent("SELECT"); err != nil {
		return nil, err
	}
	sel := &parseSelect{source: strings.TrimSpace(sql)}
	if p.tok.kind == tokStar {
		sel.star = true
		if err := p.advance(); err != nil {
			return nil, err
		}
	} else {
		for {
			if p.tok.kind != tokIdent {
				return nil, fmt.Errorf("pureanalyzer: want column name or * got %v", p.tok.kind)
			}
			sel.cols = append(sel.cols, p.tok.lit)
			if err := p.advance(); err != nil {
				return nil, err
			}
			if p.tok.kind == tokComma {
				if err := p.advance(); err != nil {
					return nil, err
				}
				continue
			}
			break
		}
	}
	if err := p.expectIdent("FROM"); err != nil {
		return nil, err
	}
	if p.tok.kind != tokIdent {
		return nil, fmt.Errorf("pureanalyzer: want table name")
	}
	sel.table = p.tok.lit
	if err := p.advance(); err != nil {
		return nil, err
	}
	if p.tok.kind == tokEOF {
		return sel, nil
	}
	if err := p.expectIdent("WHERE"); err != nil {
		return nil, err
	}
	we, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	sel.where = we
	if p.tok.kind != tokEOF {
		return nil, fmt.Errorf("pureanalyzer: trailing input at %d", p.tok.pos)
	}
	return sel, nil
}

func (p *parser) advance() error {
	t, err := p.lex.next()
	if err != nil {
		return err
	}
	p.tok = t
	return nil
}

func (p *parser) expectIdent(want string) error {
	if p.tok.kind != tokIdent || !strings.EqualFold(p.tok.lit, want) {
		return fmt.Errorf("pureanalyzer: want %q got %v %q", want, p.tok.kind, p.tok.lit)
	}
	return p.advance()
}

func (p *parser) parseOr() (*parseExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.tok.kind == tokIdent && strings.EqualFold(p.tok.lit, "OR") {
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &parseExpr{op: "OR", left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (*parseExpr, error) {
	left, err := p.parseCmp()
	if err != nil {
		return nil, err
	}
	for p.tok.kind == tokIdent && strings.EqualFold(p.tok.lit, "AND") {
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseCmp()
		if err != nil {
			return nil, err
		}
		left = &parseExpr{op: "AND", left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseCmp() (*parseExpr, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	var op string
	switch p.tok.kind {
	case tokEq:
		op = "="
	case tokNe:
		op = "!="
	case tokLt:
		op = "<"
	case tokLe:
		op = "<="
	case tokGt:
		op = ">"
	case tokGe:
		op = ">="
	default:
		return left, nil
	}
	if err := p.advance(); err != nil {
		return nil, err
	}
	right, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	return &parseExpr{op: op, left: left, right: right}, nil
}

func (p *parser) parseAdditive() (*parseExpr, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}
	for p.tok.kind == tokPlus || p.tok.kind == tokMinus {
		op := "+"
		if p.tok.kind == tokMinus {
			op = "-"
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseMultiplicative()
		if err != nil {
			return nil, err
		}
		left = &parseExpr{op: op, left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseMultiplicative() (*parseExpr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.tok.kind == tokStar || p.tok.kind == tokSlash {
		op := "*"
		if p.tok.kind == tokSlash {
			op = "/"
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &parseExpr{op: op, left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseUnary() (*parseExpr, error) {
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (*parseExpr, error) {
	switch p.tok.kind {
	case tokIdent:
		// function call or bare ident
		name := p.tok.lit
		if err := p.advance(); err != nil {
			return nil, err
		}
		if p.tok.kind == tokLParen {
			if err := p.advance(); err != nil {
				return nil, err
			}
			var args []*parseExpr
			if p.tok.kind != tokRParen {
				for {
					arg, err := p.parseOr()
					if err != nil {
						return nil, err
					}
					args = append(args, arg)
					if p.tok.kind == tokComma {
						if err := p.advance(); err != nil {
							return nil, err
						}
						continue
					}
					break
				}
			}
			if p.tok.kind != tokRParen {
				return nil, fmt.Errorf("pureanalyzer: want )")
			}
			if err := p.advance(); err != nil {
				return nil, err
			}
			return &parseExpr{fn: strings.ToUpper(name), args: args}, nil
		}
		return &parseExpr{litKind: "ident", lit: name}, nil
	case tokInt:
		v := p.tok.lit
		if err := p.advance(); err != nil {
			return nil, err
		}
		return &parseExpr{litKind: "int", lit: v}, nil
	case tokString:
		v := p.tok.lit
		if err := p.advance(); err != nil {
			return nil, err
		}
		return &parseExpr{litKind: "string", lit: v}, nil
	case tokAt:
		if err := p.advance(); err != nil {
			return nil, err
		}
		if p.tok.kind != tokIdent {
			return nil, fmt.Errorf("pureanalyzer: want param name after @")
		}
		nm := strings.ToLower(p.tok.lit)
		if err := p.advance(); err != nil {
			return nil, err
		}
		return &parseExpr{litKind: "param_named", lit: nm}, nil
	case tokQuestion:
		if err := p.advance(); err != nil {
			return nil, err
		}
		return &parseExpr{litKind: "param_pos", lit: "1"}, nil
	case tokLParen:
		if err := p.advance(); err != nil {
			return nil, err
		}
		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.tok.kind != tokRParen {
			return nil, fmt.Errorf("pureanalyzer: want )")
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		return e, nil
	default:
		return nil, fmt.Errorf("%w: unexpected token %v", ErrUnsupportedFeature, p.tok.kind)
	}
}
