package pureanalyzer

import (
	"fmt"
	"strings"
	"unicode"
)

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokInt
	tokString
	tokStar
	tokComma
	tokDot
	tokLParen
	tokRParen
	tokEq
	tokNe
	tokLt
	tokLe
	tokGt
	tokGe
	tokPlus
	tokMinus
	tokStarOp
	tokSlash
	tokAt
	tokQuestion
)

type token struct {
	kind tokenKind
	lit  string
	pos  int // byte offset in source
}

type lexer struct {
	s   string
	i   int
	pos int
}

func newLexer(s string) *lexer {
	return &lexer{s: strings.TrimSpace(s), i: 0, pos: 0}
}

func (l *lexer) next() (token, error) {
	l.skipSpace()
	if l.i >= len(l.s) {
		return token{kind: tokEOF, pos: l.i}, nil
	}
	l.pos = l.i
	ch := l.s[l.i]
	switch ch {
	case ',':
		l.i++
		return token{kind: tokComma, pos: l.pos}, nil
	case '.':
		l.i++
		return token{kind: tokDot, pos: l.pos}, nil
	case '(':
		l.i++
		return token{kind: tokLParen, pos: l.pos}, nil
	case ')':
		l.i++
		return token{kind: tokRParen, pos: l.pos}, nil
	case '*':
		l.i++
		return token{kind: tokStar, pos: l.pos}, nil
	case '+':
		l.i++
		return token{kind: tokPlus, pos: l.pos}, nil
	case '-':
		l.i++
		return token{kind: tokMinus, pos: l.pos}, nil
	case '/':
		l.i++
		return token{kind: tokSlash, pos: l.pos}, nil
	case '=':
		l.i++
		return token{kind: tokEq, pos: l.pos}, nil
	case '<':
		l.i++
		if l.i < len(l.s) && l.s[l.i] == '=' {
			l.i++
			return token{kind: tokLe, pos: l.pos}, nil
		}
		if l.i < len(l.s) && l.s[l.i] == '>' {
			l.i++
			return token{kind: tokNe, pos: l.pos}, nil
		}
		return token{kind: tokLt, pos: l.pos}, nil
	case '>':
		l.i++
		if l.i < len(l.s) && l.s[l.i] == '=' {
			l.i++
			return token{kind: tokGe, pos: l.pos}, nil
		}
		return token{kind: tokGt, pos: l.pos}, nil
	case '!':
		if l.i+1 < len(l.s) && l.s[l.i+1] == '=' {
			l.i += 2
			return token{kind: tokNe, pos: l.pos}, nil
		}
		return token{}, fmt.Errorf("pureanalyzer: unexpected %q at %d", ch, l.pos)
	case '@':
		l.i++
		return token{kind: tokAt, pos: l.pos}, nil
	case '?':
		l.i++
		return token{kind: tokQuestion, pos: l.pos}, nil
	case '\'':
		return l.lexString()
	}

	if unicode.IsDigit(rune(ch)) {
		return l.lexInt()
	}

	if isIdentStart(ch) {
		return l.lexIdentOrKeyword()
	}

	return token{}, fmt.Errorf("pureanalyzer: unexpected byte %q at %d", ch, l.pos)
}

func (l *lexer) skipSpace() {
	for l.i < len(l.s) {
		c := l.s[l.i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			l.i++
			continue
		}
		break
	}
}

func isIdentStart(b byte) bool {
	return b == '_' || unicode.IsLetter(rune(b))
}

func isIdentCont(b byte) bool {
	return b == '_' || unicode.IsLetter(rune(b)) || unicode.IsDigit(rune(b))
}

func (l *lexer) lexIdentOrKeyword() (token, error) {
	start := l.i
	for l.i < len(l.s) && isIdentCont(l.s[l.i]) {
		l.i++
	}
	return token{kind: tokIdent, lit: l.s[start:l.i], pos: start}, nil
}

func (l *lexer) lexInt() (token, error) {
	start := l.i
	for l.i < len(l.s) && unicode.IsDigit(rune(l.s[l.i])) {
		l.i++
	}
	return token{kind: tokInt, lit: l.s[start:l.i], pos: start}, nil
}

func (l *lexer) lexString() (token, error) {
	// opening '
	l.i++
	startContent := l.i
	var sb strings.Builder
	for l.i < len(l.s) {
		c := l.s[l.i]
		if c == '\'' {
			l.i++
			if l.i < len(l.s) && l.s[l.i] == '\'' {
				sb.WriteByte('\'')
				l.i++
				continue
			}
			return token{kind: tokString, lit: sb.String(), pos: startContent - 1}, nil
		}
		sb.WriteByte(c)
		l.i++
	}
	return token{}, fmt.Errorf("pureanalyzer: unterminated string at %d", startContent)
}
