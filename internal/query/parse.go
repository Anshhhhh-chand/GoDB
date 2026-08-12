// Package query implements a small SQL subset on top of the key-value engine:
// CREATE TABLE, CREATE INDEX, INSERT, SELECT ... WHERE, UPDATE, and DELETE.
// Rows are mapped onto the KV keyspace, and an indexed column gets a second
// keyspace so equality lookups on it avoid a full table scan.
//
// This file is the front end: a hand-written tokenizer and a recursive-descent
// parser producing the AST consumed by the executor.
package query

import (
	"fmt"
	"strings"
	"unicode"
)

// ---- AST ----

// Statement is any parsed SQL statement.
type Statement interface{ isStatement() }

// Column is a column definition in CREATE TABLE.
type Column struct {
	Name string
	Type string // "TEXT" or "INT"
}

// CreateTable is `CREATE TABLE t (col type, ..., PRIMARY KEY (col))`.
type CreateTable struct {
	Table   string
	Columns []Column
	PK      string
}

// CreateIndex is `CREATE INDEX ON t (col)`.
type CreateIndex struct {
	Table  string
	Column string
}

// Insert is `INSERT INTO t (cols...) VALUES (vals...)`.
type Insert struct {
	Table   string
	Columns []string
	Values  []string
}

// Predicate is a single equality `col = value`.
type Predicate struct {
	Column string
	Value  string
}

// Select is `SELECT cols FROM t [WHERE col = value]`.
type Select struct {
	Table   string
	Columns []string // ["*"] for all
	Where   *Predicate
}

// Update is `UPDATE t SET col = value [, ...] [WHERE col = value]`.
type Update struct {
	Table string
	Set   map[string]string
	Where *Predicate
}

// Delete is `DELETE FROM t [WHERE col = value]`.
type Delete struct {
	Table string
	Where *Predicate
}

func (CreateTable) isStatement() {}
func (CreateIndex) isStatement() {}
func (Insert) isStatement()      {}
func (Select) isStatement()      {}
func (Update) isStatement()      {}
func (Delete) isStatement()      {}

// ---- Tokenizer ----

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tString
	tNumber
	tPunct
)

type token struct {
	kind tokKind
	text string
}

func tokenize(sql string) ([]token, error) {
	var toks []token
	rs := []rune(sql)
	i := 0
	for i < len(rs) {
		c := rs[i]
		switch {
		case unicode.IsSpace(c):
			i++
		case c == '\'':
			// string literal
			i++
			start := i
			var sb strings.Builder
			for i < len(rs) && rs[i] != '\'' {
				sb.WriteRune(rs[i])
				i++
			}
			if i >= len(rs) {
				return nil, fmt.Errorf("unterminated string literal at %d", start)
			}
			i++ // closing quote
			toks = append(toks, token{tString, sb.String()})
		case unicode.IsLetter(c) || c == '_':
			start := i
			for i < len(rs) && (unicode.IsLetter(rs[i]) || unicode.IsDigit(rs[i]) || rs[i] == '_') {
				i++
			}
			toks = append(toks, token{tIdent, string(rs[start:i])})
		case unicode.IsDigit(c) || (c == '-' && i+1 < len(rs) && unicode.IsDigit(rs[i+1])):
			start := i
			i++
			for i < len(rs) && (unicode.IsDigit(rs[i]) || rs[i] == '.') {
				i++
			}
			toks = append(toks, token{tNumber, string(rs[start:i])})
		case strings.ContainsRune("(),=*;", c):
			toks = append(toks, token{tPunct, string(c)})
			i++
		default:
			return nil, fmt.Errorf("unexpected character %q", c)
		}
	}
	toks = append(toks, token{tEOF, ""})
	return toks, nil
}

// ---- Parser ----

type parser struct {
	toks []token
	pos  int
}

// Parse parses a single SQL statement.
func Parse(sql string) (Statement, error) {
	toks, err := tokenize(sql)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	// Allow an optional trailing semicolon.
	if p.peek().kind == tPunct && p.peek().text == ";" {
		p.next()
	}
	if p.peek().kind != tEOF {
		return nil, fmt.Errorf("unexpected trailing input near %q", p.peek().text)
	}
	return stmt, nil
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token { t := p.toks[p.pos]; p.pos++; return t }

func (p *parser) expectPunct(s string) error {
	t := p.next()
	if t.kind != tPunct || t.text != s {
		return fmt.Errorf("expected %q, got %q", s, t.text)
	}
	return nil
}

func (p *parser) expectKeyword(kw string) error {
	t := p.next()
	if t.kind != tIdent || !strings.EqualFold(t.text, kw) {
		return fmt.Errorf("expected keyword %s, got %q", kw, t.text)
	}
	return nil
}

func (p *parser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tIdent && strings.EqualFold(t.text, kw)
}

func (p *parser) parseIdent() (string, error) {
	t := p.next()
	if t.kind != tIdent {
		return "", fmt.Errorf("expected identifier, got %q", t.text)
	}
	return t.text, nil
}

// parseValue accepts a string, number, or bare identifier as a literal value.
func (p *parser) parseValue() (string, error) {
	t := p.next()
	switch t.kind {
	case tString, tNumber, tIdent:
		return t.text, nil
	default:
		return "", fmt.Errorf("expected a value, got %q", t.text)
	}
}

func (p *parser) parseStatement() (Statement, error) {
	switch {
	case p.isKeyword("CREATE"):
		p.next()
		if p.isKeyword("TABLE") {
			return p.parseCreateTable()
		}
		if p.isKeyword("INDEX") {
			return p.parseCreateIndex()
		}
		return nil, fmt.Errorf("expected TABLE or INDEX after CREATE")
	case p.isKeyword("INSERT"):
		return p.parseInsert()
	case p.isKeyword("SELECT"):
		return p.parseSelect()
	case p.isKeyword("UPDATE"):
		return p.parseUpdate()
	case p.isKeyword("DELETE"):
		return p.parseDelete()
	default:
		return nil, fmt.Errorf("unsupported statement starting with %q", p.peek().text)
	}
}

func (p *parser) parseCreateTable() (Statement, error) {
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	name, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	ct := &CreateTable{Table: name}
	for {
		if p.isKeyword("PRIMARY") {
			p.next()
			if err := p.expectKeyword("KEY"); err != nil {
				return nil, err
			}
			if err := p.expectPunct("("); err != nil {
				return nil, err
			}
			pk, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			ct.PK = pk
			if err := p.expectPunct(")"); err != nil {
				return nil, err
			}
		} else {
			col, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			typ, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			ct.Columns = append(ct.Columns, Column{Name: col, Type: strings.ToUpper(typ)})
		}
		if p.peek().text == "," {
			p.next()
			continue
		}
		break
	}
	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}
	if ct.PK == "" && len(ct.Columns) > 0 {
		ct.PK = ct.Columns[0].Name // default: first column is the primary key
	}
	return ct, nil
}

func (p *parser) parseCreateIndex() (Statement, error) {
	if err := p.expectKeyword("INDEX"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	col, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}
	return &CreateIndex{Table: table, Column: col}, nil
}

func (p *parser) parseInsert() (Statement, error) {
	if err := p.expectKeyword("INSERT"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	ins := &Insert{Table: table}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	for {
		col, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		ins.Columns = append(ins.Columns, col)
		if p.peek().text == "," {
			p.next()
			continue
		}
		break
	}
	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	for {
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		ins.Values = append(ins.Values, v)
		if p.peek().text == "," {
			p.next()
			continue
		}
		break
	}
	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}
	if len(ins.Columns) != len(ins.Values) {
		return nil, fmt.Errorf("INSERT column/value count mismatch: %d vs %d", len(ins.Columns), len(ins.Values))
	}
	return ins, nil
}

func (p *parser) parseSelect() (Statement, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	sel := &Select{}
	if p.peek().text == "*" {
		p.next()
		sel.Columns = []string{"*"}
	} else {
		for {
			col, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			sel.Columns = append(sel.Columns, col)
			if p.peek().text == "," {
				p.next()
				continue
			}
			break
		}
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	sel.Table = table
	if p.isKeyword("WHERE") {
		pred, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		sel.Where = pred
	}
	return sel, nil
}

func (p *parser) parseUpdate() (Statement, error) {
	if err := p.expectKeyword("UPDATE"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	upd := &Update{Table: table, Set: map[string]string{}}
	for {
		col, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expectPunct("="); err != nil {
			return nil, err
		}
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		upd.Set[col] = val
		if p.peek().text == "," {
			p.next()
			continue
		}
		break
	}
	if p.isKeyword("WHERE") {
		pred, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		upd.Where = pred
	}
	return upd, nil
}

func (p *parser) parseDelete() (Statement, error) {
	if err := p.expectKeyword("DELETE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	del := &Delete{Table: table}
	if p.isKeyword("WHERE") {
		pred, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		del.Where = pred
	}
	return del, nil
}

func (p *parser) parseWhere() (*Predicate, error) {
	if err := p.expectKeyword("WHERE"); err != nil {
		return nil, err
	}
	col, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectPunct("="); err != nil {
		return nil, err
	}
	val, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	return &Predicate{Column: col, Value: val}, nil
}
