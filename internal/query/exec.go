package query

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Iterator is the range-scan cursor the executor needs from the store.
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Close() error
}

// KV is the storage interface the query layer runs on. godb.DB satisfies it
// via a thin adapter.
type KV interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Scan(start, end []byte) (Iterator, error)
}

// Result is the outcome of executing a statement.
type Result struct {
	Columns      []string   // populated for SELECT
	Rows         [][]string // populated for SELECT
	RowsAffected int        // populated for INSERT/UPDATE/DELETE
	Message      string     // populated for DDL
}

type schema struct {
	Columns []Column `json:"columns"`
	PK      string   `json:"pk"`
	Indexes []string `json:"indexes"`
}

func (s *schema) hasColumn(name string) bool {
	for _, c := range s.Columns {
		if c.Name == name {
			return true
		}
	}
	return false
}

func (s *schema) isIndexed(col string) bool {
	for _, c := range s.Indexes {
		if c == col {
			return true
		}
	}
	return false
}

func (s *schema) columnNames() []string {
	out := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		out[i] = c.Name
	}
	return out
}

// ---- key encoding ----

const (
	sep          = "\x00"
	schemaSpace  = "sql" + sep + "schema" + sep
	rowSpace     = "sql" + sep + "row" + sep
	indexSpace   = "sql" + sep + "idx" + sep
)

func schemaKey(table string) []byte { return []byte(schemaSpace + table) }
func rowPrefix(table string) string { return rowSpace + table + sep }
func rowKey(table, pk string) []byte { return []byte(rowPrefix(table) + pk) }
func indexValPrefix(table, col, val string) string {
	return indexSpace + table + sep + col + sep + val + sep
}
func indexKey(table, col, val, pk string) []byte {
	return []byte(indexValPrefix(table, col, val) + pk)
}

// successor returns the smallest key greater than every key with the given
// prefix, for use as an exclusive Scan upper bound.
func successor(prefix string) []byte {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			out := make([]byte, i+1)
			copy(out, b[:i+1])
			out[i]++
			return out
		}
	}
	return nil // prefix is all 0xff: unbounded
}

// Exec parses and executes a single SQL statement against kv.
func Exec(kv KV, sql string) (*Result, error) {
	stmt, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	switch s := stmt.(type) {
	case *CreateTable:
		return execCreateTable(kv, s)
	case *CreateIndex:
		return execCreateIndex(kv, s)
	case *Insert:
		return execInsert(kv, s)
	case *Select:
		return execSelect(kv, s)
	case *Update:
		return execUpdate(kv, s)
	case *Delete:
		return execDelete(kv, s)
	default:
		return nil, fmt.Errorf("unsupported statement")
	}
}

func loadSchema(kv KV, table string) (*schema, error) {
	b, found, err := kv.Get(schemaKey(table))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("no such table: %s", table)
	}
	var s schema
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func saveSchema(kv KV, table string, s *schema) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return kv.Put(schemaKey(table), b)
}

func execCreateTable(kv KV, s *CreateTable) (*Result, error) {
	if _, found, _ := kv.Get(schemaKey(s.Table)); found {
		return nil, fmt.Errorf("table already exists: %s", s.Table)
	}
	sc := &schema{Columns: s.Columns, PK: s.PK}
	if !sc.hasColumn(sc.PK) {
		return nil, fmt.Errorf("primary key %q is not a declared column", sc.PK)
	}
	if err := saveSchema(kv, s.Table, sc); err != nil {
		return nil, err
	}
	return &Result{Message: "CREATE TABLE " + s.Table}, nil
}

func execCreateIndex(kv KV, s *CreateIndex) (*Result, error) {
	sc, err := loadSchema(kv, s.Table)
	if err != nil {
		return nil, err
	}
	if !sc.hasColumn(s.Column) {
		return nil, fmt.Errorf("no such column: %s", s.Column)
	}
	if !sc.isIndexed(s.Column) {
		sc.Indexes = append(sc.Indexes, s.Column)
		if err := saveSchema(kv, s.Table, sc); err != nil {
			return nil, err
		}
	}
	// Backfill the index from existing rows.
	rows, err := scanAllRows(kv, s.Table)
	if err != nil {
		return nil, err
	}
	for pk, row := range rows {
		if v, ok := row[s.Column]; ok {
			if err := kv.Put(indexKey(s.Table, s.Column, v, pk), nil); err != nil {
				return nil, err
			}
		}
	}
	return &Result{Message: "CREATE INDEX ON " + s.Table + "(" + s.Column + ")"}, nil
}

func execInsert(kv KV, s *Insert) (*Result, error) {
	sc, err := loadSchema(kv, s.Table)
	if err != nil {
		return nil, err
	}
	row := map[string]string{}
	for i, col := range s.Columns {
		if !sc.hasColumn(col) {
			return nil, fmt.Errorf("no such column: %s", col)
		}
		row[col] = s.Values[i]
	}
	pk, ok := row[sc.PK]
	if !ok {
		return nil, fmt.Errorf("missing primary key column %q in INSERT", sc.PK)
	}
	if _, found, _ := kv.Get(rowKey(s.Table, pk)); found {
		return nil, fmt.Errorf("duplicate primary key: %s", pk)
	}
	if err := putRow(kv, s.Table, pk, row); err != nil {
		return nil, err
	}
	for _, col := range sc.Indexes {
		if v, ok := row[col]; ok {
			if err := kv.Put(indexKey(s.Table, col, v, pk), nil); err != nil {
				return nil, err
			}
		}
	}
	return &Result{RowsAffected: 1}, nil
}

func execSelect(kv KV, s *Select) (*Result, error) {
	sc, err := loadSchema(kv, s.Table)
	if err != nil {
		return nil, err
	}
	rows, err := matchingRows(kv, sc, s.Table, s.Where)
	if err != nil {
		return nil, err
	}

	cols := s.Columns
	if len(cols) == 1 && cols[0] == "*" {
		cols = sc.columnNames()
	} else {
		for _, c := range cols {
			if !sc.hasColumn(c) {
				return nil, fmt.Errorf("no such column: %s", c)
			}
		}
	}

	// Deterministic output ordering by primary key.
	pks := make([]string, 0, len(rows))
	for pk := range rows {
		pks = append(pks, pk)
	}
	sort.Strings(pks)

	res := &Result{Columns: cols}
	for _, pk := range pks {
		row := rows[pk]
		out := make([]string, len(cols))
		for i, c := range cols {
			out[i] = row[c]
		}
		res.Rows = append(res.Rows, out)
	}
	return res, nil
}

func execUpdate(kv KV, s *Update) (*Result, error) {
	sc, err := loadSchema(kv, s.Table)
	if err != nil {
		return nil, err
	}
	if _, changingPK := s.Set[sc.PK]; changingPK {
		return nil, fmt.Errorf("cannot UPDATE the primary key column %q", sc.PK)
	}
	for col := range s.Set {
		if !sc.hasColumn(col) {
			return nil, fmt.Errorf("no such column: %s", col)
		}
	}
	rows, err := matchingRows(kv, sc, s.Table, s.Where)
	if err != nil {
		return nil, err
	}
	affected := 0
	for pk, row := range rows {
		// Remove stale index entries for indexed columns that change.
		for _, col := range sc.Indexes {
			if _, ok := s.Set[col]; ok {
				if old, had := row[col]; had {
					kv.Delete(indexKey(s.Table, col, old, pk))
				}
			}
		}
		for col, val := range s.Set {
			row[col] = val
		}
		if err := putRow(kv, s.Table, pk, row); err != nil {
			return nil, err
		}
		for _, col := range sc.Indexes {
			if _, ok := s.Set[col]; ok {
				if err := kv.Put(indexKey(s.Table, col, row[col], pk), nil); err != nil {
					return nil, err
				}
			}
		}
		affected++
	}
	return &Result{RowsAffected: affected}, nil
}

func execDelete(kv KV, s *Delete) (*Result, error) {
	sc, err := loadSchema(kv, s.Table)
	if err != nil {
		return nil, err
	}
	rows, err := matchingRows(kv, sc, s.Table, s.Where)
	if err != nil {
		return nil, err
	}
	affected := 0
	for pk, row := range rows {
		for _, col := range sc.Indexes {
			if v, ok := row[col]; ok {
				kv.Delete(indexKey(s.Table, col, v, pk))
			}
		}
		if err := kv.Delete(rowKey(s.Table, pk)); err != nil {
			return nil, err
		}
		affected++
	}
	return &Result{RowsAffected: affected}, nil
}

// matchingRows returns pk -> row for the rows satisfying where (nil = all),
// using the primary key or a secondary index when possible and otherwise
// falling back to a full table scan.
func matchingRows(kv KV, sc *schema, table string, where *Predicate) (map[string]map[string]string, error) {
	if where == nil {
		return scanAllRows(kv, table)
	}
	if !sc.hasColumn(where.Column) {
		return nil, fmt.Errorf("no such column: %s", where.Column)
	}

	// Primary-key equality: a single point lookup.
	if where.Column == sc.PK {
		row, found, err := getRow(kv, table, where.Value)
		if err != nil || !found {
			return map[string]map[string]string{}, err
		}
		return map[string]map[string]string{where.Value: row}, nil
	}

	// Indexed column: scan just the matching index slice, then fetch rows.
	if sc.isIndexed(where.Column) {
		out := map[string]map[string]string{}
		prefix := indexValPrefix(table, where.Column, where.Value)
		it, err := kv.Scan([]byte(prefix), successor(prefix))
		if err != nil {
			return nil, err
		}
		defer it.Close()
		var pks []string
		for it.Next() {
			pks = append(pks, strings.TrimPrefix(string(it.Key()), prefix))
		}
		for _, pk := range pks {
			row, found, err := getRow(kv, table, pk)
			if err != nil {
				return nil, err
			}
			if found {
				out[pk] = row
			}
		}
		return out, nil
	}

	// Unindexed column: full scan with a filter.
	all, err := scanAllRows(kv, table)
	if err != nil {
		return nil, err
	}
	for pk, row := range all {
		if row[where.Column] != where.Value {
			delete(all, pk)
		}
	}
	return all, nil
}

func scanAllRows(kv KV, table string) (map[string]map[string]string, error) {
	prefix := rowPrefix(table)
	it, err := kv.Scan([]byte(prefix), successor(prefix))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := map[string]map[string]string{}
	for it.Next() {
		pk := strings.TrimPrefix(string(it.Key()), prefix)
		var row map[string]string
		if err := json.Unmarshal(it.Value(), &row); err != nil {
			return nil, err
		}
		out[pk] = row
	}
	return out, nil
}

func getRow(kv KV, table, pk string) (map[string]string, bool, error) {
	b, found, err := kv.Get(rowKey(table, pk))
	if err != nil || !found {
		return nil, found, err
	}
	var row map[string]string
	if err := json.Unmarshal(b, &row); err != nil {
		return nil, false, err
	}
	return row, true, nil
}

func putRow(kv KV, table, pk string, row map[string]string) error {
	b, err := json.Marshal(row)
	if err != nil {
		return err
	}
	return kv.Put(rowKey(table, pk), b)
}
