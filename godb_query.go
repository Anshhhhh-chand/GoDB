package godb

import "github.com/ronitraj/godb/internal/query"

// QueryResult is the outcome of executing a SQL statement via Query.
type QueryResult = query.Result

// kvAdapter adapts *DB to the query layer's storage interface.
type kvAdapter struct{ db *DB }

func (a kvAdapter) Get(key []byte) ([]byte, bool, error) { return a.db.Get(key) }
func (a kvAdapter) Put(key, value []byte) error          { return a.db.Put(key, value) }
func (a kvAdapter) Delete(key []byte) error              { return a.db.Delete(key) }
func (a kvAdapter) Scan(start, end []byte) (query.Iterator, error) {
	return a.db.Scan(start, end)
}

// Query parses and executes a single SQL statement from GoDB's supported subset
// (CREATE TABLE, CREATE INDEX, INSERT, SELECT ... WHERE, UPDATE, DELETE) against
// the key-value engine.
func (db *DB) Query(sql string) (*QueryResult, error) {
	return query.Exec(kvAdapter{db}, sql)
}
