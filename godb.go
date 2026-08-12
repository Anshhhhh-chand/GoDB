// Package godb is a persistent, transactional key-value store with an LSM-tree
// storage engine: write-ahead-log durability, immutable SSTables with Bloom
// filters, background compaction, and MVCC snapshot-isolation transactions.
//
// This file is the public API. Everything below it is implemented in the
// internal packages: keys, memtable, wal, sstable, bloom, lsm, and txn.
package godb

import (
	"github.com/ronitraj/godb/internal/keys"
	"github.com/ronitraj/godb/internal/lsm"
	"github.com/ronitraj/godb/internal/txn"
)

// Options configures a database.
type Options struct {
	// Dir is the directory where the WAL, SSTables, and manifest live.
	Dir string
	// MemtableThreshold is the size in bytes at which the in-memory buffer is
	// flushed to an SSTable. Zero selects a default.
	MemtableThreshold int
	// L0CompactionTrigger is the number of level-0 SSTables that triggers a
	// compaction. Zero selects a default.
	L0CompactionTrigger int
	// SyncWrites fsyncs the WAL before acknowledging a write. Defaults to true
	// via Open; set explicitly through OpenWith.
	SyncWrites bool
}

// DB is a handle to a GoDB database.
type DB struct {
	e     *lsm.Engine
	store *txn.Store
}

// Iterator ranges over key/value pairs from DB.Scan.
type Iterator = lsm.ScanIterator

// Tx is a transaction with snapshot isolation. Obtain one from DB.Begin.
type Tx = txn.Txn

// TxIterator ranges over key/value pairs from Tx.Scan.
type TxIterator = txn.ScanIterator

// ErrConflict is returned by Tx.Commit on a write-write conflict.
var ErrConflict = txn.ErrConflict

// Open opens (or creates) a database in dir with durable, synchronous writes.
func Open(dir string) (*DB, error) {
	return OpenWith(Options{Dir: dir, SyncWrites: true})
}

// OpenWith opens a database with explicit options.
func OpenWith(opts Options) (*DB, error) {
	e, err := lsm.Open(lsm.Options{
		Dir:                 opts.Dir,
		MemtableThreshold:   opts.MemtableThreshold,
		L0CompactionTrigger: opts.L0CompactionTrigger,
		SyncWrites:          opts.SyncWrites,
	})
	if err != nil {
		return nil, err
	}
	return &DB{e: e, store: txn.New(e)}, nil
}

// Put stores value under key (auto-committed).
func (db *DB) Put(key, value []byte) error {
	_, err := db.e.Commit(lsm.NoSnapshot, []lsm.Mutation{{Key: key, Value: value, Kind: keys.KindSet}})
	return err
}

// Delete removes key (auto-committed). Deleting an absent key is not an error.
func (db *DB) Delete(key []byte) error {
	_, err := db.e.Commit(lsm.NoSnapshot, []lsm.Mutation{{Key: key, Kind: keys.KindDelete}})
	return err
}

// Get returns the current value for key. found is false if the key is absent or
// deleted.
func (db *DB) Get(key []byte) (value []byte, found bool, err error) {
	return db.e.Get(key, db.e.CurrentSeq())
}

// Scan returns an iterator over keys in [start, end) at a consistent snapshot.
// A nil start or end is unbounded on that side. The caller must Close it.
func (db *DB) Scan(start, end []byte) (*Iterator, error) {
	return db.e.Scan(start, end), nil
}

// Begin starts a transaction with snapshot isolation.
func (db *DB) Begin() *Tx { return db.store.Begin() }

// Close flushes state and releases resources.
func (db *DB) Close() error { return db.e.Close() }
