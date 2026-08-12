// Package godb is a persistent, transactional key-value store with an LSM-tree
// storage engine. This file defines the public API surface.
//
// Phase 1 provides an in-memory implementation backed by a sorted memtable.
// Later phases add the write-ahead log, on-disk SSTables, compaction, Bloom
// filters, and MVCC transactions behind this same API without changing it.
package godb

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ronitraj/godb/internal/keys"
	"github.com/ronitraj/godb/internal/memtable"
)

// ErrClosed is returned by operations on a closed database.
var ErrClosed = errors.New("godb: database is closed")

// DB is a handle to a GoDB key-value store.
type DB struct {
	mu     sync.RWMutex
	mem    *memtable.Memtable
	seq    atomic.Uint64
	closed atomic.Bool
}

// Open creates an in-memory GoDB instance. Phase 2 will extend this to open a
// directory, replay the write-ahead log, and load SSTables from disk.
func Open() *DB {
	return &DB{mem: memtable.New()}
}

// nextSeq allocates a globally unique, monotonically increasing sequence number.
func (db *DB) nextSeq() keys.SeqNum {
	return keys.SeqNum(db.seq.Add(1))
}

// Put stores value under key. An empty value is permitted; use Delete to remove
// a key.
func (db *DB) Put(key, value []byte) error {
	return db.write(key, value, keys.KindSet)
}

// Delete removes key. It is not an error to delete a key that does not exist;
// the operation records a tombstone.
func (db *DB) Delete(key []byte) error {
	return db.write(key, nil, keys.KindDelete)
}

func (db *DB) write(key, value []byte, kind keys.Kind) error {
	if db.closed.Load() {
		return ErrClosed
	}
	seq := db.nextSeq()
	// Copy caller-owned slices; the memtable retains them for the record's life.
	ik := keys.InternalKey{UserKey: clone(key), Seq: seq, Kind: kind}
	var v []byte
	if kind == keys.KindSet {
		v = clone(value)
	}
	db.mu.Lock()
	db.mem.Put(ik, v)
	db.mu.Unlock()
	return nil
}

// Get returns the current value for key. found is false if the key is absent or
// has been deleted.
func (db *DB) Get(key []byte) (value []byte, found bool, err error) {
	if db.closed.Load() {
		return nil, false, ErrClosed
	}
	snapshot := keys.SeqNum(db.seq.Load())
	db.mu.RLock()
	v, kind, ok := db.mem.Get(key, snapshot)
	db.mu.RUnlock()
	if !ok || kind == keys.KindDelete {
		return nil, false, nil
	}
	return clone(v), true, nil
}

// Scan returns an iterator over the current values whose keys lie in the range
// [start, end). A nil start scans from the first key; a nil end scans to the
// last. The iterator observes a consistent snapshot taken at the time of the
// call and yields at most one (the newest visible) value per user key, skipping
// deleted keys.
func (db *DB) Scan(start, end []byte) (*Iterator, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}
	snapshot := keys.SeqNum(db.seq.Load())
	db.mu.RLock()
	inner := db.mem.NewIterator(start, end)
	db.mu.RUnlock()
	return &Iterator{inner: inner, snapshot: snapshot}, nil
}

// Close releases the database. After Close, all operations return ErrClosed.
func (db *DB) Close() error {
	db.closed.Store(true)
	return nil
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// Iterator yields key/value pairs from a Scan in ascending key order.
type Iterator struct {
	inner       *memtable.Iterator
	snapshot    keys.SeqNum
	lastUserKey []byte
	key         []byte
	value       []byte
}

// Next advances the iterator, returning false when the range is exhausted.
func (it *Iterator) Next() bool {
	for it.inner.Next() {
		ik, err := keys.Decode(it.inner.Key())
		if err != nil {
			continue
		}
		// The memtable yields versions newest-first within a user key, so the
		// first version we see for a key that is visible to the snapshot is the
		// authoritative one; skip any older versions of the same key.
		if it.lastUserKey != nil && equal(ik.UserKey, it.lastUserKey) {
			continue
		}
		if ik.Seq > it.snapshot {
			// A version newer than our snapshot; ignore but do not yet mark the
			// key as seen, since an older visible version may follow.
			continue
		}
		it.lastUserKey = clone(ik.UserKey)
		if ik.Kind == keys.KindDelete {
			// Deleted at the snapshot; skip the whole key.
			continue
		}
		it.key = clone(ik.UserKey)
		it.value = clone(it.inner.Value())
		return true
	}
	return false
}

// Key returns the current user key.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current value.
func (it *Iterator) Value() []byte { return it.value }

// Close releases iterator resources. It is safe to call multiple times.
func (it *Iterator) Close() error { return nil }

func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
