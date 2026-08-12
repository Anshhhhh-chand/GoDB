// Package memtable provides the sorted in-memory write buffer of the storage
// engine. It is implemented as a hand-written skip list keyed by the internal
// key encoding from package keys, so entries are ordered user-key ascending
// and newest-version-first within a user key.
//
// A Memtable is safe for concurrent readers and a single writer when guarded
// by the caller's lock; it performs no internal locking so the engine can
// coordinate flushes and reads with a single RWMutex.
package memtable

import (
	"bytes"
	"math/rand"

	"github.com/ronitraj/godb/internal/keys"
)

const (
	maxLevel    = 16   // supports ~4^16 entries before degrading
	probability = 0.25 // fraction of nodes promoted to the next level
)

type node struct {
	key   []byte // encoded internal key
	value []byte
	next  []*node
}

// Memtable is a sorted, in-memory collection of versioned records.
type Memtable struct {
	head   *node
	level  int
	rng    *rand.Rand
	length int
	bytes  int // approximate resident size, for flush thresholds
}

// New returns an empty Memtable. The seed makes level promotion deterministic
// per-instance, which keeps tests reproducible without a global lock on rand.
func New() *Memtable {
	return &Memtable{
		head:  &node{next: make([]*node, maxLevel)},
		level: 1,
		rng:   rand.New(rand.NewSource(0x1E3779B97F4A7C15)),
	}
}

// Len reports the number of records (including tombstones).
func (m *Memtable) Len() int { return m.length }

// Size reports the approximate number of resident key+value bytes. The engine
// compares this against a threshold to decide when to flush.
func (m *Memtable) Size() int { return m.bytes }

func (m *Memtable) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && m.rng.Float64() < probability {
		lvl++
	}
	return lvl
}

// Put inserts a versioned record. Because sequence numbers are globally unique,
// every Put creates a new node rather than overwriting; older versions remain
// visible to snapshots that predate this write.
func (m *Memtable) Put(ik keys.InternalKey, value []byte) {
	encoded := keys.Encode(ik)

	var update [maxLevel]*node
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && keys.Compare(x.next[i].key, encoded) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}

	lvl := m.randomLevel()
	if lvl > m.level {
		for i := m.level; i < lvl; i++ {
			update[i] = m.head
		}
		m.level = lvl
	}

	n := &node{key: encoded, value: value, next: make([]*node, lvl)}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	m.length++
	m.bytes += len(encoded) + len(value)
}

// Get returns the newest version of userKey whose sequence number is at or
// below snapshot. found reports whether any such record exists in this
// memtable; kind tells the caller whether that record is a value or a
// tombstone (a tombstone still terminates the engine's level search).
func (m *Memtable) Get(userKey []byte, snapshot keys.SeqNum) (value []byte, kind keys.Kind, found bool) {
	// Seek to the first internal key >= (userKey, snapshot). Higher sequence
	// numbers sort earlier, so this lands on the newest version visible to the
	// snapshot, if one exists for this user key. The target uses KindDelete
	// (the smallest kind byte) so that a record whose sequence number exactly
	// equals the snapshot — of either kind — sorts at or after the target and
	// is therefore found rather than skipped.
	target := keys.Encode(keys.InternalKey{UserKey: userKey, Seq: snapshot, Kind: keys.KindDelete})

	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && keys.Compare(x.next[i].key, target) < 0 {
			x = x.next[i]
		}
	}
	cand := x.next[0]
	if cand == nil {
		return nil, 0, false
	}
	ik, err := keys.Decode(cand.key)
	if err != nil || !equal(ik.UserKey, userKey) {
		return nil, 0, false
	}
	return cand.value, ik.Kind, true
}

// Iterator yields records in internal-key order.
type Iterator struct {
	cur *node
	end []byte // exclusive user-key upper bound; nil means unbounded
}

// NewIterator returns an iterator over records whose user key lies in
// [start, end). A nil start begins at the first record; a nil end is unbounded.
func (m *Memtable) NewIterator(start, end []byte) *Iterator {
	x := m.head
	if start != nil {
		target := keys.Encode(keys.InternalKey{UserKey: start, Seq: keys.SeqNum(^uint64(0)), Kind: keys.KindSet})
		for i := m.level - 1; i >= 0; i-- {
			for x.next[i] != nil && keys.Compare(x.next[i].key, target) < 0 {
				x = x.next[i]
			}
		}
	}
	return &Iterator{cur: x, end: end}
}

// Next advances to the following record, returning false at the end of range.
func (it *Iterator) Next() bool {
	if it.cur == nil {
		return false
	}
	it.cur = it.cur.next[0]
	if it.cur == nil {
		return false
	}
	if it.end != nil && bytes.Compare(keys.UserKey(it.cur.key), it.end) >= 0 {
		it.cur = nil
		return false
	}
	return true
}

// Key returns the encoded internal key at the current position.
func (it *Iterator) Key() []byte { return it.cur.key }

// Value returns the value at the current position.
func (it *Iterator) Value() []byte { return it.cur.value }

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
