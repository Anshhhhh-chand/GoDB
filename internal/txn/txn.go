// Package txn implements MVCC transactions with snapshot isolation on top of
// the storage engine. A transaction reads from a fixed snapshot taken at Begin
// (so it never blocks writers and sees a consistent view), buffers its own
// writes for read-your-writes semantics, and on Commit is checked for
// write-write conflicts under first-committer-wins.
package txn

import (
	"errors"
	"sort"

	"github.com/ronitraj/godb/internal/keys"
	"github.com/ronitraj/godb/internal/lsm"
)

// ErrConflict is returned by Commit when another transaction committed a
// conflicting write first.
var ErrConflict = lsm.ErrConflict

// ErrDone is returned when a transaction is used after Commit or Rollback.
var ErrDone = errors.New("txn: transaction already finished")

// Store creates transactions over an engine.
type Store struct {
	e *lsm.Engine
}

// New returns a transaction store for the engine.
func New(e *lsm.Engine) *Store { return &Store{e: e} }

// Engine exposes the underlying engine (for auto-commit fast paths).
func (s *Store) Engine() *lsm.Engine { return s.e }

type writeOp struct {
	value []byte
	kind  keys.Kind
}

// Txn is an in-progress transaction.
type Txn struct {
	store   *Store
	readSeq keys.SeqNum
	writes  map[string]writeOp
	done    bool
}

// Begin starts a transaction reading at the current snapshot.
func (s *Store) Begin() *Txn {
	return &Txn{
		store:   s,
		readSeq: s.e.AcquireSnapshot(),
		writes:  make(map[string]writeOp),
	}
}

// Get returns the value for key, honoring the transaction's own uncommitted
// writes (read-your-writes) before falling back to its read snapshot.
func (t *Txn) Get(key []byte) ([]byte, bool, error) {
	if t.done {
		return nil, false, ErrDone
	}
	if w, ok := t.writes[string(key)]; ok {
		if w.kind == keys.KindDelete {
			return nil, false, nil
		}
		return append([]byte(nil), w.value...), true, nil
	}
	return t.store.e.Get(key, t.readSeq)
}

// Put buffers a write.
func (t *Txn) Put(key, value []byte) error {
	if t.done {
		return ErrDone
	}
	t.writes[string(key)] = writeOp{value: append([]byte(nil), value...), kind: keys.KindSet}
	return nil
}

// Delete buffers a tombstone.
func (t *Txn) Delete(key []byte) error {
	if t.done {
		return ErrDone
	}
	t.writes[string(key)] = writeOp{kind: keys.KindDelete}
	return nil
}

// Commit checks for write-write conflicts and, if none, atomically applies the
// buffered writes. A read-only transaction commits trivially. After Commit the
// transaction is finished.
func (t *Txn) Commit() error {
	if t.done {
		return ErrDone
	}
	defer t.finish()

	if len(t.writes) == 0 {
		return nil
	}
	batch := make([]lsm.Mutation, 0, len(t.writes))
	for k, w := range t.writes {
		batch = append(batch, lsm.Mutation{Key: []byte(k), Value: w.value, Kind: w.kind})
	}
	_, err := t.store.e.Commit(t.readSeq, batch)
	return err
}

// Rollback discards the transaction's buffered writes.
func (t *Txn) Rollback() error {
	if t.done {
		return ErrDone
	}
	t.finish()
	return nil
}

func (t *Txn) finish() {
	t.done = true
	t.store.e.ReleaseSnapshot(t.readSeq)
	t.writes = nil
}

// Scan iterates keys in [start, end) at the transaction's snapshot, overlaying
// the transaction's own buffered writes (which take precedence and may hide
// keys via tombstones).
func (t *Txn) Scan(start, end []byte) (*ScanIterator, error) {
	if t.done {
		return nil, ErrDone
	}
	// Collect buffered writes within range, sorted by key.
	var buffered []bufEntry
	for k, w := range t.writes {
		if inRange([]byte(k), start, end) {
			buffered = append(buffered, bufEntry{key: []byte(k), op: w})
		}
	}
	sort.Slice(buffered, func(i, j int) bool { return string(buffered[i].key) < string(buffered[j].key) })

	base := t.store.e.ScanSnapshot(t.readSeq, start, end)
	return &ScanIterator{base: base, buffered: buffered}, nil
}

func inRange(k, start, end []byte) bool {
	if start != nil && string(k) < string(start) {
		return false
	}
	if end != nil && string(k) >= string(end) {
		return false
	}
	return true
}

type bufEntry struct {
	key []byte
	op  writeOp
}

// ScanIterator overlays a transaction's buffered writes on top of an engine
// snapshot scan.
type ScanIterator struct {
	base     *lsm.ScanIterator
	buffered []bufEntry
	bi       int

	baseValid bool
	baseKey   []byte
	baseVal   []byte

	curKey []byte
	curVal []byte
	started bool
}

func (s *ScanIterator) advanceBase() {
	s.baseValid = s.base.Next()
	if s.baseValid {
		s.baseKey = s.base.Key()
		s.baseVal = s.base.Value()
	}
}

// Next advances the merged iterator.
func (s *ScanIterator) Next() bool {
	if !s.started {
		s.started = true
		s.advanceBase()
	}
	for {
		var bufKey []byte
		if s.bi < len(s.buffered) {
			bufKey = s.buffered[s.bi].key
		}

		switch {
		case !s.baseValid && bufKey == nil:
			return false

		case bufKey == nil || (s.baseValid && string(s.baseKey) < string(bufKey)):
			// Base-only key.
			s.curKey = append([]byte(nil), s.baseKey...)
			s.curVal = append([]byte(nil), s.baseVal...)
			s.advanceBase()
			return true

		case !s.baseValid || string(bufKey) < string(s.baseKey):
			// Buffered-only key.
			e := s.buffered[s.bi]
			s.bi++
			if e.op.kind == keys.KindDelete {
				continue
			}
			s.curKey = append([]byte(nil), e.key...)
			s.curVal = append([]byte(nil), e.op.value...)
			return true

		default:
			// Same key in both: the buffered write wins.
			e := s.buffered[s.bi]
			s.bi++
			s.advanceBase()
			if e.op.kind == keys.KindDelete {
				continue
			}
			s.curKey = append([]byte(nil), e.key...)
			s.curVal = append([]byte(nil), e.op.value...)
			return true
		}
	}
}

// Key returns the current user key.
func (s *ScanIterator) Key() []byte { return s.curKey }

// Value returns the current value.
func (s *ScanIterator) Value() []byte { return s.curVal }

// Close releases the underlying snapshot scan.
func (s *ScanIterator) Close() error { return s.base.Close() }
