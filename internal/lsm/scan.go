package lsm

import (
	"bytes"
	"container/heap"

	"github.com/ronitraj/godb/internal/keys"
	"github.com/ronitraj/godb/internal/memtable"
	"github.com/ronitraj/godb/internal/sstable"
)

// maxSeqKey is the smallest internal key for a given user key (largest sequence
// number => inverted trailer of zero), used as a seek target so a scan starts
// at the newest version of the range's first key.
func seekTarget(userKey []byte) []byte {
	return keys.Encode(keys.InternalKey{UserKey: userKey, Seq: keys.SeqNum(^uint64(0)), Kind: keys.KindDelete})
}

// srcIter is the common shape of every input to a scan's merge: a memtable
// snapshot, an immutable memtable, or an SSTable.
type srcIter interface {
	valid() bool
	key() []byte
	value() []byte
	next()
}

// --- adapters ---

type memEntry struct{ k, v []byte }

type sliceIter struct {
	entries []memEntry
	i       int
}

func (s *sliceIter) valid() bool  { return s.i < len(s.entries) }
func (s *sliceIter) key() []byte  { return s.entries[s.i].k }
func (s *sliceIter) value() []byte { return s.entries[s.i].v }
func (s *sliceIter) next()        { s.i++ }

type memIterAdapter struct {
	it *memtable.Iterator
	ok bool
}

func newMemIterAdapter(m *memtable.Memtable, start, end []byte) *memIterAdapter {
	it := m.NewIterator(start, end)
	a := &memIterAdapter{it: it}
	a.ok = it.Next()
	return a
}

func (a *memIterAdapter) valid() bool  { return a.ok }
func (a *memIterAdapter) key() []byte  { return a.it.Key() }
func (a *memIterAdapter) value() []byte { return a.it.Value() }
func (a *memIterAdapter) next()        { a.ok = a.it.Next() }

type sstIterAdapter struct {
	it  *sstable.Iterator
	end []byte
}

func newSSTIterAdapter(r *sstable.Reader, start, end []byte) *sstIterAdapter {
	it := r.NewIterator()
	if start != nil {
		it.Seek(seekTarget(start))
	} else {
		it.SeekToFirst()
	}
	return &sstIterAdapter{it: it, end: end}
}

func (a *sstIterAdapter) valid() bool {
	if !a.it.Valid() {
		return false
	}
	if a.end != nil && bytes.Compare(keys.UserKey(a.it.Key()), a.end) >= 0 {
		return false
	}
	return true
}
func (a *sstIterAdapter) key() []byte  { return a.it.Key() }
func (a *sstIterAdapter) value() []byte { return a.it.Value() }
func (a *sstIterAdapter) next()        { a.it.Next() }

// --- heap over sources ---

type srcHeap []srcIter

func (h srcHeap) Len() int            { return len(h) }
func (h srcHeap) Less(i, j int) bool  { return keys.Compare(h[i].key(), h[j].key()) < 0 }
func (h srcHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *srcHeap) Push(x any)         { *h = append(*h, x.(srcIter)) }
func (h *srcHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ScanIterator yields the newest value per user key visible at a fixed
// snapshot, in ascending key order, across the whole engine.
type ScanIterator struct {
	e            *Engine
	snapshot     keys.SeqNum
	ownsSnapshot bool
	pinned       []*tableHandle
	h            srcHeap
	decided      []byte
	curKey       []byte
	curVal       []byte
	closed       bool
}

// Scan returns an iterator over keys in [start, end) at a fresh consistent
// snapshot. A nil start or end is unbounded on that side. The scan registers
// its snapshot so compaction preserves the versions it needs, and pins the
// SSTables it reads; the caller must Close it.
func (e *Engine) Scan(start, end []byte) *ScanIterator {
	snapshot := e.AcquireSnapshot()
	it := e.ScanSnapshot(snapshot, start, end)
	it.ownsSnapshot = true // Close releases the snapshot we acquired here
	return it
}

// ScanSnapshot is like Scan but reads at the caller-provided snapshot and does
// not acquire or release it — the caller owns the snapshot's lifetime (used by
// transactions that scan at their own read snapshot).
func (e *Engine) ScanSnapshot(snapshot keys.SeqNum, start, end []byte) *ScanIterator {
	e.mu.RLock()
	// Copy the active memtable's range: it is the only source still being
	// mutated, so a copy gives us a stable, race-free view.
	var entries []memEntry
	mit := e.mem.NewIterator(start, end)
	for mit.Next() {
		entries = append(entries,
			memEntry{k: append([]byte(nil), mit.Key()...), v: append([]byte(nil), mit.Value()...)})
	}
	imms := make([]*memtable.Memtable, len(e.imm))
	for i, im := range e.imm {
		imms[i] = im.mem // immutable once frozen: safe to read without copying
	}
	pinned := make([]*tableHandle, 0, len(e.tables))
	for _, t := range e.tables {
		t.acquire()
		pinned = append(pinned, t)
	}
	e.mu.RUnlock()

	var srcs []srcIter
	srcs = append(srcs, &sliceIter{entries: entries})
	for _, m := range imms {
		srcs = append(srcs, newMemIterAdapter(m, start, end))
	}
	for _, t := range pinned {
		srcs = append(srcs, newSSTIterAdapter(t.reader, start, end))
	}

	h := make(srcHeap, 0, len(srcs))
	for _, s := range srcs {
		if s.valid() {
			h = append(h, s)
		}
	}
	heap.Init(&h)

	return &ScanIterator{e: e, snapshot: snapshot, pinned: pinned, h: h}
}

// Snapshot returns the sequence number this iterator reads at.
func (s *ScanIterator) Snapshot() keys.SeqNum { return s.snapshot }

func (s *ScanIterator) advanceTop() {
	s.h[0].next()
	if s.h[0].valid() {
		heap.Fix(&s.h, 0)
	} else {
		heap.Pop(&s.h)
	}
}

// Next advances to the next visible key, returning false when the range is
// exhausted.
func (s *ScanIterator) Next() bool {
	for len(s.h) > 0 {
		ik, err := keys.Decode(s.h[0].key())
		if err != nil {
			s.advanceTop()
			continue
		}
		uk := ik.UserKey
		if s.decided != nil && bytes.Equal(uk, s.decided) {
			s.advanceTop()
			continue
		}
		s.decided = append([]byte(nil), uk...)

		var chosenVal []byte
		var chosenKind keys.Kind
		chosen := false
		for len(s.h) > 0 {
			jk, jerr := keys.Decode(s.h[0].key())
			if jerr != nil || !bytes.Equal(jk.UserKey, uk) {
				break
			}
			if !chosen && jk.Seq <= s.snapshot {
				chosenVal = append([]byte(nil), s.h[0].value()...)
				chosenKind = jk.Kind
				chosen = true
			}
			s.advanceTop()
		}
		if chosen && chosenKind != keys.KindDelete {
			s.curKey = s.decided
			s.curVal = chosenVal
			return true
		}
	}
	return false
}

// Key returns the current user key.
func (s *ScanIterator) Key() []byte { return s.curKey }

// Value returns the current value.
func (s *ScanIterator) Value() []byte { return s.curVal }

// Close releases the scan's snapshot and unpins its SSTables.
func (s *ScanIterator) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	for _, t := range s.pinned {
		t.release()
	}
	if s.ownsSnapshot {
		s.e.ReleaseSnapshot(s.snapshot)
	}
	return nil
}
