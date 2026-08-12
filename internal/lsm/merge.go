package lsm

import (
	"container/heap"

	"github.com/ronitraj/godb/internal/keys"
	"github.com/ronitraj/godb/internal/sstable"
)

// mergeIterator performs an ordered k-way merge over several SSTable iterators,
// yielding records in ascending internal-key order. It is the engine used by
// compaction to combine overlapping tables into one.
type mergeIterator struct {
	h mergeHeap
}

type mergeHeap []*sstable.Iterator

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	return keys.Compare(h[i].Key(), h[j].Key()) < 0
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(*sstable.Iterator)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// newMergeIterator seeds the merge from each iterator's first record.
func newMergeIterator(iters []*sstable.Iterator) *mergeIterator {
	h := make(mergeHeap, 0, len(iters))
	for _, it := range iters {
		it.SeekToFirst()
		if it.Valid() {
			h = append(h, it)
		}
	}
	heap.Init(&h)
	return &mergeIterator{h: h}
}

// Valid reports whether a current record is available.
func (m *mergeIterator) Valid() bool { return len(m.h) > 0 }

// Key returns the current smallest internal key across all inputs.
func (m *mergeIterator) Key() []byte { return m.h[0].Key() }

// Value returns the value paired with the current key.
func (m *mergeIterator) Value() []byte { return m.h[0].Value() }

// Next advances the input that produced the current record and restores the
// heap invariant.
func (m *mergeIterator) Next() {
	top := m.h[0]
	if top.Next() {
		heap.Fix(&m.h, 0)
	} else {
		heap.Pop(&m.h)
	}
}
