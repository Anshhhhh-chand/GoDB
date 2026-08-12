// Package lsm implements the log-structured merge-tree storage engine that
// backs GoDB. It composes the pieces built in earlier phases — the sorted
// memtable, the write-ahead log, and immutable SSTables — and adds the moving
// parts that make them a database: crash recovery, background flushing,
// snapshot-aware compaction, and MVCC commit with first-committer-wins
// conflict detection.
//
// Durability: every commit is appended to the WAL and fsync'd before it is
// acknowledged. Concurrency: reads take a consistent sequence-number snapshot
// and never block writers; compaction keeps retired SSTables alive via
// reference counts until in-flight scans release them.
package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ronitraj/godb/internal/keys"
	"github.com/ronitraj/godb/internal/memtable"
	"github.com/ronitraj/godb/internal/sstable"
	"github.com/ronitraj/godb/internal/wal"
)

// ErrConflict is returned when a transaction's commit loses a write-write race
// (first-committer-wins).
var ErrConflict = errors.New("lsm: write-write conflict")

// NoSnapshot disables conflict checking for a commit (used by auto-commit
// single writes, which always win).
const NoSnapshot = keys.SeqNum(0)

// Options configures an Engine.
type Options struct {
	Dir                 string
	MemtableThreshold   int  // flush when the memtable exceeds this many bytes
	L0CompactionTrigger int  // compact once this many level-0 tables accumulate
	SyncWrites          bool // fsync the WAL before acknowledging (durability)
}

func (o Options) withDefaults() Options {
	if o.MemtableThreshold <= 0 {
		o.MemtableThreshold = 4 << 20 // 4 MiB
	}
	if o.L0CompactionTrigger <= 0 {
		o.L0CompactionTrigger = 4
	}
	return o
}

// Mutation is a single key change within a commit batch.
type Mutation struct {
	Key   []byte
	Value []byte
	Kind  keys.Kind
}

type immTable struct {
	mem    *memtable.Memtable
	walNum uint64
}

type tableHandle struct {
	num    uint64
	level  int
	reader *sstable.Reader
	refs   atomic.Int32 // 1 for the engine's ownership + 1 per in-flight scan
	dir    string
	retired atomic.Bool
}

func (t *tableHandle) acquire() { t.refs.Add(1) }

func (t *tableHandle) release() {
	if t.refs.Add(-1) == 0 {
		t.reader.Close()
		if t.retired.Load() {
			os.Remove(sstPath(t.dir, t.num))
		}
	}
}

// Engine is the storage engine.
type Engine struct {
	opts Options
	dir  string

	mu     sync.RWMutex // guards mem, imm, tables
	mem    *memtable.Memtable
	memWAL *wal.Writer
	memWALNum uint64
	imm    []*immTable
	tables []*tableHandle // read order: level ascending, then num descending

	seq         atomic.Uint64
	nextFileNum atomic.Uint64

	commitMu  sync.Mutex
	lastWrite map[string]keys.SeqNum // last committed seq per key, for conflict checks

	snapMu    sync.Mutex
	snapshots map[keys.SeqNum]int // active read snapshots, refcounted

	man   *manifest
	manMu sync.Mutex // serializes manifest rewrites (worker + Open)

	flushCh chan struct{}
	closeCh chan struct{}
	doneCh  chan struct{}
	closed  atomic.Bool
}

func sstPath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.sst", num))
}
func walPath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.wal", num))
}

// Open opens (or creates) an engine rooted at opts.Dir, recovering any durable
// state left by a previous run.
func Open(opts Options) (*Engine, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	man, err := loadManifest(opts.Dir)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		opts:      opts,
		dir:       opts.Dir,
		lastWrite: make(map[string]keys.SeqNum),
		snapshots: make(map[keys.SeqNum]int),
		man:       man,
		flushCh:   make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	e.seq.Store(man.LastSeq)
	e.nextFileNum.Store(man.NextFileNum)

	// Load the SSTables listed in the manifest.
	for _, tm := range man.Tables {
		r, err := sstable.Open(sstPath(opts.Dir, tm.Num))
		if err != nil {
			return nil, fmt.Errorf("open sstable %d: %w", tm.Num, err)
		}
		th := &tableHandle{num: tm.Num, level: tm.Level, reader: r, dir: opts.Dir}
		th.refs.Store(1)
		e.tables = append(e.tables, th)
	}
	e.sortTables()

	// Replay any WAL segments (unflushed writes) into a memtable, then make
	// that data durable as an SSTable before continuing, so recovery is
	// crash-safe and the WAL segments can be reclaimed.
	if err := e.recoverWALs(); err != nil {
		return nil, err
	}

	// Fresh memtable + WAL for new writes.
	e.mem = memtable.New()
	e.memWALNum = e.nextFileNum.Add(1) - 1
	w, err := wal.Create(walPath(opts.Dir, e.memWALNum))
	if err != nil {
		return nil, err
	}
	e.memWAL = w
	if err := e.persistManifest(); err != nil {
		return nil, err
	}

	go e.worker()
	return e, nil
}

func (e *Engine) recoverWALs() error {
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return err
	}
	var walNums []uint64
	for _, ent := range entries {
		var num uint64
		if n, _ := fmt.Sscanf(ent.Name(), "%06d.wal", &num); n == 1 {
			walNums = append(walNums, num)
		}
	}
	if len(walNums) == 0 {
		return nil
	}
	sort.Slice(walNums, func(i, j int) bool { return walNums[i] < walNums[j] })

	mem := memtable.New()
	maxSeq := e.seq.Load()
	for _, num := range walNums {
		err := wal.Replay(walPath(e.dir, num), func(r wal.Record) error {
			mem.Put(keys.InternalKey{UserKey: r.Key, Seq: r.Seq, Kind: r.Kind}, r.Value)
			if uint64(r.Seq) > maxSeq {
				maxSeq = uint64(r.Seq)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	e.seq.Store(maxSeq)

	if mem.Len() > 0 {
		num := e.nextFileNum.Add(1) - 1
		if err := e.flushMemToTable(mem, num); err != nil {
			return err
		}
		if err := e.persistManifest(); err != nil {
			return err
		}
	}
	// The recovered data is now durable in an SSTable; drop the old WAL segments.
	for _, num := range walNums {
		os.Remove(walPath(e.dir, num))
	}
	return nil
}

// --- snapshots -----------------------------------------------------------

// CurrentSeq returns the highest committed sequence number.
func (e *Engine) CurrentSeq() keys.SeqNum { return keys.SeqNum(e.seq.Load()) }

// AcquireSnapshot registers and returns a read snapshot at the current sequence
// number. Compaction will not discard versions a registered snapshot can see.
func (e *Engine) AcquireSnapshot() keys.SeqNum {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	s := keys.SeqNum(e.seq.Load())
	e.snapshots[s]++
	return s
}

// ReleaseSnapshot releases a snapshot previously acquired.
func (e *Engine) ReleaseSnapshot(s keys.SeqNum) {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	if e.snapshots[s] <= 1 {
		delete(e.snapshots, s)
	} else {
		e.snapshots[s]--
	}
}

func (e *Engine) oldestSnapshot() keys.SeqNum {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	oldest := keys.SeqNum(e.seq.Load())
	for s := range e.snapshots {
		if s < oldest {
			oldest = s
		}
	}
	return oldest
}

// --- writes --------------------------------------------------------------

// Commit atomically applies a batch of mutations. If readSeq is not NoSnapshot,
// it first verifies that none of the batch's keys were committed by another
// transaction after readSeq, returning ErrConflict otherwise. On success it
// returns the commit sequence number assigned to every mutation in the batch.
func (e *Engine) Commit(readSeq keys.SeqNum, batch []Mutation) (keys.SeqNum, error) {
	if e.closed.Load() {
		return 0, os.ErrClosed
	}
	e.commitMu.Lock()
	defer e.commitMu.Unlock()

	if readSeq != NoSnapshot {
		for _, m := range batch {
			if last, ok := e.lastWrite[string(m.Key)]; ok && last > readSeq {
				return 0, ErrConflict
			}
		}
	}

	seq := keys.SeqNum(e.seq.Add(1))

	for _, m := range batch {
		if err := e.memWAL.Append(wal.Record{Seq: seq, Kind: m.Kind, Key: m.Key, Value: m.Value}); err != nil {
			return 0, err
		}
	}
	if e.opts.SyncWrites {
		if err := e.memWAL.Sync(); err != nil {
			return 0, err
		}
	}

	e.mu.Lock()
	for _, m := range batch {
		e.mem.Put(keys.InternalKey{UserKey: m.Key, Seq: seq, Kind: m.Kind}, m.Value)
		e.lastWrite[string(m.Key)] = seq
	}
	overThreshold := e.mem.Size() >= e.opts.MemtableThreshold
	if overThreshold {
		e.rotateMemtableLocked()
	}
	e.mu.Unlock()

	if overThreshold {
		e.signalFlush()
	}
	return seq, nil
}

// rotateMemtableLocked freezes the active memtable and starts a fresh one. It
// must be called with e.mu held.
func (e *Engine) rotateMemtableLocked() {
	e.imm = append(e.imm, &immTable{mem: e.mem, walNum: e.memWALNum})

	newNum := e.nextFileNum.Add(1) - 1
	w, err := wal.Create(walPath(e.dir, newNum))
	if err != nil {
		// If we cannot open a new WAL, keep writing to the old memtable rather
		// than losing durability; undo the freeze.
		e.imm = e.imm[:len(e.imm)-1]
		return
	}
	e.mem = memtable.New()
	e.memWAL = w
	e.memWALNum = newNum
}

func (e *Engine) signalFlush() {
	select {
	case e.flushCh <- struct{}{}:
	default:
	}
}

// --- reads ---------------------------------------------------------------

// Get returns the newest value for key visible at snapshot. found is false if
// the key is absent or deleted at that snapshot.
func (e *Engine) Get(key []byte, snapshot keys.SeqNum) (value []byte, found bool, err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if v, kind, ok := e.mem.Get(key, snapshot); ok {
		return visible(v, kind)
	}
	for i := len(e.imm) - 1; i >= 0; i-- {
		if v, kind, ok := e.imm[i].mem.Get(key, snapshot); ok {
			return visible(v, kind)
		}
	}
	for _, t := range e.tables {
		v, kind, ok, err := t.reader.Get(key, snapshot)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return visible(v, kind)
		}
	}
	return nil, false, nil
}

func visible(v []byte, kind keys.Kind) ([]byte, bool, error) {
	if kind == keys.KindDelete {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

// --- flush & compaction worker ------------------------------------------

func (e *Engine) worker() {
	defer close(e.doneCh)
	for {
		select {
		case <-e.closeCh:
			e.drainFlush()
			return
		case <-e.flushCh:
			e.drainFlush()
			e.maybeCompact()
		}
	}
}

func (e *Engine) drainFlush() {
	for {
		e.mu.RLock()
		if len(e.imm) == 0 {
			e.mu.RUnlock()
			return
		}
		im := e.imm[0]
		e.mu.RUnlock()

		num := e.nextFileNum.Add(1) - 1
		if err := e.flushMemToTable(im.mem, num); err != nil {
			// Best-effort: leave the immutable in place to retry on next signal.
			return
		}
		if err := e.persistManifest(); err != nil {
			return
		}

		e.mu.Lock()
		if len(e.imm) > 0 && e.imm[0] == im {
			e.imm = e.imm[1:]
		}
		e.mu.Unlock()
		os.Remove(walPath(e.dir, im.walNum))
	}
}

// flushMemToTable writes mem to a new level-0 SSTable numbered num, records it
// in the in-memory manifest, and installs its reader. The caller persists the
// manifest and reclaims the WAL.
func (e *Engine) flushMemToTable(mem *memtable.Memtable, num uint64) error {
	path := sstPath(e.dir, num)
	w, err := sstable.NewWriter(path)
	if err != nil {
		return err
	}
	it := mem.NewIterator(nil, nil)
	for it.Next() {
		if err := w.Add(it.Key(), it.Value()); err != nil {
			w.Finish()
			return err
		}
	}
	if err := w.Finish(); err != nil {
		return err
	}

	r, err := sstable.Open(path)
	if err != nil {
		return err
	}
	th := &tableHandle{num: num, level: 0, reader: r, dir: e.dir}
	th.refs.Store(1)

	e.manMu.Lock()
	e.man.Tables = append(e.man.Tables, tableMeta{Num: num, Level: 0})
	e.manMu.Unlock()

	e.mu.Lock()
	e.tables = append(e.tables, th)
	e.sortTables()
	e.mu.Unlock()
	return nil
}

func (e *Engine) maybeCompact() {
	e.mu.RLock()
	l0 := 0
	for _, t := range e.tables {
		if t.level == 0 {
			l0++
		}
	}
	if l0 < e.opts.L0CompactionTrigger {
		e.mu.RUnlock()
		return
	}
	// Snapshot the full table set and pin the readers for the merge.
	victims := make([]*tableHandle, len(e.tables))
	copy(victims, e.tables)
	for _, t := range victims {
		t.acquire()
	}
	e.mu.RUnlock()

	err := e.compact(victims)
	for _, t := range victims {
		t.release()
	}
	if err != nil {
		return
	}
}

// compact merges victims into a single level-1 SSTable, dropping versions that
// no active snapshot can observe and tombstones that shadow nothing.
func (e *Engine) compact(victims []*tableHandle) error {
	keepThreshold := e.oldestSnapshot()

	iters := make([]*sstable.Iterator, 0, len(victims))
	for _, t := range victims {
		iters = append(iters, t.reader.NewIterator())
	}
	merged := newMergeIterator(iters)

	num := e.nextFileNum.Add(1) - 1
	path := sstPath(e.dir, num)
	w, err := sstable.NewWriter(path)
	if err != nil {
		return err
	}

	var curKey []byte
	pastCutoff := false
	for merged.Valid() {
		ik, derr := keys.Decode(merged.Key())
		if derr != nil {
			w.Finish()
			return derr
		}
		if curKey == nil || !bytesEqual(ik.UserKey, curKey) {
			curKey = append([]byte(nil), ik.UserKey...)
			pastCutoff = false
		}
		switch {
		case pastCutoff:
			// A visible version for this key was already emitted; drop the rest.
		case ik.Seq > keepThreshold:
			// Newer than any snapshot boundary: a live reader may still need it.
			if err := w.Add(merged.Key(), merged.Value()); err != nil {
				w.Finish()
				return err
			}
		default:
			// First version at/below the oldest snapshot: this is the last one
			// any snapshot can see. Emit it unless it is a tombstone (which then
			// shadows nothing and can be dropped entirely).
			pastCutoff = true
			if ik.Kind != keys.KindDelete {
				if err := w.Add(merged.Key(), merged.Value()); err != nil {
					w.Finish()
					return err
				}
			}
		}
		merged.Next()
	}
	if err := w.Finish(); err != nil {
		return err
	}

	r, err := sstable.Open(path)
	if err != nil {
		return err
	}
	newTable := &tableHandle{num: num, level: 1, reader: r, dir: e.dir}
	newTable.refs.Store(1)

	// Update the manifest to replace victims with the merged table.
	victimNums := make(map[uint64]bool, len(victims))
	for _, t := range victims {
		victimNums[t.num] = true
	}
	e.manMu.Lock()
	kept := e.man.Tables[:0]
	for _, tm := range e.man.Tables {
		if !victimNums[tm.Num] {
			kept = append(kept, tm)
		}
	}
	e.man.Tables = append(kept, tableMeta{Num: num, Level: 1})
	e.manMu.Unlock()
	if err := e.persistManifest(); err != nil {
		return err
	}

	// Swap the in-memory table set and retire the victims.
	e.mu.Lock()
	newSet := make([]*tableHandle, 0, len(e.tables))
	for _, t := range e.tables {
		if !victimNums[t.num] {
			newSet = append(newSet, t)
		}
	}
	newSet = append(newSet, newTable)
	e.tables = newSet
	e.sortTables()
	e.mu.Unlock()

	for _, t := range victims {
		t.retired.Store(true)
		t.release() // drop the engine's ownership ref; file removed at refs==0
	}
	return nil
}

func (e *Engine) persistManifest() error {
	e.manMu.Lock()
	defer e.manMu.Unlock()
	e.man.NextFileNum = e.nextFileNum.Load()
	e.man.LastSeq = e.seq.Load()
	return writeManifest(e.dir, e.man)
}

// sortTables orders tables for reads: level ascending, then file number
// descending (newer data first). Must be called with e.mu held for writing.
func (e *Engine) sortTables() {
	sort.SliceStable(e.tables, func(i, j int) bool {
		if e.tables[i].level != e.tables[j].level {
			return e.tables[i].level < e.tables[j].level
		}
		return e.tables[i].num > e.tables[j].num
	})
}

// Close flushes outstanding state, stops the worker, and releases resources.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(e.closeCh)
	<-e.doneCh

	if err := e.memWAL.Close(); err != nil {
		return err
	}
	if err := e.persistManifest(); err != nil {
		return err
	}
	e.mu.Lock()
	for _, t := range e.tables {
		t.reader.Close()
	}
	e.mu.Unlock()
	return nil
}

func bytesEqual(a, b []byte) bool {
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
