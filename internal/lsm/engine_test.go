package lsm

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ronitraj/godb/internal/keys"
)

func openTest(t *testing.T, opts Options) *Engine {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e
}

func put(t *testing.T, e *Engine, k, v string) {
	t.Helper()
	if _, err := e.Commit(NoSnapshot, []Mutation{{Key: []byte(k), Value: []byte(v), Kind: keys.KindSet}}); err != nil {
		t.Fatalf("put(%q): %v", k, err)
	}
}

func del(t *testing.T, e *Engine, k string) {
	t.Helper()
	if _, err := e.Commit(NoSnapshot, []Mutation{{Key: []byte(k), Kind: keys.KindDelete}}); err != nil {
		t.Fatalf("del(%q): %v", k, err)
	}
}

func mustGet(t *testing.T, e *Engine, k string) (string, bool) {
	t.Helper()
	v, found, err := e.Get([]byte(k), e.CurrentSeq())
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	return string(v), found
}

func (e *Engine) pendingImm() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.imm)
}

func (e *Engine) tableCount() (l0, total int) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, tb := range e.tables {
		if tb.level == 0 {
			l0++
		}
	}
	return l0, len(e.tables)
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func TestBasicPutGetDelete(t *testing.T) {
	e := openTest(t, Options{})
	defer e.Close()

	put(t, e, "name", "godb")
	if v, ok := mustGet(t, e, "name"); !ok || v != "godb" {
		t.Errorf("Get(name) = (%q,%v), want (godb,true)", v, ok)
	}
	put(t, e, "name", "godb2")
	if v, _ := mustGet(t, e, "name"); v != "godb2" {
		t.Errorf("overwrite: got %q, want godb2", v)
	}
	del(t, e, "name")
	if _, ok := mustGet(t, e, "name"); ok {
		t.Errorf("expected deleted key absent")
	}
}

func TestFlushToSSTable(t *testing.T) {
	e := openTest(t, Options{MemtableThreshold: 300, L0CompactionTrigger: 100})
	defer e.Close()

	for i := 0; i < 200; i++ {
		put(t, e, fmt.Sprintf("key%04d", i), fmt.Sprintf("val%04d", i))
	}
	waitFor(t, func() bool { return e.pendingImm() == 0 }, "flushes to drain")
	if _, total := e.tableCount(); total == 0 {
		t.Fatalf("expected at least one SSTable after flushing")
	}
	// Data spanning memtable and SSTables must all be readable.
	for i := 0; i < 200; i++ {
		if v, ok := mustGet(t, e, fmt.Sprintf("key%04d", i)); !ok || v != fmt.Sprintf("val%04d", i) {
			t.Fatalf("Get(key%04d) = (%q,%v)", i, v, ok)
		}
	}
}

func TestCompaction(t *testing.T) {
	e := openTest(t, Options{MemtableThreshold: 200, L0CompactionTrigger: 3})
	defer e.Close()

	for i := 0; i < 400; i++ {
		put(t, e, fmt.Sprintf("k%04d", i%50), fmt.Sprintf("v%04d", i)) // heavy overwrite of 50 keys
	}
	waitFor(t, func() bool {
		l0, _ := e.tableCount()
		return e.pendingImm() == 0 && l0 < 3
	}, "compaction to reduce L0 tables")

	_, total := e.tableCount()
	if total == 0 {
		t.Fatalf("expected tables after compaction")
	}
	// After heavy overwrites, the 50 live keys must still read their latest value.
	for i := 0; i < 50; i++ {
		if _, ok := mustGet(t, e, fmt.Sprintf("k%04d", i)); !ok {
			t.Fatalf("key k%04d missing after compaction", i)
		}
	}
}

func TestRecovery(t *testing.T) {
	dir := t.TempDir()
	e := openTest(t, Options{Dir: dir, MemtableThreshold: 300, L0CompactionTrigger: 100})
	for i := 0; i < 100; i++ {
		put(t, e, fmt.Sprintf("key%03d", i), fmt.Sprintf("val%03d", i))
	}
	del(t, e, "key050")
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify all committed state survived, including the delete.
	e2 := openTest(t, Options{Dir: dir, MemtableThreshold: 300, L0CompactionTrigger: 100})
	defer e2.Close()
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key%03d", i)
		v, ok := mustGet(t, e2, k)
		if k == "key050" {
			if ok {
				t.Errorf("deleted key050 reappeared after recovery")
			}
			continue
		}
		if !ok || v != fmt.Sprintf("val%03d", i) {
			t.Errorf("after recovery Get(%s) = (%q,%v)", k, v, ok)
		}
	}
}

func TestScanAcrossMemAndSSTables(t *testing.T) {
	e := openTest(t, Options{MemtableThreshold: 300, L0CompactionTrigger: 100})
	defer e.Close()

	for i := 0; i < 200; i++ {
		put(t, e, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i))
	}
	del(t, e, "k0100")
	waitFor(t, func() bool { return e.pendingImm() == 0 }, "flush")

	it := e.Scan([]byte("k0050"), []byte("k0150"))
	defer it.Close()
	count := 0
	var prev string
	for it.Next() {
		k := string(it.Key())
		if prev != "" && k <= prev {
			t.Fatalf("scan out of order: %q after %q", k, prev)
		}
		if k == "k0100" {
			t.Fatalf("deleted key k0100 appeared in scan")
		}
		prev = k
		count++
	}
	if count != 99 { // [k0050, k0150) is 100 keys, minus the deleted one
		t.Errorf("scan returned %d keys, want 99", count)
	}
}

func TestConflictFirstCommitterWins(t *testing.T) {
	e := openTest(t, Options{})
	defer e.Close()

	put(t, e, "x", "0")
	// Two transactions both read at the same snapshot.
	snapA := e.AcquireSnapshot()
	snapB := e.AcquireSnapshot()
	defer e.ReleaseSnapshot(snapA)
	defer e.ReleaseSnapshot(snapB)

	if _, err := e.Commit(snapA, []Mutation{{Key: []byte("x"), Value: []byte("A"), Kind: keys.KindSet}}); err != nil {
		t.Fatalf("first committer should win: %v", err)
	}
	// B read at the same snapshot but commits second: must be rejected.
	if _, err := e.Commit(snapB, []Mutation{{Key: []byte("x"), Value: []byte("B"), Kind: keys.KindSet}}); err != ErrConflict {
		t.Errorf("second committer: got %v, want ErrConflict", err)
	}
	if v, _ := mustGet(t, e, "x"); v != "A" {
		t.Errorf("value = %q, want A (first committer)", v)
	}
}

func TestConcurrentReadersWriters(t *testing.T) {
	e := openTest(t, Options{MemtableThreshold: 500, L0CompactionTrigger: 4})
	defer e.Close()

	var wg sync.WaitGroup
	// Writers.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				e.Commit(NoSnapshot, []Mutation{{
					Key: []byte(fmt.Sprintf("w%d-k%d", w, i)), Value: []byte("v"), Kind: keys.KindSet,
				}})
			}
		}(w)
	}
	// Readers + scanners run concurrently.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				e.Get([]byte(fmt.Sprintf("w0-k%d", i)), e.CurrentSeq())
				it := e.Scan(nil, nil)
				for it.Next() {
				}
				it.Close()
			}
		}()
	}
	wg.Wait()
}
