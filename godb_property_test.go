package godb

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// TestPropertyAgainstReferenceMap runs a long sequence of random operations
// against both GoDB and a reference map, asserting they always agree. A reopen
// midway also checks that persistence preserves the model. This is the
// project's fuzz/property safety net.
func TestPropertyAgainstReferenceMap(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenWith(Options{Dir: dir, MemtableThreshold: 512, L0CompactionTrigger: 3, SyncWrites: false})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	model := map[string]string{}
	rng := rand.New(rand.NewSource(1234))
	keyspace := 60

	check := func(step int) {
		// Point reads for every possible key.
		for i := 0; i < keyspace; i++ {
			k := fmt.Sprintf("k%02d", i)
			v, found, err := db.Get([]byte(k))
			if err != nil {
				t.Fatalf("step %d Get(%s): %v", step, k, err)
			}
			mv, mok := model[k]
			if found != mok || (found && string(v) != mv) {
				t.Fatalf("step %d mismatch on %s: db=(%q,%v) model=(%q,%v)", step, k, v, found, mv, mok)
			}
		}
	}

	for step := 0; step < 4000; step++ {
		k := fmt.Sprintf("k%02d", rng.Intn(keyspace))
		switch rng.Intn(3) {
		case 0, 1:
			v := fmt.Sprintf("v%d", rng.Intn(1000))
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				t.Fatalf("Put: %v", err)
			}
			model[k] = v
		case 2:
			if err := db.Delete([]byte(k)); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			delete(model, k)
		}

		if step%500 == 0 {
			check(step)
		}
		// Reopen once to exercise recovery against the live model.
		if step == 2000 {
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			db, err = OpenWith(Options{Dir: dir, MemtableThreshold: 512, L0CompactionTrigger: 3, SyncWrites: false})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			check(step)
		}
	}
	check(4000)

	// Final full-range scan must equal the sorted model.
	it, _ := db.Scan(nil, nil)
	var got []string
	for it.Next() {
		got = append(got, string(it.Key())+"="+string(it.Value()))
	}
	it.Close()

	var want []string
	for k, v := range model {
		want = append(want, k+"="+v)
	}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("final scan has %d entries, model has %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("final scan[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	db.Close()
}
