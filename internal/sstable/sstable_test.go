package sstable

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ronitraj/godb/internal/keys"
)

func write(t *testing.T, path string, recs []keys.InternalKey, vals map[string]string) *Reader {
	t.Helper()
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, ik := range recs {
		if err := w.Add(keys.Encode(ik), []byte(vals[string(ik.UserKey)])); err != nil {
			t.Fatalf("Add(%q): %v", ik.UserKey, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return r
}

func TestWriteReadPointLookup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sst")
	recs := []keys.InternalKey{
		{UserKey: []byte("apple"), Seq: 3, Kind: keys.KindSet},
		{UserKey: []byte("banana"), Seq: 5, Kind: keys.KindSet},
		{UserKey: []byte("cherry"), Seq: 7, Kind: keys.KindSet},
	}
	vals := map[string]string{"apple": "red", "banana": "yellow", "cherry": "dark"}
	r := write(t, path, recs, vals)
	defer r.Close()

	for uk, want := range vals {
		v, kind, found, err := r.Get([]byte(uk), 100)
		if err != nil || !found || kind != keys.KindSet || string(v) != want {
			t.Errorf("Get(%q) = (%q,%v,%v,%v), want (%q,SET,true,nil)", uk, v, kind, found, err, want)
		}
	}
	if _, _, found, _ := r.Get([]byte("missing"), 100); found {
		t.Errorf("expected 'missing' absent")
	}
}

func TestSnapshotVisibility(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sst")
	// Two versions of "k": seq 2 (old) and seq 8 (new). Newest must sort first.
	recs := []keys.InternalKey{
		{UserKey: []byte("k"), Seq: 8, Kind: keys.KindSet},
		{UserKey: []byte("k"), Seq: 2, Kind: keys.KindSet},
	}
	w, _ := NewWriter(path)
	w.Add(keys.Encode(recs[0]), []byte("new"))
	w.Add(keys.Encode(recs[1]), []byte("old"))
	w.Finish()
	r, _ := Open(path)
	defer r.Close()

	v, _, _, _ := r.Get([]byte("k"), 5) // snapshot between the two versions
	if string(v) != "old" {
		t.Errorf("snapshot 5: got %q, want old", v)
	}
	v, _, _, _ = r.Get([]byte("k"), 100)
	if string(v) != "new" {
		t.Errorf("snapshot 100: got %q, want new", v)
	}
}

func TestTombstoneFound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sst")
	w, _ := NewWriter(path)
	w.Add(keys.Encode(keys.InternalKey{UserKey: []byte("k"), Seq: 4, Kind: keys.KindDelete}), nil)
	w.Finish()
	r, _ := Open(path)
	defer r.Close()
	_, kind, found, _ := r.Get([]byte("k"), 100)
	if !found || kind != keys.KindDelete {
		t.Errorf("expected tombstone found, got kind=%v found=%v", kind, found)
	}
}

func TestManyKeysMultiBlockIterationAndLookup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sst")
	n := 5000 // enough to span many 4KB blocks and force boundary straddling
	w, _ := NewWriter(path)
	for i := 0; i < n; i++ {
		ik := keys.InternalKey{UserKey: []byte(fmt.Sprintf("key%06d", i)), Seq: 1, Kind: keys.KindSet}
		if err := w.Add(keys.Encode(ik), []byte(fmt.Sprintf("val%06d", i))); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	w.Finish()
	r, _ := Open(path)
	defer r.Close()

	// Every key must be found (exercises the block index + boundary crossing).
	for i := 0; i < n; i++ {
		uk := fmt.Sprintf("key%06d", i)
		v, _, found, err := r.Get([]byte(uk), 100)
		if err != nil || !found || string(v) != fmt.Sprintf("val%06d", i) {
			t.Fatalf("Get(%q) = (%q,%v,%v)", uk, v, found, err)
		}
	}

	// Full iteration must be ordered and complete.
	it := r.NewIterator()
	it.SeekToFirst()
	count := 0
	var prev []byte
	for it.Valid() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("iteration out of order at record %d", count)
		}
		prev = append(prev[:0:0], it.Key()...)
		count++
		it.Next()
	}
	if count != n {
		t.Errorf("iterated %d records, want %d", count, n)
	}
}

func TestBloomRejectsAbsent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sst")
	w, _ := NewWriter(path)
	for i := 0; i < 100; i++ {
		w.Add(keys.Encode(keys.InternalKey{UserKey: []byte(fmt.Sprintf("present%03d", i)), Seq: 1, Kind: keys.KindSet}), []byte("x"))
	}
	w.Finish()
	r, _ := Open(path)
	defer r.Close()
	if r.MayContain([]byte("present050")) != true {
		t.Errorf("bloom should report present key as maybe-present")
	}
	// Not a hard guarantee, but a clearly-absent key should usually be rejected.
	if r.MayContain([]byte("definitely-not-here-zzz")) {
		t.Logf("bloom false positive (allowed, but noted)")
	}
}

func TestAddOutOfOrderRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sst")
	w, _ := NewWriter(path)
	w.Add(keys.Encode(keys.InternalKey{UserKey: []byte("b"), Seq: 1, Kind: keys.KindSet}), []byte("x"))
	if err := w.Add(keys.Encode(keys.InternalKey{UserKey: []byte("a"), Seq: 1, Kind: keys.KindSet}), []byte("y")); err == nil {
		t.Errorf("expected out-of-order Add to be rejected")
	}
}
