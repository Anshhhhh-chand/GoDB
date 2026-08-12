package memtable

import (
	"bytes"
	"testing"

	"github.com/ronitraj/godb/internal/keys"
)

func put(m *Memtable, key, val string, seq keys.SeqNum, kind keys.Kind) {
	m.Put(keys.InternalKey{UserKey: []byte(key), Seq: seq, Kind: kind}, []byte(val))
}

func TestGetReturnsNewestVisibleVersion(t *testing.T) {
	m := New()
	put(m, "k", "v1", 1, keys.KindSet)
	put(m, "k", "v2", 5, keys.KindSet)
	put(m, "k", "v3", 9, keys.KindSet)

	// A snapshot at seq 7 must see v2 (the newest version with seq <= 7).
	val, kind, found := m.Get([]byte("k"), 7)
	if !found || kind != keys.KindSet || !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("snapshot 7: got (%q, %v, %v), want (v2, SET, true)", val, kind, found)
	}

	// The latest snapshot sees v3.
	val, _, found = m.Get([]byte("k"), 100)
	if !found || !bytes.Equal(val, []byte("v3")) {
		t.Fatalf("latest: got (%q, %v), want (v3, true)", val, found)
	}
}

func TestGetTombstone(t *testing.T) {
	m := New()
	put(m, "k", "v1", 1, keys.KindSet)
	put(m, "k", "", 5, keys.KindDelete)

	_, kind, found := m.Get([]byte("k"), 10)
	if !found || kind != keys.KindDelete {
		t.Fatalf("expected to find a tombstone, got kind=%v found=%v", kind, found)
	}

	// A snapshot before the delete still sees the value.
	val, kind, found := m.Get([]byte("k"), 3)
	if !found || kind != keys.KindSet || !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("pre-delete snapshot: got (%q, %v, %v), want (v1, SET, true)", val, kind, found)
	}
}

func TestGetMissing(t *testing.T) {
	m := New()
	put(m, "a", "1", 1, keys.KindSet)
	if _, _, found := m.Get([]byte("z"), 100); found {
		t.Errorf("expected z to be absent")
	}
}

func TestIteratorRangeAndOrder(t *testing.T) {
	m := New()
	put(m, "b", "2", 1, keys.KindSet)
	put(m, "a", "1", 1, keys.KindSet)
	put(m, "d", "4", 1, keys.KindSet)
	put(m, "c", "3", 1, keys.KindSet)

	var gotKeys []string
	it := m.NewIterator([]byte("b"), []byte("d")) // [b, d)
	for it.Next() {
		ik, _ := keys.Decode(it.Key())
		gotKeys = append(gotKeys, string(ik.UserKey))
	}
	want := []string{"b", "c"}
	if len(gotKeys) != len(want) {
		t.Fatalf("range scan keys = %v, want %v", gotKeys, want)
	}
	for i := range want {
		if gotKeys[i] != want[i] {
			t.Errorf("range scan keys = %v, want %v", gotKeys, want)
			break
		}
	}
}

func TestSizeAndLenGrow(t *testing.T) {
	m := New()
	if m.Len() != 0 || m.Size() != 0 {
		t.Fatalf("empty memtable should report zero len/size")
	}
	put(m, "k", "value", 1, keys.KindSet)
	if m.Len() != 1 {
		t.Errorf("Len = %d, want 1", m.Len())
	}
	if m.Size() == 0 {
		t.Errorf("Size should grow after a Put")
	}
}
