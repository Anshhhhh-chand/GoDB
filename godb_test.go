package godb

import (
	"bytes"
	"fmt"
	"testing"
)

func mustGet(t *testing.T, db *DB, key string) (string, bool) {
	t.Helper()
	v, found, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return string(v), found
}

func TestPutGet(t *testing.T) {
	db := Open()
	defer db.Close()

	if err := db.Put([]byte("name"), []byte("godb")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if v, found := mustGet(t, db, "name"); !found || v != "godb" {
		t.Errorf("Get(name) = (%q, %v), want (godb, true)", v, found)
	}
}

func TestPutOverwrite(t *testing.T) {
	db := Open()
	defer db.Close()

	db.Put([]byte("k"), []byte("v1"))
	db.Put([]byte("k"), []byte("v2"))
	if v, found := mustGet(t, db, "k"); !found || v != "v2" {
		t.Errorf("after overwrite Get(k) = (%q, %v), want (v2, true)", v, found)
	}
}

func TestDelete(t *testing.T) {
	db := Open()
	defer db.Close()

	db.Put([]byte("k"), []byte("v"))
	db.Delete([]byte("k"))
	if _, found := mustGet(t, db, "k"); found {
		t.Errorf("expected k to be deleted")
	}
	// Re-inserting after delete works.
	db.Put([]byte("k"), []byte("again"))
	if v, found := mustGet(t, db, "k"); !found || v != "again" {
		t.Errorf("after re-put Get(k) = (%q, %v), want (again, true)", v, found)
	}
}

func TestGetMissing(t *testing.T) {
	db := Open()
	defer db.Close()
	if _, found := mustGet(t, db, "absent"); found {
		t.Errorf("expected absent key to be not found")
	}
}

func TestScanRangeOrderAndDedup(t *testing.T) {
	db := Open()
	defer db.Close()

	for _, k := range []string{"c", "a", "e", "b", "d"} {
		db.Put([]byte(k), []byte("v-"+k))
	}
	db.Put([]byte("c"), []byte("v-c2")) // newer version of c
	db.Delete([]byte("d"))              // d removed

	it, err := db.Scan([]byte("b"), []byte("e")) // [b, e): expect b, c, (d deleted)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer it.Close()

	type kv struct{ k, v string }
	var got []kv
	for it.Next() {
		got = append(got, kv{string(it.Key()), string(it.Value())})
	}
	want := []kv{{"b", "v-b"}, {"c", "v-c2"}}
	if len(got) != len(want) {
		t.Fatalf("scan = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("scan[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestScanFullRange(t *testing.T) {
	db := Open()
	defer db.Close()
	n := 50
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	it, _ := db.Scan(nil, nil)
	defer it.Close()
	count := 0
	var prev []byte
	for it.Next() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("scan not in ascending order at %q after %q", it.Key(), prev)
		}
		prev = append(prev[:0:0], it.Key()...)
		count++
	}
	if count != n {
		t.Errorf("full scan returned %d keys, want %d", count, n)
	}
}

func TestClosedDBRejects(t *testing.T) {
	db := Open()
	db.Close()
	if err := db.Put([]byte("k"), []byte("v")); err != ErrClosed {
		t.Errorf("Put after Close = %v, want ErrClosed", err)
	}
	if _, _, err := db.Get([]byte("k")); err != ErrClosed {
		t.Errorf("Get after Close = %v, want ErrClosed", err)
	}
}
