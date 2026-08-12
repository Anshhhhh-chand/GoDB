package godb

import (
	"bytes"
	"fmt"
	"testing"
)

func open(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func mustGet(t *testing.T, db *DB, key string) (string, bool) {
	t.Helper()
	v, found, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return string(v), found
}

func TestPutGetDelete(t *testing.T) {
	db := open(t)
	defer db.Close()

	if err := db.Put([]byte("name"), []byte("godb")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if v, found := mustGet(t, db, "name"); !found || v != "godb" {
		t.Errorf("Get(name) = (%q,%v), want (godb,true)", v, found)
	}
	db.Put([]byte("name"), []byte("godb2"))
	if v, _ := mustGet(t, db, "name"); v != "godb2" {
		t.Errorf("overwrite = %q, want godb2", v)
	}
	db.Delete([]byte("name"))
	if _, found := mustGet(t, db, "name"); found {
		t.Errorf("expected name deleted")
	}
}

func TestScan(t *testing.T) {
	db := open(t)
	defer db.Close()
	for _, k := range []string{"c", "a", "e", "b", "d"} {
		db.Put([]byte(k), []byte("v-"+k))
	}
	db.Delete([]byte("d"))

	it, err := db.Scan([]byte("b"), []byte("e")) // [b, e): b, c (d deleted)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer it.Close()
	var got []string
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Errorf("scan = %v, want [b c]", got)
	}
}

func TestTransactionCommit(t *testing.T) {
	db := open(t)
	defer db.Close()

	tx := db.Begin()
	tx.Put([]byte("a"), []byte("1"))
	tx.Put([]byte("b"), []byte("2"))
	// Uncommitted writes are invisible outside the transaction...
	if _, found := mustGet(t, db, "a"); found {
		t.Errorf("uncommitted write visible outside txn")
	}
	// ...but visible inside it (read-your-writes).
	if v, found, _ := tx.Get([]byte("a")); !found || string(v) != "1" {
		t.Errorf("read-your-writes failed: (%q,%v)", v, found)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if v, found := mustGet(t, db, "a"); !found || v != "1" {
		t.Errorf("after commit Get(a) = (%q,%v)", v, found)
	}
}

func TestTransactionRollback(t *testing.T) {
	db := open(t)
	defer db.Close()

	db.Put([]byte("k"), []byte("original"))
	tx := db.Begin()
	tx.Put([]byte("k"), []byte("changed"))
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if v, _ := mustGet(t, db, "k"); v != "original" {
		t.Errorf("after rollback Get(k) = %q, want original", v)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	db := open(t)
	defer db.Close()

	db.Put([]byte("k"), []byte("v1"))
	tx := db.Begin() // snapshot sees v1
	// A concurrent write after the snapshot must not be visible to the txn.
	db.Put([]byte("k"), []byte("v2"))
	if v, found, _ := tx.Get([]byte("k")); !found || string(v) != "v1" {
		t.Errorf("snapshot isolation broken: txn sees (%q,%v), want v1", v, found)
	}
	tx.Rollback()
	if v, _ := mustGet(t, db, "k"); v != "v2" {
		t.Errorf("outside txn = %q, want v2", v)
	}
}

func TestWriteConflict(t *testing.T) {
	db := open(t)
	defer db.Close()
	db.Put([]byte("x"), []byte("0"))

	a := db.Begin()
	b := db.Begin()
	a.Put([]byte("x"), []byte("A"))
	b.Put([]byte("x"), []byte("B"))
	if err := a.Commit(); err != nil {
		t.Fatalf("first committer should win: %v", err)
	}
	if err := b.Commit(); err != ErrConflict {
		t.Errorf("second committer = %v, want ErrConflict", err)
	}
}

func TestTransactionScanOverlay(t *testing.T) {
	db := open(t)
	defer db.Close()
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Put([]byte("c"), []byte("3"))

	tx := db.Begin()
	tx.Put([]byte("b"), []byte("22")) // overwrite
	tx.Delete([]byte("c"))            // hide
	tx.Put([]byte("d"), []byte("4"))  // new

	it, err := tx.Scan(nil, nil)
	if err != nil {
		t.Fatalf("tx.Scan: %v", err)
	}
	defer it.Close()
	got := map[string]string{}
	for it.Next() {
		got[string(it.Key())] = string(it.Value())
	}
	want := map[string]string{"a": "1", "b": "22", "d": "4"} // c hidden
	if len(got) != len(want) {
		t.Fatalf("overlay scan = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("overlay scan[%s] = %q, want %q", k, got[k], v)
		}
	}
}

func TestDurabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	n := 500
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("val%04d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	db.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		v, found, _ := db2.Get([]byte(fmt.Sprintf("key%04d", i)))
		if !found || !bytes.Equal(v, []byte(fmt.Sprintf("val%04d", i))) {
			t.Fatalf("after reopen Get(key%04d) = (%q,%v)", i, v, found)
		}
	}
}
