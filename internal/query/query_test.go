package query

import (
	"sort"
	"testing"
)

// memKV is an in-memory KV implementing the query layer's storage interface,
// so the query package can be tested without the full engine.
type memKV struct {
	m map[string][]byte
}

func newMemKV() *memKV { return &memKV{m: map[string][]byte{}} }

func (k *memKV) Get(key []byte) ([]byte, bool, error) {
	v, ok := k.m[string(key)]
	return v, ok, nil
}
func (k *memKV) Put(key, value []byte) error { k.m[string(key)] = append([]byte(nil), value...); return nil }
func (k *memKV) Delete(key []byte) error     { delete(k.m, string(key)); return nil }

type memIter struct {
	keys []string
	m    map[string][]byte
	i    int
}

func (k *memKV) Scan(start, end []byte) (Iterator, error) {
	var ks []string
	for key := range k.m {
		if string(start) != "" && key < string(start) {
			continue
		}
		if end != nil && key >= string(end) {
			continue
		}
		ks = append(ks, key)
	}
	sort.Strings(ks)
	return &memIter{keys: ks, m: k.m, i: -1}, nil
}

func (it *memIter) Next() bool     { it.i++; return it.i < len(it.keys) }
func (it *memIter) Key() []byte    { return []byte(it.keys[it.i]) }
func (it *memIter) Value() []byte  { return it.m[it.keys[it.i]] }
func (it *memIter) Close() error   { return nil }

func exec(t *testing.T, kv KV, sql string) *Result {
	t.Helper()
	r, err := Exec(kv, sql)
	if err != nil {
		t.Fatalf("Exec(%q): %v", sql, err)
	}
	return r
}

func TestCreateInsertSelect(t *testing.T) {
	kv := newMemKV()
	exec(t, kv, "CREATE TABLE users (id INT, name TEXT, city TEXT, PRIMARY KEY (id))")
	exec(t, kv, "INSERT INTO users (id, name, city) VALUES (1, 'alice', 'paris')")
	exec(t, kv, "INSERT INTO users (id, name, city) VALUES (2, 'bob', 'london')")
	exec(t, kv, "INSERT INTO users (id, name, city) VALUES (3, 'carol', 'paris')")

	r := exec(t, kv, "SELECT name, city FROM users WHERE id = 2")
	if len(r.Rows) != 1 || r.Rows[0][0] != "bob" || r.Rows[0][1] != "london" {
		t.Fatalf("PK select = %v, want [[bob london]]", r.Rows)
	}

	all := exec(t, kv, "SELECT * FROM users")
	if len(all.Rows) != 3 {
		t.Fatalf("SELECT * returned %d rows, want 3", len(all.Rows))
	}
}

func TestIndexedWhere(t *testing.T) {
	kv := newMemKV()
	exec(t, kv, "CREATE TABLE users (id INT, name TEXT, city TEXT, PRIMARY KEY (id))")
	exec(t, kv, "INSERT INTO users (id, name, city) VALUES (1, 'alice', 'paris')")
	exec(t, kv, "INSERT INTO users (id, name, city) VALUES (2, 'bob', 'london')")
	exec(t, kv, "INSERT INTO users (id, name, city) VALUES (3, 'carol', 'paris')")
	exec(t, kv, "CREATE INDEX ON users (city)")

	// Indexed lookup should return both Paris rows (backfilled + future inserts).
	r := exec(t, kv, "SELECT name FROM users WHERE city = 'paris'")
	got := []string{}
	for _, row := range r.Rows {
		got = append(got, row[0])
	}
	sort.Strings(got)
	if len(got) != 2 || got[0] != "alice" || got[1] != "carol" {
		t.Fatalf("indexed WHERE = %v, want [alice carol]", got)
	}

	// Verify the index keyspace actually holds entries (proves it's not a scan).
	found := 0
	for k := range kv.m {
		if len(k) > 7 && k[:7] == "sql\x00idx" {
			found++
		}
	}
	if found == 0 {
		t.Errorf("expected secondary index entries in the keyspace")
	}
}

func TestUpdateAndDelete(t *testing.T) {
	kv := newMemKV()
	exec(t, kv, "CREATE TABLE t (id INT, v TEXT, PRIMARY KEY (id))")
	exec(t, kv, "INSERT INTO t (id, v) VALUES (1, 'a')")
	exec(t, kv, "INSERT INTO t (id, v) VALUES (2, 'b')")

	u := exec(t, kv, "UPDATE t SET v = 'z' WHERE id = 1")
	if u.RowsAffected != 1 {
		t.Fatalf("UPDATE affected %d, want 1", u.RowsAffected)
	}
	r := exec(t, kv, "SELECT v FROM t WHERE id = 1")
	if r.Rows[0][0] != "z" {
		t.Fatalf("after update v = %q, want z", r.Rows[0][0])
	}

	d := exec(t, kv, "DELETE FROM t WHERE id = 2")
	if d.RowsAffected != 1 {
		t.Fatalf("DELETE affected %d, want 1", d.RowsAffected)
	}
	if r := exec(t, kv, "SELECT * FROM t"); len(r.Rows) != 1 {
		t.Fatalf("after delete %d rows remain, want 1", len(r.Rows))
	}
}

func TestUpdateIndexMaintained(t *testing.T) {
	kv := newMemKV()
	exec(t, kv, "CREATE TABLE t (id INT, city TEXT, PRIMARY KEY (id))")
	exec(t, kv, "INSERT INTO t (id, city) VALUES (1, 'paris')")
	exec(t, kv, "CREATE INDEX ON t (city)")
	exec(t, kv, "UPDATE t SET city = 'london' WHERE id = 1")

	if r := exec(t, kv, "SELECT id FROM t WHERE city = 'paris'"); len(r.Rows) != 0 {
		t.Errorf("stale index entry: found %d rows for old value", len(r.Rows))
	}
	if r := exec(t, kv, "SELECT id FROM t WHERE city = 'london'"); len(r.Rows) != 1 {
		t.Errorf("index not updated: found %d rows for new value, want 1", len(r.Rows))
	}
}

func TestParseErrors(t *testing.T) {
	kv := newMemKV()
	for _, sql := range []string{
		"SELCT * FROM t",
		"CREATE TABLE",
		"INSERT INTO t (a) VALUES (1, 2)",
		"SELECT * FROM nope WHERE id = 1",
	} {
		if _, err := Exec(kv, sql); err == nil {
			t.Errorf("expected error for %q", sql)
		}
	}
}
