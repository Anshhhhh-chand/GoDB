package godb

import (
	"fmt"
	"testing"
)

// These benchmarks measure the core operations. Run with:
//
//	go test -bench=. -benchmem
//
// BenchmarkGetMiss in particular exercises the Bloom filters: absent-key
// lookups should skip SSTables cheaply.

func benchDB(b *testing.B) *DB {
	b.Helper()
	db, err := OpenWith(Options{Dir: b.TempDir(), MemtableThreshold: 1 << 20, L0CompactionTrigger: 4, SyncWrites: false})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	return db
}

func BenchmarkPut(b *testing.B) {
	db := benchDB(b)
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put([]byte(fmt.Sprintf("key%08d", i)), []byte("value-payload-of-some-size"))
	}
}

func BenchmarkGetHit(b *testing.B) {
	db := benchDB(b)
	defer db.Close()
	const n = 100000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%08d", i)), []byte("v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("key%08d", i%n)))
	}
}

func BenchmarkGetMiss(b *testing.B) {
	db := benchDB(b)
	defer db.Close()
	const n = 100000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%08d", i)), []byte("v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("absent%08d", i)))
	}
}

func BenchmarkScan100(b *testing.B) {
	db := benchDB(b)
	defer db.Close()
	const n = 100000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%08d", i)), []byte("v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := []byte(fmt.Sprintf("key%08d", (i*100)%n))
		it, _ := db.Scan(start, nil)
		count := 0
		for it.Next() && count < 100 {
			count++
		}
		it.Close()
	}
}
