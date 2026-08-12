// Command godb-bench is a throughput and latency harness for GoDB. It measures
// write and read operations per second along with p50/p99 latency, and reports
// the Bloom-filter benefit by comparing present-key vs absent-key lookups.
//
// Usage:
//
//	godb-bench -n 200000
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/ronitraj/godb"
)

func main() {
	n := flag.Int("n", 200000, "number of operations per phase")
	sync := flag.Bool("sync", false, "fsync every write (durable but slower)")
	flag.Parse()

	dir, err := os.MkdirTemp("", "godb-bench-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	db, err := godb.OpenWith(godb.Options{Dir: dir, MemtableThreshold: 4 << 20, L0CompactionTrigger: 4, SyncWrites: *sync})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Printf("GoDB benchmark: n=%d sync=%v\n\n", *n, *sync)

	// Writes.
	writeLat := make([]time.Duration, *n)
	value := []byte("benchmark-value-payload-0123456789")
	startWrite := time.Now()
	for i := 0; i < *n; i++ {
		key := []byte(fmt.Sprintf("key%09d", i))
		t0 := time.Now()
		db.Put(key, value)
		writeLat[i] = time.Since(t0)
	}
	report("WRITE", *n, time.Since(startWrite), writeLat)

	// Point reads (present keys).
	rng := rand.New(rand.NewSource(1))
	readLat := make([]time.Duration, *n)
	startRead := time.Now()
	for i := 0; i < *n; i++ {
		key := []byte(fmt.Sprintf("key%09d", rng.Intn(*n)))
		t0 := time.Now()
		db.Get(key)
		readLat[i] = time.Since(t0)
	}
	report("READ (hit)", *n, time.Since(startRead), readLat)

	// Absent-key reads: these are where the per-SSTable Bloom filters pay off.
	missLat := make([]time.Duration, *n)
	startMiss := time.Now()
	for i := 0; i < *n; i++ {
		key := []byte(fmt.Sprintf("absent%09d", i))
		t0 := time.Now()
		db.Get(key)
		missLat[i] = time.Since(t0)
	}
	report("READ (miss, Bloom-filtered)", *n, time.Since(startMiss), missLat)
}

func report(name string, n int, wall time.Duration, lat []time.Duration) {
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	opsPerSec := float64(n) / wall.Seconds()
	fmt.Printf("%-30s %10.0f ops/sec   p50=%-10v p99=%-10v max=%v\n",
		name, opsPerSec, lat[len(lat)/2], lat[pct(lat, 99)], lat[len(lat)-1])
}

func pct(lat []time.Duration, p int) int {
	idx := p * len(lat) / 100
	if idx >= len(lat) {
		idx = len(lat) - 1
	}
	return idx
}
