// Command crasher is a helper for the crash-injection test. It opens a GoDB
// database, writes N keys (fsync'd, since SyncWrites defaults on), prints each
// acknowledged key to stdout, and then exits *without* calling Close — the
// parent kills it to simulate a crash. On the next open, every key the parent
// observed on stdout must be present.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/ronitraj/godb"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: crasher <dir> <n>")
		os.Exit(2)
	}
	dir := os.Args[1]
	n, _ := strconv.Atoi(os.Args[2])

	db, err := godb.Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}

	out := bufio.NewWriter(os.Stdout)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("val%06d", i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			fmt.Fprintln(os.Stderr, "put:", err)
			os.Exit(1)
		}
		// Report the write as acknowledged only after Put returns (post-fsync).
		fmt.Fprintln(out, k)
		out.Flush()
	}
	// Intentionally do not Close; parent kills us mid-run.
	select {}
}
