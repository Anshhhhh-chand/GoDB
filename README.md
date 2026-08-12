# GoDB

A persistent, transactional key-value store written in **Go**, from scratch, using only the
standard library (the memtable is a hand-written skip list — **no external dependencies**).
GoDB is a full **LSM-tree** storage engine with write-ahead-log durability, MVCC snapshot
isolation, per-SSTable Bloom filters, background compaction, a SQL-subset query layer, and a
TCP server with a REPL client.

GoDB is built from the ground up to implement fundamental database systems internals end-to-end: storage engines, crash durability (WAL / fsync / recovery), probabilistic indexing (Bloom filters), log-structured merge trees with background compaction, and concurrency control (MVCC with snapshot isolation).

---

## Architecture

```
          ┌─────────────────────────────────────────────────────┐
Client →  │  REPL client  ───TCP, length-prefixed wire protocol──┐
          └─────────────────────────────────────────────────────┘│
                                                                  ▼
          ┌─────────────────────────────────────────────────────────┐
          │  SERVER            net.Listener, one goroutine / conn     │
          │  QUERY LAYER       tokenizer → parser → executor (SQL sub)│
          │  TRANSACTION MGR   MVCC, snapshot isolation, Begin/Commit │
          │  STORAGE ENGINE    LSM: memtable + WAL → SSTables + compac│
          │  DISK              os / bufio / encoding/binary           │
          └─────────────────────────────────────────────────────────┘
```

Every record is a versioned **internal key** — `userKey ++ ^seqNum ++ kind(put|tombstone)`.
That one monotonic sequence number is *simultaneously* the LSM sort order and the MVCC
timestamp, ensuring newest versions sort first and snapshot isolation works without locks.

## Use it as a library

```go
db, _ := godb.Open("./data")
defer db.Close()

db.Put([]byte("name"), []byte("godb"))
v, found, _ := db.Get([]byte("name"))          // "godb", true

it, _ := db.Scan([]byte("a"), []byte("z"))      // half-open range [start, end)
for it.Next() {
    fmt.Printf("%s = %s\n", it.Key(), it.Value())
}
it.Close()

// Transactions with snapshot isolation and first-committer-wins.
tx := db.Begin()
tx.Put([]byte("k"), []byte("v"))
if err := tx.Commit(); err == godb.ErrConflict {
    // retry
}

// SQL subset, mapped onto the KV engine.
db.Query("CREATE TABLE users (id INT, name TEXT, city TEXT, PRIMARY KEY (id))")
db.Query("INSERT INTO users (id, name, city) VALUES (1, 'alice', 'paris')")
db.Query("CREATE INDEX ON users (city)")        // secondary index keyspace
res, _ := db.Query("SELECT name FROM users WHERE city = 'paris'")
```

## Run it as a server

```bash
make server                       # go run ./cmd/godb-server -addr :6380 -dir ./data
# in another terminal:
make cli                          # REPL client
```

```
godb> PUT hello world
OK
godb> GET hello
world
godb> CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))
CREATE TABLE t
godb> INSERT INTO t (id, name) VALUES (1, 'ada')
1 row affected
godb> SELECT * FROM t WHERE id = 1
id | name
1 | ada
(1 row)
```

Supported SQL: `CREATE TABLE`, `CREATE INDEX`, `INSERT`, `SELECT … WHERE`, `UPDATE`, `DELETE`.
KV verbs over the wire: `GET`, `PUT`, `DEL`, `SCAN`.

## Docker

```bash
docker build -t godb .
docker run -p 6380:6380 -v godb-data:/data godb
```

## Build, test, benchmark

Requires Go 1.22+.

```bash
make race       # go test -race ./...   (what CI runs)
make bench      # micro-benchmarks + throughput/latency harness
make cover      # coverage summary
```

### Sample benchmark (Apple Silicon, in-memory temp dir, sync off)

| Operation                    | Throughput      | p50    | p99     |
|------------------------------|-----------------|--------|---------|
| Write                        | ~790K ops/sec   | 0.6 µs | 7.8 µs  |
| Read (hit)                   | ~284K ops/sec   | 2.5 µs | 16 µs   |
| Read (miss, Bloom-filtered)  | ~4.5M ops/sec   | 83 ns  | 1.6 µs  |

The absent-key read path is ~16× faster than a hit because the per-SSTable Bloom filter
rejects the key without ever touching a data block, eliminating unnecessary disk I/O.

## Testing strategy

- **Unit tests** in every package (`keys`, `memtable`, `wal`, `sstable`, `bloom`, `lsm`,
  `query`, `protocol`).
- **Crash-injection** (`internal/crashtest`): a child process writes fsync-durable data and
  is `SIGKILL`ed mid-write; on reopen, every acknowledged write must survive. It does.
- **Property/fuzz test**: thousands of random ops (with a mid-run reopen) checked against a
  reference `map` — the engine must always agree.
- **Race detector**: `go test -race ./...` in CI.

## Project layout

```
godb/
├── godb.go / godb_query.go     public API (DB, Tx, Iterator, Query)
├── internal/
│   ├── keys/                   internal key encoding + comparator
│   ├── memtable/               hand-written skip list
│   ├── wal/                    write-ahead log + torn-tail-tolerant recovery
│   ├── sstable/                SSTable writer/reader, sparse block index
│   ├── bloom/                  hand-written Bloom filter
│   ├── lsm/                    engine: recovery, flush, compaction, MVCC
│   ├── txn/                    transaction objects (Begin/Commit/Rollback)
│   ├── query/                  SQL tokenizer, parser, executor
│   ├── protocol/               length-prefixed wire protocol + interpreter
│   └── crashtest/              crash-injection durability test
└── cmd/
    ├── godb-server/            TCP server
    ├── godb-cli/               REPL client
    └── godb-bench/             throughput / latency harness
```

## Key features

- **Storage Engine**: LSM-tree architecture with an in-memory skip-list memtable and immutable on-disk SSTables (4KB block size).
- **Durability & Recovery**: Append-only Write-Ahead Log (WAL) with CRC32-Castagnoli checksums and fsync-before-ack durability; crash-safe recovery tolerating torn tails.
- **Fast Lookups**: Per-SSTable Bloom filters (Kirsch–Mitzenmacher double-hashing) and in-memory sparse block indexes for fast point-lookup rejections and binary searches.
- **Background Compaction**: Multi-way merge compaction (L0 → L1) with garbage collection of obsolete versions and tombstones.
- **ACID Transactions**: MVCC with snapshot isolation, read-your-writes guarantees, and optimistic first-committer-wins conflict detection.
- **SQL Query Layer**: Hand-written recursive-descent parser and executor supporting DDL (`CREATE TABLE`, `CREATE INDEX`) and DML (`INSERT`, `SELECT ... WHERE`, `UPDATE`, `DELETE`) with secondary index prefix scanning.
- **Client & Server**: Length-prefixed wire protocol over TCP, multi-threaded server daemon, and interactive REPL CLI.
- **Zero Dependencies**: Pure Go standard library implementation with zero third-party packages.

## License

[MIT](LICENSE)
