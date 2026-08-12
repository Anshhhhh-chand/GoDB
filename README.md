# GoDB

A persistent, transactional key-value store written in **Go**, from scratch, using only the
standard library (plus a hand-written skip-list memtable — no external DB/KV dependency).
GoDB is an **LSM-tree** storage engine with write-ahead-log durability, MVCC snapshot
isolation, Bloom-filter-accelerated reads, and a TCP server — built to demonstrate database
internals end to end.

> **Why this exists:** it targets the competencies backend interviews actually probe —
> storage internals, durability (WAL/fsync/recovery), indexing (LSM/B-tree/Bloom filters),
> and concurrency (MVCC, isolation levels) — rather than being another CRUD app.

---

## Architecture

```
          ┌─────────────────────────────────────────────────────┐
Client →  │  REPL client  ───TCP, length-prefixed wire protocol──┐
          └─────────────────────────────────────────────────────┘│
                                                                  ▼
          ┌─────────────────────────────────────────────────────────┐
          │  SERVER            net.Listener, one goroutine / conn     │
          │  QUERY LAYER       tokenizer → parser → planner (SQL sub) │
          │  TRANSACTION MGR   MVCC, snapshot isolation, Begin/Commit │
          │  STORAGE ENGINE    LSM: memtable + WAL → SSTables + compac│
          │  DISK              os / bufio / encoding/binary           │
          └─────────────────────────────────────────────────────────┘
```

Every record is a versioned **internal key** — `userKey ++ ^seqNum ++ kind(put|tombstone)`.
That one monotonic sequence number is *simultaneously* the LSM sort order and the MVCC
timestamp. See [`DESIGN.md`](DESIGN.md) for the full high-level design, on-disk formats, and
the write/read paths.

## Public API

```go
db := godb.Open()
defer db.Close()

db.Put([]byte("name"), []byte("godb"))
v, found, _ := db.Get([]byte("name"))   // "godb", true

it, _ := db.Scan([]byte("a"), []byte("z")) // half-open range [start, end)
for it.Next() {
    fmt.Printf("%s = %s\n", it.Key(), it.Value())
}
it.Close()

db.Delete([]byte("name"))
```

`Scan` observes a consistent snapshot and yields the newest visible value per key, skipping
deletes.

## Build & test

Requires Go 1.22+.

```bash
go test -race ./...      # unit tests under the race detector
go vet ./...
```

## Project layout

```
godb/
├── godb.go                 public API (DB, Iterator)
├── DESIGN.md               high-level design
├── internal/
│   ├── keys/               internal key encoding + comparator
│   ├── memtable/           hand-written skip list
│   ├── wal/                write-ahead log + recovery        (planned)
│   ├── sstable/            SSTable writer/reader, block index (planned)
│   ├── bloom/              hand-written Bloom filter          (planned)
│   ├── lsm/                LSM engine + compaction            (planned)
│   ├── txn/                MVCC transaction manager           (planned)
│   └── query/              SQL-subset tokenizer/parser        (planned)
└── cmd/
    ├── godb-server/        TCP server                         (planned)
    └── godb-cli/           REPL client                        (planned)
```

## Roadmap

| Phase | Deliverable | Status |
|-------|-------------|--------|
| 0 | Public API + architecture locked | ✅ |
| 1 | In-memory sorted memtable behind the API | ✅ |
| 2 | WAL + fsync + crash recovery | ⏳ |
| 3 | SSTable flush + LSM reads + background compaction | ⏳ |
| 4 | Per-SSTable Bloom filter + sparse block index | ⏳ |
| 5 | MVCC snapshot isolation, Begin/Commit/Rollback | ⏳ |
| 6 | SQL-subset query layer + secondary index | ⏳ |
| 7 | TCP server + REPL client + wire protocol | ⏳ |
| 8 | Benchmark harness, fuzz/property tests, vs SQLite | ⏳ |

The versioning machinery for MVCC (unique per-write sequence numbers, retained old versions)
is already in place from Phase 1, so later phases layer on without a rewrite.

## License

[MIT](LICENSE)
