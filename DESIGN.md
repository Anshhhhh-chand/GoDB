# GoDB — High-Level Design

A persistent, transactional key-value store with a SQL-subset query layer, written in Go
using only the standard library (plus one small memtable dependency). LSM-tree storage,
WAL durability, MVCC snapshot isolation, Bloom-filter-accelerated reads, and a TCP server.

---

## 1. Design goals & non-goals

**Goals**
- Durability: an acknowledged write survives a crash (WAL + fsync before ack).
- Correctness: reads never observe partial writes; transactions see a consistent snapshot.
- Bounded read amplification via background compaction.
- Measurable performance (throughput + p50/p99), so every optimization is quantified.
- Minimal dependencies — the engine is hand-written; that *is* the value.

**Non-goals (v1)**
- Distributed / replication / consensus. Single node only.
- Full SQL. A deliberate subset (CREATE TABLE, INSERT, SELECT…WHERE, UPDATE, DELETE).
- Serializable isolation. We target **snapshot isolation** (first-committer-wins).

---

## 2. Layered architecture

```
          ┌─────────────────────────────────────────────────────┐
Client →  │  REPL client  ───TCP, length-prefixed wire protocol──┐
          └─────────────────────────────────────────────────────┘│
                                                                  ▼
          ┌─────────────────────────────────────────────────────────┐
          │  SERVER            net.Listener, one goroutine / conn     │
          ├─────────────────────────────────────────────────────────┤
          │  QUERY LAYER       tokenizer → parser → planner           │
          │                    (maps SQL rows onto KV keyspaces)      │
          ├─────────────────────────────────────────────────────────┤
          │  TRANSACTION MGR   MVCC, snapshot isolation, commit ts    │
          │                    Begin / Commit / Rollback              │
          ├─────────────────────────────────────────────────────────┤
          │  STORAGE ENGINE (LSM-tree)                                │
          │    ┌──────────┐  flush   ┌───────────────────────────┐    │
          │    │ Memtable │ ───────► │ SSTables  L0 → L1 → …      │    │
          │    │ (sorted) │          │ (immutable, sorted)       │    │
          │    └────┬─────┘          └───────────┬───────────────┘    │
          │         │ append                     │ merge              │
          │    ┌────▼─────┐              ┌────────▼────────┐           │
          │    │   WAL    │              │  Compaction     │           │
          │    │ (fsync)  │              │  worker (bg)    │           │
          │    └──────────┘              └─────────────────┘           │
          ├─────────────────────────────────────────────────────────┤
          │  DISK              os / bufio / encoding/binary            │
          └─────────────────────────────────────────────────────────┘
```

Each layer depends only on the one below it. The public API (`Get/Put/Delete/Scan/Begin…`)
is exposed at the **storage engine + transaction** boundary so the DB is usable as an
embedded library *and* wrapped by the server.

---

## 3. Public API (locked first — Phase 0)

```go
// Package godb
type DB interface {
    Get(key []byte) (value []byte, found bool, err error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Scan(start, end []byte) (Iterator, error)   // [start, end)

    Begin() (*Txn, error)
    Close() error
}

type Txn interface {
    Get(key []byte) ([]byte, bool, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Scan(start, end []byte) (Iterator, error)
    Commit() error     // first-committer-wins; returns ErrConflict on write-write clash
    Rollback() error
}

type Iterator interface {
    Next() bool
    Key() []byte
    Value() []byte
    Err() error
    Close() error
}
```

Non-transactional `DB.Put/Get` are sugar over a one-statement auto-commit txn, so there is
a single code path through the transaction manager.

---

## 4. Core data model — everything is a versioned KV pair

Internally every record is an **internal key**:

```
internalKey = userKey ++ seqNum(8B, big-endian, inverted) ++ kind(1B)
kind ∈ { PUT=1, DELETE=0 (tombstone) }
```

- Sorting by `internalKey` groups identical user keys together, newest version first
  (seqNum inverted so a descending scan finds the latest version immediately).
- **Tombstones** represent deletes; they are physically removed only during compaction.
- The monotonic `seqNum` is the MVCC timestamp — this is the single mechanism behind
  both LSM ordering and snapshot isolation.

---

## 5. Write path

```
Put(k,v)  ──►  assign seqNum (atomic)  ──►  append record to WAL  ──►  fsync
          ──►  insert into memtable  ──►  ACK to caller
          ──►  if memtable.size > threshold: swap to immutable, signal flush worker
```

- **WAL first, fsync before ack** = the durability guarantee. Nothing is acknowledged
  until it is on stable storage.
- Memtable insert is O(log n) (skip list / B-tree).
- Flush is asynchronous: a full memtable becomes read-only, a fresh one takes writes, and
  a background goroutine serializes the frozen memtable to an L0 SSTable, then truncates
  the corresponding WAL segment.

---

## 6. Read path

```
Get(k, snapshotSeq):
  1. active memtable          (newest)
  2. immutable memtable(s) being flushed
  3. SSTables L0 newest→oldest, then L1, L2 …   (oldest)
  For each SSTable, in order:
     - Bloom filter: definitely-absent? → skip file entirely
     - sparse block index: binary-search to the right block
     - scan block for the newest version with seqNum ≤ snapshotSeq
  First match wins. A tombstone match ⇒ "not found".
```

Read amplification is bounded by the number of levels; Bloom filters make most SSTable
probes O(1) rejections. This is where Phase 4's before/after latency numbers come from.

---

## 7. On-disk formats

**WAL record** (append-only, one per mutation):
```
| recordLen u32 | seqNum u64 | kind u8 | keyLen u32 | key | valLen u32 | val | crc32 u32 |
```
CRC guards against torn/partial tail writes; recovery stops at the first bad CRC.

**SSTable file** (immutable, written once on flush):
```
[ data blocks ]        sorted internalKey→value, grouped into ~4KB blocks
[ bloom filter ]       hand-written, hash/fnv, k hashes
[ sparse index ]       first key of each block → block offset  (binary searchable)
[ footer ]             offsets of bloom + index + magic number
```

---

## 8. Transactions & MVCC (Phase 5)

- `Begin()` captures a **read snapshot** = current committed seqNum. All reads in the txn
  filter to versions with `seqNum ≤ snapshot`, so writers never block readers.
- Writes buffer in a per-txn write set (not visible to others until commit).
- `Commit()`:
  1. Conflict check: for each written key, verify no *other* committed txn wrote it after
     our snapshot → **first-committer-wins**, else `ErrConflict`.
  2. Assign a commit seqNum, write the whole write set to WAL (fsync), apply to memtable.
- `Rollback()` discards the write set — cheap, nothing was published.

Snapshot isolation gives non-blocking consistent reads, the property interviewers probe.

---

## 9. Concurrency model

| Concern                     | Mechanism                                             |
|-----------------------------|-------------------------------------------------------|
| seqNum allocation           | `sync/atomic` counter                                 |
| memtable / SSTable set swap  | `sync.RWMutex` (reads take R, flush/compaction take W)|
| flush & compaction          | background goroutines, driven by channels             |
| conflict detection          | short critical section under commit lock              |
| WAL append                  | single writer goroutine or mutex-guarded append       |

`go test -race` runs in CI on every push — concurrency correctness is demonstrable, not asserted.

---

## 10. Query layer (Phase 6)

`tokenizer → parser (recursive descent) → AST → planner`. SQL subset:
`CREATE TABLE`, `INSERT`, `SELECT … WHERE`, `UPDATE`, `DELETE`.

Row mapping onto the KV engine:
```
primary:    table/<tableName>/<primaryKey>      → encoded row
secondary:  index/<tableName>/<col>/<value>/<pk> → (empty)   // second keyspace
```
`WHERE indexedCol = x` does a `Scan` over the `index/…/x/` prefix instead of a full table scan.

---

## 11. Package / module layout

```
godb/
├── go.mod
├── DESIGN.md                 ← this file
├── cmd/
│   ├── godb-server/          ← TCP server entrypoint
│   └── godb-cli/             ← REPL client
├── internal/
│   ├── memtable/             ← skip list / B-tree, Phase 1
│   ├── wal/                  ← write-ahead log + recovery, Phase 2
│   ├── sstable/              ← writer, reader, block index, Phase 3/4
│   ├── bloom/                ← hand-written bloom filter, Phase 4
│   ├── lsm/                  ← engine: memtable+wal+sstables+compaction, Phase 3
│   ├── txn/                  ← MVCC transaction manager, Phase 5
│   ├── keys/                 ← internal key encoding, shared
│   └── query/                ← tokenizer, parser, planner, Phase 6
├── godb.go                   ← public API (DB, Txn, Iterator)
├── bench/                    ← benchmark harness, Phase 8
└── .github/workflows/ci.yml  ← go test -race + go test -bench
```

---

## 12. Build phases (each ends with green tests)

| Phase | Deliverable                                             | Headline result |
|-------|--------------------------------------------------------|-----------------|
| 0     | Public API + architecture locked                        | —               |
| 1     | In-memory sorted memtable behind the API                | unit tests green|
| 2     | WAL + fsync + crash-recovery                             | crash-injection: no acked data lost |
| 3     | SSTable flush + LSM reads + background compaction        | bounded read amp |
| 4     | Bloom filter + sparse block index                        | point-lookup latency ↓ (measured) |
| 5     | MVCC snapshot isolation, Begin/Commit/Rollback           | race-free under `-race` |
| 6     | SQL-subset query layer + secondary index                 | indexed WHERE avoids full scan |
| 7     | TCP server + REPL client + wire protocol                 | it's a server, not a lib |
| 8     | Benchmark harness, fuzz/property tests, vs SQLite, README| quantified bullets |

---

## 13. Testing strategy

- **Unit** — per package, kept green from Phase 1 onward.
- **Property / fuzz** — random op sequences against a reference `map[string][]byte`; the
  engine must always agree with the model.
- **Crash-injection** — kill mid-write (via a subprocess), restart, assert every acked
  write survived and no torn record is replayed.
- **Race** — `go test -race` in CI.
- **Benchmark** — `go test -bench`: write/read throughput, p50/p99, with/without Bloom,
  before/after compaction, and against SQLite on an identical workload.
