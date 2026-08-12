// Package sstable implements immutable, sorted on-disk tables (SSTables) — the
// persistent unit of the LSM tree. A table stores records sorted by internal
// key, grouped into fixed-size data blocks, with three trailers that make
// lookups cheap:
//
//   - a per-table Bloom filter, to reject keys that are definitely absent;
//   - a sparse block index (first key + offset of each block), to binary-search
//     to the one block that could hold a key;
//   - a fixed footer pointing at the index and Bloom sections.
//
// On-disk layout:
//
//	[ data blocks ]         each: repeated (keyLen u32, key, valLen u32, val)
//	[ index ]               repeated (keyLen u32, firstKey, offset u64, len u32)
//	[ bloom filter ]        bloom.Marshal()
//	[ footer, fixed 40B ]   indexOff u64, indexLen u32, bloomOff u64,
//	                        bloomLen u32, count u64, magic u64
package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/ronitraj/godb/internal/bloom"
	"github.com/ronitraj/godb/internal/keys"
)

const (
	blockSize  = 4096
	footerLen  = 40
	magic      = 0x676F6462_53535401 // "godbSST" + version 1
	bloomFPRate = 0.01
)

// ---- Writer -------------------------------------------------------------

// Writer builds an SSTable. Records must be added in ascending internal-key
// order (the memtable and merge iterators already yield them that way).
type Writer struct {
	f       *os.File
	bw      *bufio.Writer
	offset  uint64 // bytes written to the data section so far
	block   []byte // current in-progress block
	blockFirstKey []byte
	index   []indexEntry
	userKeys [][]byte // collected for Bloom sizing/build at Finish
	count   uint64
	lastKey []byte
}

type indexEntry struct {
	firstKey []byte
	offset   uint64
	length   uint32
}

// NewWriter creates an SSTable at path.
func NewWriter(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, bw: bufio.NewWriter(f)}, nil
}

// Add appends one record. internalKey must be strictly greater than the
// previous key added.
func (w *Writer) Add(internalKey, value []byte) error {
	if w.lastKey != nil && keys.Compare(internalKey, w.lastKey) <= 0 {
		return fmt.Errorf("sstable: keys added out of order")
	}
	w.lastKey = append(w.lastKey[:0], internalKey...)

	if w.blockFirstKey == nil {
		w.blockFirstKey = append([]byte(nil), internalKey...)
	}
	w.block = appendRecord(w.block, internalKey, value)
	w.userKeys = append(w.userKeys, append([]byte(nil), keys.UserKey(internalKey)...))
	w.count++

	if len(w.block) >= blockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *Writer) flushBlock() error {
	if len(w.block) == 0 {
		return nil
	}
	n, err := w.bw.Write(w.block)
	if err != nil {
		return err
	}
	w.index = append(w.index, indexEntry{
		firstKey: w.blockFirstKey,
		offset:   w.offset,
		length:   uint32(n),
	})
	w.offset += uint64(n)
	w.block = w.block[:0]
	w.blockFirstKey = nil
	return nil
}

// Finish flushes the final block, writes the index, Bloom filter, and footer,
// fsyncs, and closes the file.
func (w *Writer) Finish() error {
	if err := w.flushBlock(); err != nil {
		return err
	}

	// Index section.
	indexOff := w.offset
	var indexBuf []byte
	for _, e := range w.index {
		indexBuf = appendUvarintKey(indexBuf, e.firstKey)
		var tmp [12]byte
		binary.BigEndian.PutUint64(tmp[0:], e.offset)
		binary.BigEndian.PutUint32(tmp[8:], e.length)
		indexBuf = append(indexBuf, tmp[:]...)
	}
	if _, err := w.bw.Write(indexBuf); err != nil {
		return err
	}
	indexLen := uint32(len(indexBuf))
	w.offset += uint64(indexLen)

	// Bloom section.
	f := bloom.New(int(w.count), bloomFPRate)
	for _, uk := range w.userKeys {
		f.Add(uk)
	}
	bloomBuf := f.Marshal()
	bloomOff := w.offset
	if _, err := w.bw.Write(bloomBuf); err != nil {
		return err
	}
	bloomLen := uint32(len(bloomBuf))
	w.offset += uint64(bloomLen)

	// Footer.
	var footer [footerLen]byte
	binary.BigEndian.PutUint64(footer[0:], indexOff)
	binary.BigEndian.PutUint32(footer[8:], indexLen)
	binary.BigEndian.PutUint64(footer[12:], bloomOff)
	binary.BigEndian.PutUint32(footer[20:], bloomLen)
	binary.BigEndian.PutUint64(footer[24:], w.count)
	binary.BigEndian.PutUint64(footer[32:], magic)
	if _, err := w.bw.Write(footer[:]); err != nil {
		return err
	}

	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// Count reports how many records have been added.
func (w *Writer) Count() uint64 { return w.count }

func appendRecord(dst, key, val []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(key)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, key...)
	binary.BigEndian.PutUint32(hdr[:], uint32(len(val)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, val...)
	return dst
}

func appendUvarintKey(dst, key []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(key)))
	dst = append(dst, hdr[:]...)
	return append(dst, key...)
}

// ---- Reader -------------------------------------------------------------

// Reader provides point lookups and ordered iteration over an SSTable. The
// index and Bloom filter are held in memory; data blocks are read on demand.
type Reader struct {
	f      *os.File
	path   string
	index  []indexEntry
	filter *bloom.Filter
	count  uint64
}

// Open opens an SSTable for reading.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() < footerLen {
		f.Close()
		return nil, fmt.Errorf("sstable: file too small to be valid: %s", path)
	}

	var footer [footerLen]byte
	if _, err := f.ReadAt(footer[:], fi.Size()-footerLen); err != nil {
		f.Close()
		return nil, err
	}
	if got := binary.BigEndian.Uint64(footer[32:]); got != magic {
		f.Close()
		return nil, fmt.Errorf("sstable: bad magic 0x%x in %s", got, path)
	}
	indexOff := binary.BigEndian.Uint64(footer[0:])
	indexLen := binary.BigEndian.Uint32(footer[8:])
	bloomOff := binary.BigEndian.Uint64(footer[12:])
	bloomLen := binary.BigEndian.Uint32(footer[20:])
	count := binary.BigEndian.Uint64(footer[24:])

	indexBuf := make([]byte, indexLen)
	if _, err := f.ReadAt(indexBuf, int64(indexOff)); err != nil {
		f.Close()
		return nil, err
	}
	index, err := parseIndex(indexBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	bloomBuf := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomBuf, int64(bloomOff)); err != nil {
		f.Close()
		return nil, err
	}
	filter, err := bloom.Unmarshal(bloomBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &Reader{f: f, path: path, index: index, filter: filter, count: count}, nil
}

func parseIndex(b []byte) ([]indexEntry, error) {
	var out []indexEntry
	for len(b) > 0 {
		if len(b) < 4 {
			return nil, fmt.Errorf("sstable: truncated index")
		}
		klen := binary.BigEndian.Uint32(b[:4])
		b = b[4:]
		if uint32(len(b)) < klen+12 {
			return nil, fmt.Errorf("sstable: truncated index entry")
		}
		key := append([]byte(nil), b[:klen]...)
		b = b[klen:]
		off := binary.BigEndian.Uint64(b[:8])
		length := binary.BigEndian.Uint32(b[8:12])
		b = b[12:]
		out = append(out, indexEntry{firstKey: key, offset: off, length: length})
	}
	return out, nil
}

// Path returns the file path backing this reader.
func (r *Reader) Path() string { return r.path }

// Count reports the number of records in the table.
func (r *Reader) Count() uint64 { return r.count }

// MayContain reports whether userKey might be present, per the Bloom filter.
func (r *Reader) MayContain(userKey []byte) bool { return r.filter.MayContain(userKey) }

// Get returns the newest version of userKey with sequence <= snapshot. found is
// false if the key is absent from this table; a returned KindDelete tombstone
// still counts as found (it terminates the engine's cross-level search).
func (r *Reader) Get(userKey []byte, snapshot keys.SeqNum) (value []byte, kind keys.Kind, found bool, err error) {
	if !r.filter.MayContain(userKey) {
		return nil, 0, false, nil
	}
	target := keys.Encode(keys.InternalKey{UserKey: userKey, Seq: snapshot, Kind: keys.KindDelete})
	it := r.NewIterator()
	defer it.Close()
	if err := it.Seek(target); err != nil {
		return nil, 0, false, err
	}
	if !it.Valid() {
		return nil, 0, false, nil
	}
	ik, err := keys.Decode(it.Key())
	if err != nil {
		return nil, 0, false, err
	}
	if !bytesEqual(ik.UserKey, userKey) {
		return nil, 0, false, nil
	}
	return append([]byte(nil), it.Value()...), ik.Kind, true, nil
}

// Close releases the underlying file.
func (r *Reader) Close() error { return r.f.Close() }

// ---- Iterator -----------------------------------------------------------

// Iterator walks an SSTable's records in ascending internal-key order.
type Iterator struct {
	r        *Reader
	blockIdx int
	records  []record
	pos      int
	err      error
}

type record struct {
	key []byte
	val []byte
}

// NewIterator returns an iterator positioned before the first record.
func (r *Reader) NewIterator() *Iterator {
	return &Iterator{r: r, blockIdx: -1, pos: -1}
}

func (it *Iterator) loadBlock(i int) bool {
	if i < 0 || i >= len(it.r.index) {
		it.records = nil
		return false
	}
	e := it.r.index[i]
	buf := make([]byte, e.length)
	if _, err := it.r.f.ReadAt(buf, int64(e.offset)); err != nil {
		it.err = err
		return false
	}
	recs, err := decodeBlock(buf)
	if err != nil {
		it.err = err
		return false
	}
	it.records = recs
	it.blockIdx = i
	return true
}

func decodeBlock(b []byte) ([]record, error) {
	var out []record
	for len(b) > 0 {
		if len(b) < 4 {
			return nil, fmt.Errorf("sstable: truncated record header")
		}
		klen := binary.BigEndian.Uint32(b[:4])
		b = b[4:]
		if uint32(len(b)) < klen {
			return nil, fmt.Errorf("sstable: truncated record key")
		}
		key := b[:klen]
		b = b[klen:]
		if len(b) < 4 {
			return nil, fmt.Errorf("sstable: truncated value header")
		}
		vlen := binary.BigEndian.Uint32(b[:4])
		b = b[4:]
		if uint32(len(b)) < vlen {
			return nil, fmt.Errorf("sstable: truncated value")
		}
		val := b[:vlen]
		b = b[vlen:]
		out = append(out, record{key: key, val: val})
	}
	return out, nil
}

// SeekToFirst positions the iterator at the first record.
func (it *Iterator) SeekToFirst() {
	it.err = nil
	if len(it.r.index) == 0 {
		it.blockIdx = -1
		it.records = nil
		it.pos = -1
		return
	}
	if it.loadBlock(0) {
		it.pos = 0
	}
}

// Seek positions the iterator at the first record whose key is >= target.
func (it *Iterator) Seek(target []byte) error {
	it.err = nil
	// Binary search the index for the last block whose first key is <= target.
	idx := it.r.index
	lo, hi := 0, len(idx)
	for lo < hi {
		mid := (lo + hi) / 2
		if keys.Compare(idx[mid].firstKey, target) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	start := lo - 1
	if start < 0 {
		start = 0 // target precedes every block; begin at the first
	}
	for b := start; b < len(idx); b++ {
		if !it.loadBlock(b) {
			return it.err
		}
		for i, rec := range it.records {
			if keys.Compare(rec.key, target) >= 0 {
				it.pos = i
				return nil
			}
		}
	}
	// No record >= target.
	it.records = nil
	it.pos = -1
	it.blockIdx = len(idx)
	return nil
}

// Valid reports whether the iterator is positioned at a record.
func (it *Iterator) Valid() bool {
	return it.err == nil && it.records != nil && it.pos >= 0 && it.pos < len(it.records)
}

// Next advances to the following record.
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	it.pos++
	if it.records != nil && it.pos < len(it.records) {
		return true
	}
	// Advance to the next block.
	for b := it.blockIdx + 1; b < len(it.r.index); b++ {
		if !it.loadBlock(b) {
			return false
		}
		if len(it.records) > 0 {
			it.pos = 0
			return true
		}
	}
	it.records = nil
	it.pos = -1
	return false
}

// Key returns the current record's internal key.
func (it *Iterator) Key() []byte { return it.records[it.pos].key }

// Value returns the current record's value.
func (it *Iterator) Value() []byte { return it.records[it.pos].val }

// Err returns any error encountered during iteration.
func (it *Iterator) Err() error { return it.err }

// Close releases iterator resources (currently a no-op; the Reader owns the file).
func (it *Iterator) Close() error { return nil }

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
