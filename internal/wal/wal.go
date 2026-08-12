// Package wal implements the write-ahead log that gives GoDB its durability
// guarantee: every mutation is appended and fsync'd before the write is
// acknowledged, so an acknowledged write survives a crash.
//
// Each record is length-prefixed and CRC-checked:
//
//	| payloadLen u32 | payload | crc32(payload) u32 |
//	payload = seq u64 | kind u8 | keyLen u32 | key | valLen u32 | val
//
// On recovery the log is replayed record by record. A short read or a CRC
// mismatch marks the end of the durable prefix — that is the expected shape of
// a process killed mid-write, and everything before it is intact — so replay
// stops cleanly there rather than reporting corruption.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/ronitraj/godb/internal/keys"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record is a single logged mutation.
type Record struct {
	Seq   keys.SeqNum
	Kind  keys.Kind
	Key   []byte
	Value []byte
}

// Writer appends records to a log file.
type Writer struct {
	f  *os.File
	bw *bufio.Writer
}

// Create truncates or creates the log at path for writing.
func Create(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, bw: bufio.NewWriter(f)}, nil
}

// Append writes r to the log buffer. It does not fsync; call Sync to make the
// appended records durable before acknowledging the write(s).
func (w *Writer) Append(r Record) error {
	payloadLen := 8 + 1 + 4 + len(r.Key) + 4 + len(r.Value)
	buf := make([]byte, 4+payloadLen+4)

	binary.BigEndian.PutUint32(buf[0:], uint32(payloadLen))
	p := buf[4 : 4+payloadLen]
	binary.BigEndian.PutUint64(p[0:], uint64(r.Seq))
	p[8] = byte(r.Kind)
	binary.BigEndian.PutUint32(p[9:], uint32(len(r.Key)))
	n := 13 + copy(p[13:], r.Key)
	binary.BigEndian.PutUint32(p[n:], uint32(len(r.Value)))
	copy(p[n+4:], r.Value)

	crc := crc32.Checksum(p, crcTable)
	binary.BigEndian.PutUint32(buf[4+payloadLen:], crc)

	_, err := w.bw.Write(buf)
	return err
}

// Sync flushes buffered records to the OS and fsyncs the file. After Sync
// returns nil, the appended records are durable.
func (w *Writer) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close syncs and closes the log.
func (w *Writer) Close() error {
	if err := w.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// Replay reads the durable prefix of the log at path, invoking fn for each
// intact record in order. A truncated or CRC-failed trailing record is treated
// as the (expected) end of the log after a crash, not an error. A missing file
// replays as empty.
func Replay(path string, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // clean end (or torn length prefix)
			}
			return err
		}
		payloadLen := binary.BigEndian.Uint32(lenBuf[:])
		frame := make([]byte, int(payloadLen)+4)
		if _, err := io.ReadFull(br, frame); err != nil {
			return nil // torn payload/crc: end of durable prefix
		}
		payload := frame[:payloadLen]
		crc := binary.BigEndian.Uint32(frame[payloadLen:])
		if crc32.Checksum(payload, crcTable) != crc {
			return nil // corrupt trailing record: stop at durable prefix
		}
		rec, ok := decode(payload)
		if !ok {
			return nil
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

func decode(p []byte) (Record, bool) {
	if len(p) < 13 {
		return Record{}, false
	}
	seq := keys.SeqNum(binary.BigEndian.Uint64(p[0:]))
	kind := keys.Kind(p[8])
	klen := binary.BigEndian.Uint32(p[9:])
	if uint32(len(p)) < 13+klen+4 {
		return Record{}, false
	}
	key := append([]byte(nil), p[13:13+klen]...)
	rest := p[13+klen:]
	vlen := binary.BigEndian.Uint32(rest[0:])
	if uint32(len(rest)) < 4+vlen {
		return Record{}, false
	}
	val := append([]byte(nil), rest[4:4+vlen]...)
	return Record{Seq: seq, Kind: kind, Key: key, Value: val}, true
}
