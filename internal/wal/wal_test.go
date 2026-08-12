package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ronitraj/godb/internal/keys"
)

func TestAppendReplayRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	want := []Record{
		{Seq: 1, Kind: keys.KindSet, Key: []byte("a"), Value: []byte("1")},
		{Seq: 2, Kind: keys.KindSet, Key: []byte("b"), Value: []byte("2")},
		{Seq: 3, Kind: keys.KindDelete, Key: []byte("a"), Value: nil},
	}
	for _, r := range want {
		if err := w.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var got []Record
	if err := Replay(path, func(r Record) error { got = append(got, r); return nil }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("replayed %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Seq != want[i].Seq || got[i].Kind != want[i].Kind ||
			string(got[i].Key) != string(want[i].Key) || string(got[i].Value) != string(want[i].Value) {
			t.Errorf("record %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestReplayMissingFileIsEmpty(t *testing.T) {
	if err := Replay(filepath.Join(t.TempDir(), "absent.wal"), func(Record) error {
		t.Fatal("callback should not run for a missing file")
		return nil
	}); err != nil {
		t.Errorf("Replay of missing file = %v, want nil", err)
	}
}

func TestTornTailIsToleratedNotLost(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.wal")
	w, _ := Create(path)
	w.Append(Record{Seq: 1, Kind: keys.KindSet, Key: []byte("keep1"), Value: []byte("v1")})
	w.Append(Record{Seq: 2, Kind: keys.KindSet, Key: []byte("keep2"), Value: []byte("v2")})
	w.Close()

	// Simulate a crash mid-write by appending a truncated trailing frame.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{0x00, 0x00, 0x00, 0x40, 0x01, 0x02}) // claims 64B payload, only 6 bytes follow
	f.Close()

	var got []Record
	if err := Replay(path, func(r Record) error { got = append(got, r); return nil }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	// The two durable records must survive; the torn record is discarded.
	if len(got) != 2 || string(got[0].Key) != "keep1" || string(got[1].Key) != "keep2" {
		t.Fatalf("after torn tail, got %d records %v; want the 2 durable ones intact", len(got), got)
	}
}

func TestCorruptedRecordStopsReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.wal")
	w, _ := Create(path)
	w.Append(Record{Seq: 1, Kind: keys.KindSet, Key: []byte("good"), Value: []byte("v")})
	w.Close()

	// Flip a byte in the value to break the CRC.
	data, _ := os.ReadFile(path)
	data[len(data)-6] ^= 0xFF
	os.WriteFile(path, data, 0o644)

	count := 0
	Replay(path, func(Record) error { count++; return nil })
	if count != 0 {
		t.Errorf("expected corrupt record to be rejected, got %d records", count)
	}
}
