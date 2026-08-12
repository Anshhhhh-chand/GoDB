package keys

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []InternalKey{
		{UserKey: []byte("a"), Seq: 1, Kind: KindSet},
		{UserKey: []byte(""), Seq: 0, Kind: KindDelete},
		{UserKey: []byte("longer-key-value"), Seq: 1 << 40, Kind: KindSet},
	}
	for _, want := range cases {
		got, err := Decode(Encode(want))
		if err != nil {
			t.Fatalf("Decode(%v): %v", want, err)
		}
		if !bytes.Equal(got.UserKey, want.UserKey) || got.Seq != want.Seq || got.Kind != want.Kind {
			t.Errorf("round trip: got %+v, want %+v", got, want)
		}
	}
}

func TestOrderingUserKeyAscending(t *testing.T) {
	a := Encode(InternalKey{UserKey: []byte("apple"), Seq: 5, Kind: KindSet})
	b := Encode(InternalKey{UserKey: []byte("banana"), Seq: 5, Kind: KindSet})
	if Compare(a, b) >= 0 {
		t.Errorf("expected apple < banana")
	}
}

func TestOrderingNewestVersionFirst(t *testing.T) {
	// Within the same user key, a larger sequence number must sort earlier so
	// that a forward seek lands on the newest version first.
	newer := Encode(InternalKey{UserKey: []byte("k"), Seq: 100, Kind: KindSet})
	older := Encode(InternalKey{UserKey: []byte("k"), Seq: 10, Kind: KindSet})
	if Compare(newer, older) >= 0 {
		t.Errorf("expected newer version (seq=100) to sort before older (seq=10)")
	}
}

func TestUserKeyPrefix(t *testing.T) {
	enc := Encode(InternalKey{UserKey: []byte("hello"), Seq: 7, Kind: KindSet})
	if got := UserKey(enc); !bytes.Equal(got, []byte("hello")) {
		t.Errorf("UserKey = %q, want %q", got, "hello")
	}
}

func TestDecodeTooShort(t *testing.T) {
	if _, err := Decode([]byte{1, 2, 3}); err == nil {
		t.Errorf("expected error decoding short buffer")
	}
}
