// Package keys defines the internal key encoding shared across the storage
// engine. Every stored record is addressed by an *internal key* that carries
// the user key together with an MVCC sequence number and a record kind.
//
// The encoding is chosen so that a plain bytewise comparison of two internal
// keys yields the ordering the LSM engine needs:
//
//   - user keys sort ascending, so identical user keys are grouped together;
//   - within one user key, the *newest* version (largest sequence number)
//     sorts first, achieved by storing the bitwise-inverted sequence number.
//
// Layout:
//
//	userKey ++ (^seqNum as 8B big-endian) ++ kind(1B)
package keys

import (
	"encoding/binary"
	"fmt"
)

// Kind distinguishes a value write from a deletion tombstone.
type Kind uint8

const (
	// KindDelete marks a tombstone. Ordered before KindSet so that at equal
	// (userKey, seqNum) a delete would win, though equal seqNums never occur
	// in practice because sequence numbers are globally unique.
	KindDelete Kind = 0
	// KindSet marks a normal value write.
	KindSet Kind = 1
)

func (k Kind) String() string {
	switch k {
	case KindDelete:
		return "DELETE"
	case KindSet:
		return "SET"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

// trailerLen is the number of trailer bytes appended after the user key:
// 8 bytes for the inverted sequence number plus 1 byte for the kind.
const trailerLen = 9

// SeqNum is the MVCC sequence number / version timestamp. It is allocated
// monotonically and uniquely for every mutation.
type SeqNum uint64

// InternalKey is the decoded form of an on-disk/in-memory key.
type InternalKey struct {
	UserKey []byte
	Seq     SeqNum
	Kind    Kind
}

// Encode serializes ik into a freshly allocated byte slice using the layout
// described in the package comment.
func Encode(ik InternalKey) []byte {
	buf := make([]byte, len(ik.UserKey)+trailerLen)
	n := copy(buf, ik.UserKey)
	// Store the inverted sequence number so larger seqNums sort first.
	binary.BigEndian.PutUint64(buf[n:], ^uint64(ik.Seq))
	buf[n+8] = byte(ik.Kind)
	return buf
}

// EncodeInto is like Encode but appends into dst, returning the extended slice.
// It avoids an allocation on hot paths where the caller owns a scratch buffer.
func EncodeInto(dst, userKey []byte, seq SeqNum, kind Kind) []byte {
	dst = append(dst, userKey...)
	var trailer [trailerLen]byte
	binary.BigEndian.PutUint64(trailer[:8], ^uint64(seq))
	trailer[8] = byte(kind)
	return append(dst, trailer[:]...)
}

// Decode parses an encoded internal key. It returns an error if the buffer is
// too short to contain a trailer.
func Decode(b []byte) (InternalKey, error) {
	if len(b) < trailerLen {
		return InternalKey{}, fmt.Errorf("keys: internal key too short: %d bytes", len(b))
	}
	split := len(b) - trailerLen
	inverted := binary.BigEndian.Uint64(b[split : split+8])
	return InternalKey{
		UserKey: b[:split],
		Seq:     SeqNum(^inverted),
		Kind:    Kind(b[split+8]),
	}, nil
}

// UserKey returns just the user-key prefix of an encoded internal key without
// fully decoding it. It panics if b is too short — callers hold valid keys.
func UserKey(b []byte) []byte {
	return b[:len(b)-trailerLen]
}

// Compare orders two encoded internal keys: user key ascending, then (via the
// inverted-sequence trailer) newest version first.
//
// It deliberately does not compare the raw concatenation bytewise. Because the
// trailer is fixed-length and user keys are variable-length, a naive bytewise
// compare would pit a short key's trailer against a longer key's user-key
// bytes and mis-order them. Splitting the trailer off first is what makes the
// ordering correct for arbitrary user keys.
func Compare(a, b []byte) int {
	aSplit, bSplit := len(a)-trailerLen, len(b)-trailerLen
	if c := bytewiseCompare(a[:aSplit], b[:bSplit]); c != 0 {
		return c
	}
	// User keys equal: compare trailers (inverted seq, then kind) bytewise,
	// which yields newest-version-first.
	return bytewiseCompare(a[aSplit:], b[bSplit:])
}

func bytewiseCompare(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}
