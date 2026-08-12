// Package bloom implements a small, hand-written Bloom filter used to skip
// SSTables that cannot possibly contain a key. It uses a single FNV-1a hash
// split into two 32-bit halves and combined via double hashing to synthesize
// k independent hash functions (Kirsch–Mitzenmacher), which is both fast and
// avoids pulling in a hashing dependency.
package bloom

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

// Filter is an immutable-after-build Bloom filter.
type Filter struct {
	bits []byte
	m    uint32 // number of bits
	k    uint32 // number of hash functions
}

// New sizes a filter for n expected entries at the target false-positive rate p
// (e.g. 0.01 for 1%). It never returns fewer than 1 bit or 1 hash function.
func New(n int, p float64) *Filter {
	if n < 1 {
		n = 1
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}
	// Optimal bit count m = -n·ln(p) / (ln2)² and hash count k = (m/n)·ln2.
	ln2 := math.Ln2
	m := uint32(math.Ceil(-float64(n) * math.Log(p) / (ln2 * ln2)))
	if m < 1 {
		m = 1
	}
	k := uint32(math.Round(float64(m) / float64(n) * ln2))
	if k < 1 {
		k = 1
	}
	return &Filter{bits: make([]byte, (m+7)/8), m: m, k: k}
}

func hashes(key []byte) (h1, h2 uint32) {
	h := fnv.New64a()
	h.Write(key)
	sum := h.Sum64()
	return uint32(sum), uint32(sum >> 32)
}

// Add records key in the filter.
func (f *Filter) Add(key []byte) {
	h1, h2 := hashes(key)
	for i := uint32(0); i < f.k; i++ {
		bit := (h1 + i*h2) % f.m
		f.bits[bit/8] |= 1 << (bit % 8)
	}
}

// MayContain reports whether key might be present. False positives are
// possible; false negatives are not — a false result is definitive.
func (f *Filter) MayContain(key []byte) bool {
	h1, h2 := hashes(key)
	for i := uint32(0); i < f.k; i++ {
		bit := (h1 + i*h2) % f.m
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// Marshal serializes the filter as: m(u32) ++ k(u32) ++ bits.
func (f *Filter) Marshal() []byte {
	out := make([]byte, 8+len(f.bits))
	binary.BigEndian.PutUint32(out[0:], f.m)
	binary.BigEndian.PutUint32(out[4:], f.k)
	copy(out[8:], f.bits)
	return out
}

// Unmarshal parses a filter previously produced by Marshal.
func Unmarshal(b []byte) (*Filter, error) {
	if len(b) < 8 {
		return nil, fmt.Errorf("bloom: buffer too short: %d bytes", len(b))
	}
	m := binary.BigEndian.Uint32(b[0:])
	k := binary.BigEndian.Uint32(b[4:])
	bits := make([]byte, len(b)-8)
	copy(bits, b[8:])
	if int((m+7)/8) != len(bits) {
		return nil, fmt.Errorf("bloom: bit length mismatch: m=%d implies %d bytes, got %d", m, (m+7)/8, len(bits))
	}
	return &Filter{bits: bits, m: m, k: k}, nil
}
