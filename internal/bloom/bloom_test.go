package bloom

import (
	"fmt"
	"testing"
)

func TestNoFalseNegatives(t *testing.T) {
	f := New(1000, 0.01)
	for i := 0; i < 1000; i++ {
		f.Add([]byte(fmt.Sprintf("key-%d", i)))
	}
	for i := 0; i < 1000; i++ {
		if !f.MayContain([]byte(fmt.Sprintf("key-%d", i))) {
			t.Fatalf("false negative for key-%d: a Bloom filter must never report absent for a present key", i)
		}
	}
}

func TestFalsePositiveRateReasonable(t *testing.T) {
	n := 2000
	f := New(n, 0.01)
	for i := 0; i < n; i++ {
		f.Add([]byte(fmt.Sprintf("present-%d", i)))
	}
	fp := 0
	trials := 10000
	for i := 0; i < trials; i++ {
		if f.MayContain([]byte(fmt.Sprintf("absent-%d", i))) {
			fp++
		}
	}
	rate := float64(fp) / float64(trials)
	// Allow generous slack over the 1% target; we only guard against a broken
	// filter (e.g. everything matching).
	if rate > 0.05 {
		t.Errorf("false-positive rate %.3f exceeds 5%%, filter is not discriminating", rate)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	f := New(500, 0.01)
	for i := 0; i < 500; i++ {
		f.Add([]byte(fmt.Sprintf("k%d", i)))
	}
	g, err := Unmarshal(f.Marshal())
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for i := 0; i < 500; i++ {
		if !g.MayContain([]byte(fmt.Sprintf("k%d", i))) {
			t.Fatalf("round-tripped filter lost key k%d", i)
		}
	}
}

func TestUnmarshalRejectsCorrupt(t *testing.T) {
	if _, err := Unmarshal([]byte{1, 2, 3}); err == nil {
		t.Errorf("expected error on short buffer")
	}
}
