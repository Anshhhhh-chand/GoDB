package crashtest

import (
	"bufio"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ronitraj/godb"
)

// TestCrashInjectionNoAcknowledgedDataLost starts a child process that writes
// keys with fsync durability, kills it mid-write to simulate a crash, then
// reopens the database and asserts that every write the child *acknowledged*
// (printed to stdout after Put returned) survived. This is the headline
// durability guarantee of the write-ahead log.
func TestCrashInjectionNoAcknowledgedDataLost(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping crash-injection test in -short mode")
	}
	dir := t.TempDir()

	// Build the crasher helper.
	bin := filepath.Join(t.TempDir(), "crasher")
	build := exec.Command("go", "build", "-o", bin, "github.com/ronitraj/godb/internal/crashtest/crasher")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building crasher: %v\n%s", err, out)
	}

	cmd := exec.Command(bin, dir, "100000")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Collect acknowledged keys until we have a healthy batch, then kill hard
	// (SIGKILL) to guarantee no graceful shutdown or extra flushing.
	acked := make(map[string]bool)
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		acked[strings.TrimSpace(scanner.Text())] = true
		if len(acked) >= 5000 {
			break
		}
	}
	// Give the child a brief moment to keep writing, then kill mid-flight.
	time.Sleep(20 * time.Millisecond)
	_ = cmd.Process.Kill()
	_ = cmd.Wait()

	if len(acked) < 5000 {
		t.Fatalf("child acknowledged only %d writes before crash; test inconclusive", len(acked))
	}

	// Reopen and verify every acknowledged key is present with the right value.
	db, err := godb.Open(dir)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer db.Close()

	missing := 0
	for k := range acked {
		v, found, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		want := "val" + strings.TrimPrefix(k, "key")
		if !found || string(v) != want {
			missing++
			if missing <= 5 {
				t.Errorf("acknowledged key %s lost or wrong after crash: found=%v value=%q", k, found, v)
			}
		}
	}
	if missing > 0 {
		t.Fatalf("%d/%d acknowledged writes were lost after crash — durability violated", missing, len(acked))
	}
	t.Logf("crash-injection: all %d acknowledged writes survived", len(acked))
}
