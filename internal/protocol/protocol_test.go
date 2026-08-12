package protocol

import (
	"net"
	"strings"
	"testing"

	"github.com/ronitraj/godb"
)

func TestExecuteKVAndSQL(t *testing.T) {
	db, err := godb.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if got := Execute(db, "PUT foo bar"); got != "OK" {
		t.Errorf("PUT = %q, want OK", got)
	}
	if got := Execute(db, "GET foo"); got != "bar" {
		t.Errorf("GET = %q, want bar", got)
	}
	if got := Execute(db, "GET missing"); got != "(nil)" {
		t.Errorf("GET missing = %q, want (nil)", got)
	}
	if got := Execute(db, "DEL foo"); got != "OK" {
		t.Errorf("DEL = %q, want OK", got)
	}
	if got := Execute(db, "GET foo"); got != "(nil)" {
		t.Errorf("GET after DEL = %q, want (nil)", got)
	}

	Execute(db, "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))")
	Execute(db, "INSERT INTO users (id, name) VALUES (1, 'alice')")
	out := Execute(db, "SELECT name FROM users WHERE id = 1")
	if !strings.Contains(out, "alice") {
		t.Errorf("SELECT output = %q, want it to contain alice", out)
	}
}

func TestFrameRoundTripOverPipe(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	want := "SELECT * FROM t"
	go func() {
		WriteFrame(c1, []byte(want))
	}()
	got, err := ReadFrame(c2)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if string(got) != want {
		t.Errorf("frame round trip = %q, want %q", got, want)
	}
}
