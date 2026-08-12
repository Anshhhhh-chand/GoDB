// Package protocol defines GoDB's tiny length-prefixed wire protocol and the
// command interpreter shared by the server and REPL client.
//
// Framing: each message is a 4-byte big-endian length followed by that many
// bytes of UTF-8 payload. Requests are command lines; responses are text.
//
// Commands: the KV verbs GET/PUT/DEL/SCAN operate directly on the engine; any
// other line is executed as a SQL statement through the query layer.
package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/ronitraj/godb"
)

const maxFrame = 64 << 20 // 64 MiB guard against absurd length prefixes

// WriteFrame writes a length-prefixed payload.
func WriteFrame(w io.Writer, payload []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// ReadFrame reads a single length-prefixed payload.
func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrame {
		return nil, fmt.Errorf("protocol: frame too large: %d bytes", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// Execute runs a single command line against db and returns the text response.
func Execute(db *godb.DB, line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}
	verb := strings.ToUpper(firstWord(line))
	switch verb {
	case "GET":
		return execGet(db, line)
	case "PUT", "SET":
		return execPut(db, line)
	case "DEL", "DELETE":
		// DELETE FROM ... is SQL; a bare "DEL key" / "DELETE key" is a KV op.
		if verb == "DELETE" && strings.Contains(strings.ToUpper(line), "FROM") {
			return execSQL(db, line)
		}
		return execDel(db, line)
	case "SCAN":
		return execScan(db, line)
	default:
		return execSQL(db, line)
	}
}

func firstWord(s string) string {
	if i := strings.IndexAny(s, " \t"); i >= 0 {
		return s[:i]
	}
	return s
}

func execGet(db *godb.DB, line string) string {
	parts := strings.SplitN(line, " ", 2)
	if len(parts) != 2 {
		return "ERR usage: GET <key>"
	}
	v, found, err := db.Get([]byte(strings.TrimSpace(parts[1])))
	if err != nil {
		return "ERR " + err.Error()
	}
	if !found {
		return "(nil)"
	}
	return string(v)
}

func execPut(db *godb.DB, line string) string {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 {
		return "ERR usage: PUT <key> <value>"
	}
	if err := db.Put([]byte(parts[1]), []byte(parts[2])); err != nil {
		return "ERR " + err.Error()
	}
	return "OK"
}

func execDel(db *godb.DB, line string) string {
	parts := strings.SplitN(line, " ", 2)
	if len(parts) != 2 {
		return "ERR usage: DEL <key>"
	}
	if err := db.Delete([]byte(strings.TrimSpace(parts[1]))); err != nil {
		return "ERR " + err.Error()
	}
	return "OK"
}

func execScan(db *godb.DB, line string) string {
	fields := strings.Fields(line)
	var start, end []byte
	if len(fields) >= 2 && fields[1] != "-" {
		start = []byte(fields[1])
	}
	if len(fields) >= 3 && fields[2] != "-" {
		end = []byte(fields[2])
	}
	it, err := db.Scan(start, end)
	if err != nil {
		return "ERR " + err.Error()
	}
	defer it.Close()
	var sb strings.Builder
	n := 0
	for it.Next() {
		fmt.Fprintf(&sb, "%s = %s\n", it.Key(), it.Value())
		n++
	}
	if n == 0 {
		return "(empty range)"
	}
	return strings.TrimRight(sb.String(), "\n")
}

func execSQL(db *godb.DB, line string) string {
	res, err := db.Query(line)
	if err != nil {
		return "ERR " + err.Error()
	}
	switch {
	case res.Columns != nil:
		var sb strings.Builder
		sb.WriteString(strings.Join(res.Columns, " | "))
		sb.WriteString("\n")
		for _, row := range res.Rows {
			sb.WriteString(strings.Join(row, " | "))
			sb.WriteString("\n")
		}
		fmt.Fprintf(&sb, "(%d row%s)", len(res.Rows), plural(len(res.Rows)))
		return sb.String()
	case res.Message != "":
		return res.Message
	default:
		return fmt.Sprintf("%d row%s affected", res.RowsAffected, plural(res.RowsAffected))
	}
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
