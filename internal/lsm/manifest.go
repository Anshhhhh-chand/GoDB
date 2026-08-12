package lsm

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// manifest is the durable record of engine state that is not itself in an
// SSTable or WAL: which tables exist (and at what level), the next file number
// to allocate, and the highest sequence number committed. It is rewritten
// atomically (temp file + fsync + rename) on every flush and compaction, so a
// crash always leaves either the old or the new manifest intact.
type manifest struct {
	NextFileNum uint64      `json:"next_file_num"`
	LastSeq     uint64      `json:"last_seq"`
	Tables      []tableMeta `json:"tables"`
}

type tableMeta struct {
	Num   uint64 `json:"num"`
	Level int    `json:"level"`
}

const manifestName = "MANIFEST"

func manifestPath(dir string) string { return filepath.Join(dir, manifestName) }

func loadManifest(dir string) (*manifest, error) {
	b, err := os.ReadFile(manifestPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return &manifest{NextFileNum: 1}, nil
		}
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m.NextFileNum == 0 {
		m.NextFileNum = 1
	}
	return &m, nil
}

// writeManifest atomically replaces the manifest on disk.
func writeManifest(dir string, m *manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := manifestPath(dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, manifestPath(dir)); err != nil {
		return err
	}
	return syncDir(dir)
}

// syncDir fsyncs a directory so that a rename within it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
