package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6/memfs"
	xetstorage "github.com/wzshiming/xet/storage"
	xetmemory "github.com/wzshiming/xet/storage/memory"
)

func TestNewStorageDefaultXETStorage(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts func(root string) []Option
	}{
		{"local fs", func(root string) []Option { return []Option{WithRootDir(root)} }},
		{"custom fs", func(root string) []Option { return []Option{WithRootDir(root), WithFilesystem(memfs.New())} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			st, err := NewStorage(tc.opts(root)...)
			if err != nil {
				t.Fatalf("NewStorage: %v", err)
			}
			if got, want := st.XETDir(), filepath.Join(root, "xet"); got != want {
				t.Fatalf("XETDir() = %q, want %q", got, want)
			}
			if got, want := st.XETSpoolDir(), filepath.Join(root, "xet", "spool"); got != want {
				t.Fatalf("XETSpoolDir() = %q, want %q", got, want)
			}
			if got, want := st.XETMirrorDir(), filepath.Join(root, "xet", "mirror"); got != want {
				t.Fatalf("XETMirrorDir() = %q, want %q", got, want)
			}
			if st.XETStorage() == nil {
				t.Fatal("XETStorage() = nil, want default local store")
			}
			for _, dir := range []string{
				"xorbs", "shards", filepath.Join("index", "files"), filepath.Join("index", "chunks"), filepath.Join("index", "sha256"),
				"mirror", "spool",
			} {
				fi, err := os.Stat(filepath.Join(root, "xet", dir))
				if err != nil || !fi.IsDir() {
					t.Errorf("stat xet/%s: %v, want directory", dir, err)
				}
			}
			for _, dir := range []string{"storage", "chunks"} {
				if _, err := os.Stat(filepath.Join(root, "xet", dir)); !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("stat xet/%s: %v, want not exist", dir, err)
				}
			}
		})
	}
}

func TestNewStorageWithXETStorage(t *testing.T) {
	xs := xetmemory.NewStorage()
	for _, tc := range []struct {
		name string
		opts func(root string) []Option
	}{
		{"before root", func(root string) []Option { return []Option{WithXETStorage(xs), WithRootDir(root)} }},
		{"after root", func(root string) []Option { return []Option{WithRootDir(root), WithXETStorage(xs)} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			st, err := NewStorage(tc.opts(root)...)
			if err != nil {
				t.Fatalf("NewStorage: %v", err)
			}
			if got := st.XETStorage(); got != xetstorage.Storage(xs) {
				t.Fatalf("XETStorage() = %v, want injected store", got)
			}
			if _, err := os.Stat(filepath.Join(root, "xet", "xorbs")); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("stat root/xet/xorbs: %v, want not exist", err)
			}
			for _, dir := range []string{"mirror", "spool"} {
				fi, err := os.Stat(filepath.Join(root, "xet", dir))
				if err != nil || !fi.IsDir() {
					t.Errorf("stat xet/%s: %v, want directory", dir, err)
				}
			}
		})
	}
}

func TestNewStorageXETStorageError(t *testing.T) {
	root := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(root, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := NewStorage(WithRootDir(root))
	if err == nil {
		t.Fatalf("NewStorage(regular-file root) = %v, want error", st)
	}
	if !strings.HasPrefix(err.Error(), "create xet storage: ") {
		t.Fatalf("error = %q, want prefix %q", err, "create xet storage: ")
	}
	if _, ok := errors.AsType[*fs.PathError](err); !ok {
		t.Fatalf("error = %v, want wrapped *fs.PathError", err)
	}
}

func TestNewStorageXETCacheError(t *testing.T) {
	for _, dir := range []string{"mirror", "spool"} {
		t.Run(dir, func(t *testing.T) {
			root := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, "xet"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "xet", dir), nil, 0o644); err != nil {
				t.Fatal(err)
			}
			st, err := NewStorage(WithRootDir(root), WithXETStorage(xetmemory.NewStorage()))
			if err == nil || st != nil {
				t.Fatalf("NewStorage(regular-file xet/%s) = %v, %v; want nil, error", dir, st, err)
			}
			if _, ok := errors.AsType[*fs.PathError](err); !ok {
				t.Fatalf("error = %v, want wrapped *fs.PathError", err)
			}
		})
	}
}
