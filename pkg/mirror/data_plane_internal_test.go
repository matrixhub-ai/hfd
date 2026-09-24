package mirror

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	xetclient "github.com/wzshiming/xet/client"

	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// Identical targets collapse after one canonicalization and the newest comes first.
func TestRegisterObjectDedupesTargets(t *testing.T) {
	st, err := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	client, err := xetclient.NewClient(xetclient.WithCacheDir(filepath.Join(st.XETDir(), "chunks")))
	if err != nil {
		t.Fatalf("new xet client: %v", err)
	}
	m, err := NewMirror(WithXETStorage(st.XETStorage()), WithXETClient(client))
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}

	oid := strings.Repeat("0", 64)
	commit := strings.Repeat("a", 40)
	m.RegisterObject(oid, "/org/repo.git", commit, "model.bin", 1)
	m.RegisterObject(oid, "org/repo", commit, "model.bin", 1)
	m.RegisterObject(oid, "org/./repo.git", commit, "model.bin", 1)
	m.RegisterObject(oid, "org/other", commit, "model.bin", 1)

	want := []resolveTarget{
		{repoName: "org/other", commit: commit, path: "model.bin", size: 1},
		{repoName: "org/repo", commit: commit, path: "model.bin", size: 1},
	}
	if got := m.oidIndex[oid]; !reflect.DeepEqual(got, want) {
		t.Fatalf("oidIndex[%s] = %+v, want %+v", oid, got, want)
	}

	// A scanned target is stored verbatim: org/repo.git is a repository named repo.git.
	dotGit := strings.Repeat("1", 64)
	scanned := resolveTarget{repoName: "org/repo.git", commit: commit, path: "model.bin", size: 1}
	m.registerTarget(dotGit, scanned)
	m.RegisterObject(dotGit, "/org/repo.git.git", commit, "model.bin", 1)
	if got := m.oidIndex[dotGit]; !reflect.DeepEqual(got, []resolveTarget{scanned}) {
		t.Fatalf("oidIndex[%s] = %+v, want only %+v", dotGit, got, scanned)
	}
}
