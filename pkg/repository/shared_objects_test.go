package repository

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/go-git/go-git/v6/storage/filesystem/dotgit"
	"github.com/go-git/go-git/v6/storage/memory"
)

type renameFailFS struct {
	billy.Filesystem
}

func (fs renameFailFS) Rename(oldpath, newpath string) error {
	return errors.New("object rename failed")
}

func TestSharedPackWriterSurfacesCloseErrors(t *testing.T) {
	source := memory.NewStorage()
	blob := source.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	writer, err := blob.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("pack content\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	hash, err := source.SetEncodedObject(blob)
	if err != nil {
		t.Fatal(err)
	}
	var pack bytes.Buffer
	if _, err := packfile.NewEncoder(&pack, source, false).Encode([]plumbing.Hash{hash}, 10); err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "pack-*")
	if err != nil {
		t.Fatal(err)
	}
	local := filesystem.NewStorage(memfs.New(), cache.NewObjectLRUDefault())
	packWriter := &sharedPackWriter{file: file, storer: &sharedStorer{
		Storer: local, local: local,
		objects: filesystem.NewObjectStorage(dotgit.New(renameFailFS{memfs.New()}), cache.NewObjectLRUDefault()),
	}}
	if _, err := packWriter.Write(pack.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := packWriter.Close(); err == nil {
		t.Fatal("pack Close succeeded despite object rename failure")
	}
}

func TestSharedObjectsOpenMissingRepoWritesNothing(t *testing.T) {
	dataFS := osfs.New(t.TempDir())
	bound := BindSharedObjects(dataFS, "/repositories", "/git/sha1")
	if _, err := Open(bound, "/org/missing.git"); !errors.Is(err, ErrRepositoryNotExists) {
		t.Fatalf("Open error = %v, want ErrRepositoryNotExists", err)
	}
	if _, err := dataFS.Stat("/repositories/org"); !os.IsNotExist(err) {
		t.Fatalf("repository parent: want not exist, got %v", err)
	}
}

func TestSharedObjectsConcurrentWriters(t *testing.T) {
	dataFS := osfs.New(t.TempDir())
	bound := BindSharedObjects(dataFS, "/repositories", "/git/sha1")
	repos := make([]*Repository, 8)
	for index := range repos {
		repo, err := Init(t.Context(), bound, fmt.Sprintf("/org/repo-%d.git", index), "main")
		if err != nil {
			t.Fatal(err)
		}
		repos[index] = repo
	}
	ctx := t.Context()
	shared := []byte("same bytes\n")
	errorsCh := make(chan error, len(repos))
	var writers sync.WaitGroup
	for index, repo := range repos {
		writers.Add(1)
		go func() {
			defer writers.Done()
			// The same blob hash is written by every goroutine at once.
			_, err := repo.CreateCommit(ctx, "main", "add shared", "Test", "test@example.com",
				[]CommitOperation{{Type: CommitOperationAdd, Path: "shared.txt", Content: shared}}, "")
			if err != nil {
				errorsCh <- fmt.Errorf("repo %d shared.txt: %w", index, err)
				return
			}
			for fileIndex := range 10 {
				name := fmt.Sprintf("file-%d.txt", fileIndex)
				content := fmt.Sprintf("repo %d file %d\n", index, fileIndex)
				_, err := repo.CreateCommit(ctx, "main", "add file", "Test", "test@example.com",
					[]CommitOperation{{Type: CommitOperationAdd, Path: name, Content: []byte(content)}}, "")
				if err != nil {
					errorsCh <- fmt.Errorf("repo %d file %d: %w", index, fileIndex, err)
					return
				}
			}
		}()
	}
	writers.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	for index, repo := range repos {
		if got := readBlob(t, repo, "main", "shared.txt"); got != string(shared) {
			t.Errorf("repo %d shared.txt = %q, want %q", index, got, shared)
		}
		for fileIndex := range 10 {
			name := fmt.Sprintf("file-%d.txt", fileIndex)
			want := fmt.Sprintf("repo %d file %d\n", index, fileIndex)
			if got := readBlob(t, repo, "main", name); got != want {
				t.Errorf("repo %d %s = %q, want %q", index, name, got, want)
			}
		}
	}
	hash := sharedBlobHash(shared)
	entries, err := dataFS.ReadDir(path.Join("/git/sha1/objects", hash[:2]))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if entry.Name() == hash[2:] && !entry.IsDir() {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("shared blob count = %d, want 1", count)
	}
}

func TestSharedObjectsAlternatesLine(t *testing.T) {
	checkSharedAlternates(t, osfs.New(t.TempDir()))
}

func TestSharedObjectsMoveRewritesAlternates(t *testing.T) {
	dataDir := t.TempDir()
	bound := BindSharedObjects(osfs.New(dataDir), "/repositories", "/git/sha1")
	repo, err := Init(t.Context(), bound, "/org/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("moved\n")})
	if err := repo.Move("/deeper/org/repo.git"); err != nil {
		t.Fatal(err)
	}
	data, err := util.ReadFile(bound, "/deeper/org/repo.git/objects/info/alternates")
	if err != nil || string(data) != "../../../../../git/sha1/objects\n" {
		t.Fatalf("alternates after move = %q, %v", data, err)
	}
	gitFsck(t, filepath.Join(dataDir, "repositories/deeper/org/repo.git"))
}

func TestSharedObjectsGitBinary(t *testing.T) {
	dataDir := t.TempDir()
	bound := BindSharedObjects(osfs.New(dataDir), "/repositories", "/git/sha1")
	repoPath := "/org/repo.git"
	hostPath := filepath.Join(dataDir, "repositories/org/repo.git")
	glob := func(patterns ...string) []string {
		var files []string
		for _, pattern := range patterns {
			matches, err := filepath.Glob(pattern)
			if err != nil {
				t.Fatal(err)
			}
			files = append(files, matches...)
		}
		return files
	}
	sharedFiles := func() []string {
		return glob(filepath.Join(dataDir, "git/sha1/objects/[0-9a-f][0-9a-f]/*"))
	}
	localFiles := func() []string {
		return glob(filepath.Join(hostPath, "objects/[0-9a-f][0-9a-f]/*"), filepath.Join(hostPath, "objects/pack/*.pack"))
	}

	repo, err := Init(t.Context(), bound, repoPath, "main")
	if err != nil {
		t.Fatal(err)
	}
	gitFsck(t, hostPath)
	first := mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("alpha\n")})
	second := mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "b.txt", Content: []byte("beta\n")})
	if local := localFiles(); len(local) != 0 {
		t.Fatalf("hfd wrote local objects %v", local)
	}
	shared := sharedFiles()
	if len(shared) == 0 {
		t.Fatal("shared store has no loose objects")
	}
	gitFsck(t, hostPath)
	blob := sharedBlobHash([]byte("alpha\n"))
	tree := strings.TrimSpace(gitOut(t, hostPath, "rev-parse", second+"^{tree}"))
	objects := gitOut(t, hostPath, "rev-list", "--objects", "--all")
	for _, want := range []string{first, second, tree, blob} {
		if !strings.Contains(objects, want) {
			t.Errorf("rev-list --objects --all lacks %s:\n%s", want, objects)
		}
	}
	if got := gitOut(t, hostPath, "cat-file", "-t", second); got != "commit\n" {
		t.Errorf("cat-file -t %s = %q", second, got)
	}
	if got := gitOut(t, hostPath, "cat-file", "-p", blob); got != "alpha\n" {
		t.Errorf("cat-file -p %s = %q", blob, got)
	}

	clone := filepath.Join(t.TempDir(), "clone")
	runGit(t, "", "clone", "--quiet", "--no-local", hostPath, clone)
	gitFsck(t, clone)
	if got := gitOut(t, clone, "log", "--format=%H"); got != second+"\n"+first+"\n" {
		t.Fatalf("clone log = %q", got)
	}
	if err := os.WriteFile(filepath.Join(clone, "c.txt"), []byte("gamma\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, clone, "add", "c.txt")
	runGit(t, clone, "-c", "user.name=Test", "-c", "user.email=test@test.com", "commit", "--quiet", "-m", "pushed")
	runGit(t, clone, "push", "--quiet", "origin", "main")

	lruCache.Remove(cacheKey{bound, repoPath})
	repo, err = Open(bound, repoPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := readBlob(t, repo, "main", "c.txt"); got != "gamma\n" {
		t.Fatalf("pushed c.txt = %q", got)
	}
	if len(localFiles()) == 0 {
		t.Fatal("pushed objects are not in the repository's own object store")
	}
	if got := sharedFiles(); !slices.Equal(got, shared) {
		t.Fatalf("push changed the shared store:\n got %v\nwant %v", got, shared)
	}

	runGit(t, hostPath, "gc", "--quiet", "--prune=now")
	gitFsck(t, hostPath)
	if got := sharedFiles(); !slices.Equal(got, shared) {
		t.Fatalf("gc changed the shared store:\n got %v\nwant %v", got, shared)
	}
	lruCache.Remove(cacheKey{bound, repoPath})
	repo, err = Open(bound, repoPath)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"a.txt": "alpha\n", "c.txt": "gamma\n"} {
		if got := readBlob(t, repo, "main", name); got != want {
			t.Errorf("%s after gc = %q, want %q", name, got, want)
		}
	}
}

func checkSharedAlternates(t *testing.T, dataFS billy.Filesystem) {
	t.Helper()
	bound := BindSharedObjects(dataFS, "/repositories", "/git/sha1")
	for _, test := range []struct {
		repoPath string
		line     string
	}{
		{"/org/repo.git", "../../../../git/sha1/objects\n"},
		{"/datasets/org/ds.git", "../../../../../git/sha1/objects\n"},
	} {
		if _, err := Init(t.Context(), bound, test.repoPath, "main"); err != nil {
			t.Fatal(err)
		}
		alternates := path.Join(test.repoPath, "objects/info/alternates")
		data, err := util.ReadFile(bound, alternates)
		if err != nil || string(data) != test.line {
			t.Fatalf("alternates = %q, %v; want %q", data, err, test.line)
		}
		if _, err := Open(bound, test.repoPath); err != nil {
			t.Fatal(err)
		}
		data, err = util.ReadFile(bound, alternates)
		if err != nil || string(data) != test.line {
			t.Fatalf("reopened alternates = %q, %v; want %q", data, err, test.line)
		}
	}
	info, err := dataFS.Stat("/git/sha1/objects/pack")
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatal("shared objects/pack is not a directory")
	}
}

func TestSharedObjectsDedup(t *testing.T) {
	checkSharedDedup(t, osfs.New(t.TempDir()))
}

func checkSharedDedup(t *testing.T, dataFS billy.Filesystem) {
	t.Helper()
	bound := BindSharedObjects(dataFS, "/repositories", "/git/sha1")
	content := []byte("identical content\n")
	hash := sharedBlobHash(content)
	for _, repoPath := range []string{"/org/first.git", "/org/second.git"} {
		repo, err := Init(t.Context(), bound, repoPath, "main")
		if err != nil {
			t.Fatal(err)
		}
		commit := mustCommit(t, repo, "main", "",
			CommitOperation{Type: CommitOperationAdd, Path: "file.txt", Content: content})
		// The commit lives only in the shared store, so short hashes need HashesWithPrefix forwarding.
		if got, err := repo.ResolveRevision(commit[:7]); err != nil || got != commit {
			t.Fatalf("resolve %s = %q, %v; want %s", commit[:7], got, err, commit)
		}
		if got := readBlob(t, repo, "main", "file.txt"); got != string(content) {
			t.Fatalf("file.txt = %q", got)
		}
		if _, err := bound.Stat(path.Join(repoPath, "objects", hash[:2], hash[2:])); !os.IsNotExist(err) {
			t.Fatalf("local blob: want not exist, got %v", err)
		}
	}
	entries, err := dataFS.ReadDir(path.Join("/git/sha1/objects", hash[:2]))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if entry.Name() == hash[2:] && !entry.IsDir() {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("shared blob count = %d, want 1", count)
	}
}

func TestSharedObjectsMirrorFetchUnpacks(t *testing.T) {
	dataDir := t.TempDir()
	dataFS := osfs.New(dataDir)
	bound := BindSharedObjects(dataFS, "/repositories", "/git/sha1")
	sourcePath := filepath.Join(t.TempDir(), "source.git")
	source, err := Init(t.Context(), osfs.Default, sourcePath, "main")
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, source, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("first\n")},
		CommitOperation{Type: CommitOperationAdd, Path: "b.txt", Content: []byte("second\n")})
	mustCommit(t, source, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "a.txt", Content: []byte("updated\n")},
		CommitOperation{Type: CommitOperationAdd, Path: "c.txt", Content: []byte("third\n")})
	sourceURL := (&url.URL{Scheme: "file", Path: sourcePath}).String()
	repo, err := InitMirror(t.Context(), bound, "/org/mirror.git", sourceURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.PullMirrorRefs(t.Context(), sourceURL, []string{"refs/heads/main"}, nil); err != nil {
		t.Fatal(err)
	}
	repoHostPath := filepath.Join(dataDir, "repositories/org/mirror.git")
	for _, extension := range []string{"*.pack", "*.idx"} {
		files, err := filepath.Glob(filepath.Join(repoHostPath, "objects/pack", extension))
		if err != nil || len(files) != 0 {
			t.Fatalf("local %s files = %v, %v", extension, files, err)
		}
	}
	hash := sharedBlobHash([]byte("updated\n"))
	if _, err := dataFS.Stat(path.Join("/git/sha1/objects", hash[:2], hash[2:])); err != nil {
		t.Fatal(err)
	}
	gitFsck(t, repoHostPath)
	if got := readBlob(t, repo, "main", "a.txt"); got != "updated\n" {
		t.Fatalf("a.txt = %q", got)
	}
}

func TestSharedObjectsOnMemfs(t *testing.T) {
	checkSharedAlternates(t, memfs.New())
	checkSharedDedup(t, memfs.New())
}

func sharedBlobHash(content []byte) string {
	hasher := plumbing.NewHasher(formatcfg.DefaultObjectFormat, plumbing.BlobObject, int64(len(content)))
	_, _ = hasher.Write(content)
	return hasher.Sum().String()
}

func TestSharedStorerInterfaces(t *testing.T) {
	if _, ok := any(&sharedStorer{}).(storer.DeltaObjectStorer); ok {
		t.Fatal("shared storer exposes local-only DeltaObject")
	}
	if _, ok := any(&sharedStorer{}).(storer.PackfileWriter); !ok {
		t.Fatal("shared storer does not implement PackfileWriter")
	}
	if capable, ok := any(&sharedStorer{}).(packfile.LowMemoryCapable); !ok || !capable.LowMemoryMode() {
		t.Fatal("shared storer does not enable low-memory parsing")
	}
}

func TestSharedObjectsExtensions(t *testing.T) {
	dataDir := t.TempDir()
	bound := BindSharedObjects(osfs.New(dataDir), "/repositories", "/git/sha1")
	worktreeConfig := filepath.Join(dataDir, "repositories/org/wt.git")
	runGit(t, "", "init", "--bare", worktreeConfig)
	runGit(t, worktreeConfig, "config", "core.repositoryformatversion", "1")
	runGit(t, worktreeConfig, "config", "extensions.worktreeConfig", "true")
	if _, err := Open(bound, "/org/wt.git"); err != nil {
		t.Fatalf("open repo with worktreeConfig extension: %v", err)
	}
	runGit(t, "", "init", "--bare", "--object-format=sha256", filepath.Join(dataDir, "repositories/org/sha256.git"))
	if _, err := Open(bound, "/org/sha256.git"); !errors.Is(err, git.ErrUnknownExtension) {
		t.Fatalf("open sha256 repo: got %v, want %v", err, git.ErrUnknownExtension)
	}
}

func TestSharedObjectsScanLFSPointers(t *testing.T) {
	dataFS := osfs.New(t.TempDir())
	bound := BindSharedObjects(dataFS, "/repositories", "/git/sha1")
	repoPath := "/repo.git"
	repo, err := Init(t.Context(), bound, repoPath, "main")
	if err != nil {
		t.Fatal(err)
	}
	oid := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	content := []byte("version https://git-lfs.github.com/spec/v1\noid sha256:" + oid + "\nsize 123\n")
	mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "model.bin", Content: content})
	hash := sharedBlobHash(content)
	if _, err := dataFS.Stat(path.Join("/git/sha1/objects", hash[:2], hash[2:])); err != nil {
		t.Fatal(err)
	}
	if _, err := bound.Stat(path.Join(repoPath, "objects", hash[:2], hash[2:])); !os.IsNotExist(err) {
		t.Fatalf("local blob: want not exist, got %v", err)
	}
	pointers, err := repo.ScanLFSPointers()
	if err != nil {
		t.Fatal(err)
	}
	if len(pointers) != 1 {
		t.Fatalf("pointer count = %d, want 1", len(pointers))
	}
	if got := pointers[0].OID(); got != oid {
		t.Fatalf("pointer oid = %q, want %q", got, oid)
	}
}
