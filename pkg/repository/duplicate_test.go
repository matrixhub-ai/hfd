package repository

// Tests for Repository.Duplicate, which copies the files a bare repository needs verbatim, on osfs and memfs.

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// dupInit creates an empty repository at path.
func dupInit(t *testing.T, fs billy.Filesystem, path, branch string) *Repository {
	t.Helper()
	r, err := Init(t.Context(), fs, path, branch)
	if err != nil {
		t.Fatalf("Init(%s): %v", path, err)
	}
	return r
}

// dupCommit sets file.txt to content on branch and returns the new commit.
func dupCommit(t *testing.T, r *Repository, branch, content string) string {
	t.Helper()
	commit, err := r.CreateCommit(t.Context(), branch, "update file.txt", "Test", "test@example.com",
		[]CommitOperation{{Type: CommitOperationAdd, Path: "file.txt", Content: []byte(content)}}, "")
	if err != nil {
		t.Fatalf("CreateCommit(%s): %v", branch, err)
	}
	return commit
}

// dupRefs returns every ref of r.
func dupRefs(t *testing.T, r *Repository) map[string]string {
	t.Helper()
	refs, err := r.Refs()
	if err != nil {
		t.Fatalf("Refs(%s): %v", r.repoPath, err)
	}
	return refs
}

// dupObjectCount counts every object r stores, reachable or not.
func dupObjectCount(t *testing.T, r *Repository) int {
	t.Helper()
	iter, err := r.repo.Storer.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		t.Fatalf("IterEncodedObjects(%s): %v", r.repoPath, err)
	}
	n := 0
	err = iter.ForEach(func(plumbing.EncodedObject) error {
		n++
		return nil
	})
	if err != nil {
		t.Fatalf("count objects of %s: %v", r.repoPath, err)
	}
	return n
}

// dupFile reads file.txt at rev through the hfd API.
func dupFile(r *Repository, rev string) (string, error) {
	b, err := r.Blob(rev, "file.txt")
	if err != nil {
		return "", err
	}
	rc, err := b.NewReader()
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	return string(data), err
}

// dupHEAD returns the HEAD file of the repository at path without its trailing newline.
func dupHEAD(t *testing.T, fs billy.Filesystem, path string) string {
	t.Helper()
	data, err := util.ReadFile(fs, filepath.Join(path, "HEAD"))
	if err != nil {
		t.Fatalf("read %s/HEAD: %v", path, err)
	}
	return strings.TrimSuffix(string(data), "\n")
}

// dupTree maps every path under root to "dir", or to a file's permissions and content digest.
func dupTree(t *testing.T, fs billy.Filesystem, root string) map[string]string {
	t.Helper()
	tree := make(map[string]string)
	err := util.Walk(fs, root, func(name string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		if info.IsDir() {
			tree[rel] = "dir"
			return nil
		}
		data, err := util.ReadFile(fs, name)
		if err != nil {
			return err
		}
		tree[rel] = fmt.Sprintf("%v %x", info.Mode().Perm(), sha256.Sum256(data))
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return tree
}

// dupNeededTree is dupTree limited to what a bare repository needs: HEAD, config, packed-refs, shallow, refs and objects.
func dupNeededTree(t *testing.T, fs billy.Filesystem, root string) map[string]string {
	t.Helper()
	tree := dupTree(t, fs, root)
	maps.DeleteFunc(tree, func(p, _ string) bool {
		top, _, _ := strings.Cut(filepath.ToSlash(p), "/")
		return !slices.Contains([]string{"HEAD", "config", "packed-refs", "shallow", "refs", "objects"}, top)
	})
	return tree
}

// dupPacks lists the pack and index files of the repository at path.
func dupPacks(t *testing.T, fs billy.Filesystem, path string) []string {
	t.Helper()
	entries, err := fs.ReadDir(filepath.Join(path, "objects", "pack"))
	if err != nil {
		t.Fatalf("read %s/objects/pack: %v", path, err)
	}
	var names []string
	for _, e := range entries {
		if ext := filepath.Ext(e.Name()); ext == ".pack" || ext == ".idx" {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)
	return names
}

func TestDuplicate(t *testing.T) {
	filesystems := []struct {
		name  string
		newFS func(*testing.T) billy.Filesystem
	}{
		{"osfs", func(t *testing.T) billy.Filesystem { return osfs.New(t.TempDir()) }},
		{"memfs", func(*testing.T) billy.Filesystem { return memfs.New() }},
	}

	cases := []struct {
		name string
		run  func(t *testing.T, fs billy.Filesystem)
	}{
		{"CopiesTheRepository", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "main")
			dupCommit(t, src, "main", "one\n")
			if err := src.CreateTag("v1", "main"); err != nil {
				t.Fatalf("CreateTag(v1): %v", err)
			}
			tip := dupCommit(t, src, "main", "two\n")
			if err := src.CreateBranch("dev", "main"); err != nil {
				t.Fatalf("CreateBranch(dev): %v", err)
			}
			dupCommit(t, src, "dev", "dev\n")
			v2, err := src.repo.CreateTag("v2", plumbing.NewHash(tip), &git.CreateTagOptions{
				Tagger:  &object.Signature{Name: "Test", Email: "test@example.com", When: time.Now()},
				Message: "v2",
			})
			if err != nil {
				t.Fatalf("CreateTag(v2): %v", err)
			}
			// Like a Hub pull request, refs/pr/1 alone reaches its commit.
			if err := src.CreateBranch("pr-head", "main"); err != nil {
				t.Fatalf("CreateBranch(pr-head): %v", err)
			}
			pr := dupCommit(t, src, "pr-head", "pr\n")
			if err := src.repo.Storer.SetReference(plumbing.NewHashReference("refs/pr/1", plumbing.NewHash(pr))); err != nil {
				t.Fatalf("set refs/pr/1: %v", err)
			}
			if err := src.DeleteBranch("pr-head"); err != nil {
				t.Fatalf("DeleteBranch(pr-head): %v", err)
			}
			obj := src.repo.Storer.NewEncodedObject()
			obj.SetType(plumbing.BlobObject)
			w, err := obj.Writer()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write([]byte("dangling\n")); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			dangling, err := src.repo.Storer.SetEncodedObject(obj)
			if err != nil {
				t.Fatalf("store unreachable blob: %v", err)
			}
			// Only reachable objects move into the pack; the unreachable blob stays loose.
			if err := src.repo.RepackObjects(&git.RepackConfig{}); err != nil {
				t.Fatalf("RepackObjects: %v", err)
			}
			if err := src.repo.Storer.PackRefs(); err != nil {
				t.Fatalf("PackRefs: %v", err)
			}
			cfg, err := src.repo.Config()
			if err != nil {
				t.Fatalf("source config: %v", err)
			}
			cfg.Raw.SetOption("hfd", "", "marker", "yes")
			if err := src.repo.SetConfig(cfg); err != nil {
				t.Fatalf("set hfd.marker: %v", err)
			}
			if err := util.WriteFile(fs, "/src.git/hooks/post-update", []byte("#!/bin/sh\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			for _, p := range []string{"description", "info/exclude", "logs/HEAD", "gc.log", "FETCH_HEAD"} {
				if err := util.WriteFile(fs, filepath.Join("/src.git", p), []byte("extra\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			before := dupRefs(t, src)
			if len(before) != 5 {
				t.Fatalf("source refs = %v, want main, dev, v1, v2 and refs/pr/1", before)
			}
			objects := dupObjectCount(t, src)

			dst, err := src.Duplicate(t.Context(), "/other/copy.git")
			if err != nil {
				t.Fatalf("Duplicate: %v", err)
			}
			srcTree, dstTree := dupNeededTree(t, fs, "/src.git"), dupNeededTree(t, fs, "/other/copy.git")
			for _, p := range slices.Sorted(maps.Keys(srcTree)) {
				if dstTree[p] != srcTree[p] {
					t.Errorf("copy %s = %q, want the source's %q", p, dstTree[p], srcTree[p])
				}
			}
			for _, p := range slices.Sorted(maps.Keys(dstTree)) {
				if _, ok := srcTree[p]; !ok {
					t.Errorf("copy holds %s, which the source lacks", p)
				}
			}
			for _, p := range []string{"hooks", "hooks/post-update", "description", "info", "info/exclude", "logs", "logs/HEAD", "gc.log", "FETCH_HEAD"} {
				if _, err := fs.Stat(filepath.Join("/other/copy.git", p)); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("Stat(copy %s) = %v, want ErrNotExist", p, err)
				}
			}
			if got := dupRefs(t, dst); !maps.Equal(got, before) {
				t.Errorf("copy refs = %v, want the source's %v", got, before)
			}
			if got, want := dst.DefaultBranch(), src.DefaultBranch(); got != want {
				t.Errorf("copy DefaultBranch = %q, want %q", got, want)
			}
			if !IsRepository(fs, "/other/copy.git") {
				t.Error("IsRepository(/other/copy.git) = false after Duplicate")
			}
			for _, f := range []struct{ rev, want string }{
				{"main", "two\n"}, {"dev", "dev\n"}, {"v1", "one\n"}, {"v2", "two\n"}, {"refs/pr/1", "pr\n"},
			} {
				if got, err := dupFile(dst, f.rev); err != nil || got != f.want {
					t.Errorf("copy %s:file.txt = %q, %v; want %q", f.rev, got, err, f.want)
				}
			}
			if _, err := dst.repo.TagObject(v2.Hash()); err != nil {
				t.Errorf("copy lacks annotated tag object v2 %s: %v", v2.Hash(), err)
			}
			if err := dst.repo.Storer.HasEncodedObject(dangling); err != nil {
				t.Errorf("copy HasEncodedObject(unreachable blob %s) = %v, want nil", dangling, err)
			}
			if got := dupObjectCount(t, dst); got != objects {
				t.Errorf("copy holds %d objects, want the source's %d", got, objects)
			}
			if cfg, err := dst.repo.Config(); err != nil {
				t.Errorf("copy config: %v", err)
			} else if marker := cfg.Raw.Section("hfd").Option("marker"); marker != "yes" || !cfg.Core.IsBare {
				t.Errorf("copy config hfd.marker = %q, core.bare = %t; want yes, true", marker, cfg.Core.IsBare)
			}
			if remotes, err := dst.repo.Remotes(); err != nil || len(remotes) != 0 {
				t.Errorf("copy remotes = %v, %v; want none", remotes, err)
			}
			if got := dupRefs(t, src); !maps.Equal(got, before) {
				t.Errorf("source refs = %v after Duplicate, want %v", got, before)
			}
			if got := dupObjectCount(t, src); got != objects {
				t.Errorf("source holds %d objects after Duplicate, want %d", got, objects)
			}
			if dir := localDir(fs, "/other/copy.git"); dir != "" {
				gitFsck(t, dir)
			}
		}},
		{"KeepsNonMainDefaultBranch", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "develop")
			dupCommit(t, src, "develop", "one\n")
			dst, err := src.Duplicate(t.Context(), "/other/copy.git")
			if err != nil {
				t.Fatalf("Duplicate: %v", err)
			}
			if got := dst.DefaultBranch(); got != "develop" {
				t.Errorf("copy DefaultBranch = %q, want develop", got)
			}
			if got := dupHEAD(t, fs, "/other/copy.git"); got != "ref: refs/heads/develop" {
				t.Errorf("copy HEAD = %q, want ref: refs/heads/develop", got)
			}
		}},
		{"EmptySource", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "trunk")
			dst, err := src.Duplicate(t.Context(), "/other/copy.git")
			if err != nil {
				t.Fatalf("Duplicate of an empty repository: %v", err)
			}
			if refs := dupRefs(t, dst); len(refs) != 0 {
				t.Errorf("copy refs = %v, want none", refs)
			}
			if got, want := dst.DefaultBranch(), src.DefaultBranch(); got != want {
				t.Errorf("copy DefaultBranch = %q, want %q", got, want)
			}
			if got := dupHEAD(t, fs, "/other/copy.git"); got != "ref: refs/heads/trunk" {
				t.Errorf("copy HEAD = %q, want ref: refs/heads/trunk", got)
			}
		}},
		{"RefusesExistingRepository", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "main")
			dupCommit(t, src, "main", "source\n")
			target := dupInit(t, fs, "/other/copy.git", "main")
			dupCommit(t, target, "main", "target\n")
			want := dupRefs(t, target)
			if _, err := src.Duplicate(t.Context(), "/other/copy.git"); !errors.Is(err, ErrRepositoryAlreadyExists) {
				t.Errorf("Duplicate onto an existing repository = %v, want ErrRepositoryAlreadyExists", err)
			}
			if got := dupRefs(t, target); !maps.Equal(got, want) {
				t.Errorf("target refs = %v after the refused Duplicate, want %v", got, want)
			}
		}},
		{"RefusesExistingPath", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "main")
			dupCommit(t, src, "main", "source\n")
			if err := util.WriteFile(fs, "/other/copy.git/junk", []byte("junk\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			want := dupTree(t, fs, "/other/copy.git")
			if _, err := src.Duplicate(t.Context(), "/other/copy.git"); !errors.Is(err, ErrRepositoryAlreadyExists) {
				t.Errorf("Duplicate onto an existing directory = %v, want ErrRepositoryAlreadyExists", err)
			}
			if got := dupTree(t, fs, "/other/copy.git"); !maps.Equal(got, want) {
				t.Errorf("/other/copy.git = %v after the refused Duplicate, want only its junk %v", got, want)
			}
		}},
		{"FailureLeavesNoTarget", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "main")
			dupCommit(t, src, "main", "one\n")
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := src.Duplicate(ctx, "/other/gone.git"); err == nil {
				t.Fatal("Duplicate with a canceled context succeeded, want an error")
			}
			if _, err := fs.Stat("/other/gone.git"); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("Stat(/other/gone.git) after the failed Duplicate = %v, want ErrNotExist", err)
			}
			if IsRepository(fs, "/other/gone.git") {
				t.Error("IsRepository(/other/gone.git) = true after the failed Duplicate")
			}
		}},
		{"CreatesParentDirectories", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "main")
			dupCommit(t, src, "main", "one\n")
			if _, err := fs.Stat("/brand-new-ns"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Stat(/brand-new-ns) before Duplicate = %v, want ErrNotExist", err)
			}
			dst, err := src.Duplicate(t.Context(), "/brand-new-ns/copy.git")
			if err != nil {
				t.Fatalf("Duplicate into a new namespace: %v", err)
			}
			if !IsRepository(fs, "/brand-new-ns/copy.git") {
				t.Error("IsRepository(/brand-new-ns/copy.git) = false after Duplicate")
			}
			if got, want := dupRefs(t, dst), dupRefs(t, src); !maps.Equal(got, want) {
				t.Errorf("copy refs = %v, want the source's %v", got, want)
			}
		}},
		{"CopyIsIndependent", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "main")
			dupCommit(t, src, "main", "source\n")
			srcRefs, srcObjects := dupRefs(t, src), dupObjectCount(t, src)
			dst, err := src.Duplicate(t.Context(), "/other/copy.git")
			if err != nil {
				t.Fatalf("Duplicate: %v", err)
			}
			dupCommit(t, dst, "main", "copy\n")
			if got := dupRefs(t, src); !maps.Equal(got, srcRefs) {
				t.Errorf("source refs = %v after committing to the copy, want %v", got, srcRefs)
			}
			if got := dupObjectCount(t, src); got != srcObjects {
				t.Errorf("source holds %d objects after committing to the copy, want %d", got, srcObjects)
			}
			dstRefs, dstObjects := dupRefs(t, dst), dupObjectCount(t, dst)
			dupCommit(t, src, "main", "source again\n")
			if got := dupRefs(t, dst); !maps.Equal(got, dstRefs) {
				t.Errorf("copy refs = %v after committing to the source, want %v", got, dstRefs)
			}
			if got := dupObjectCount(t, dst); got != dstObjects {
				t.Errorf("copy holds %d objects after committing to the source, want %d", got, dstObjects)
			}
		}},
		{"SkipsTransientFiles", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "main")
			tip := dupCommit(t, src, "main", "one\n")
			// Only under objects does tmp_ mark a temporary file; a branch may carry the prefix too.
			if err := util.WriteFile(fs, "/src.git/refs/heads/tmp_x", []byte(tip+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			// Before the seeds, as go-git lists refs/heads/main.lock as a ref.
			refs := dupRefs(t, src)
			// Leftovers of interrupted writers: locks, temporary objects and pack, a pack and its sidecar awaiting their index, an orphaned index, a push's quarantined pack, a repack's pack and index awaiting their rename.
			seeds := []string{
				"config.lock",
				"refs/heads/main.lock",
				"objects/tmp_obj_abc",
				"objects/ab/tmp_obj_def",
				"objects/pack/tmp_pack_abc",
				"objects/pack/pack-" + strings.Repeat("0", 40) + ".pack",
				"objects/pack/pack-" + strings.Repeat("0", 40) + ".rev",
				"objects/pack/pack-" + strings.Repeat("f", 40) + ".idx",
				"objects/incoming-abc/pack/pack-" + strings.Repeat("1", 40) + ".pack",
				"objects/pack/.tmp-123-pack-" + strings.Repeat("2", 40) + ".pack",
				"objects/pack/.tmp-123-pack-" + strings.Repeat("2", 40) + ".idx",
			}
			for _, s := range seeds {
				if err := util.WriteFile(fs, filepath.Join("/src.git", s), []byte(tip+"\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			dst, err := src.Duplicate(t.Context(), "/other/copy.git")
			if err != nil {
				t.Fatalf("Duplicate: %v", err)
			}
			for _, s := range append(seeds, "objects/incoming-abc") {
				if _, err := fs.Stat(filepath.Join("/other/copy.git", s)); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("Stat(copy %s) = %v, want ErrNotExist", s, err)
				}
			}
			if _, err := fs.Stat("/other/copy.git/refs/heads/tmp_x"); err != nil {
				t.Errorf("Stat(copy refs/heads/tmp_x) = %v, want the branch copied", err)
			}
			dstRefs := dupRefs(t, dst)
			if got := dstRefs["refs/heads/tmp_x"]; got != tip {
				t.Errorf("copy refs/heads/tmp_x = %q, want %s", got, tip)
			}
			if !maps.Equal(dstRefs, refs) {
				t.Errorf("copy refs = %v, want the source's %v", dstRefs, refs)
			}
			if got, err := dupFile(dst, "main"); err != nil || got != "one\n" {
				t.Errorf("copy main:file.txt = %q, %v; want %q", got, err, "one\n")
			}
			for _, s := range seeds {
				if err := fs.Remove(filepath.Join("/src.git", s)); err != nil {
					t.Fatal(err)
				}
			}
			if err := util.RemoveAll(fs, "/src.git/objects/incoming-abc"); err != nil {
				t.Fatal(err)
			}
			if got, err := dupFile(src, "main"); err != nil || got != "one\n" {
				t.Errorf("source main:file.txt once the seeds are gone = %q, %v; want %q", got, err, "one\n")
			}
		}},
		{"CopiesPackedObjects", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "main")
			dupCommit(t, src, "main", "one\n")
			if err := src.CreateBranch("gone", "main"); err != nil {
				t.Fatalf("CreateBranch(gone): %v", err)
			}
			dupCommit(t, src, "gone", "gone\n")
			if err := src.repo.RepackObjects(&git.RepackConfig{}); err != nil {
				t.Fatalf("RepackObjects: %v", err)
			}
			// The pack keeps the deleted branch's objects, so packing what the refs reach anew cannot reproduce its name.
			if err := src.DeleteBranch("gone"); err != nil {
				t.Fatalf("DeleteBranch(gone): %v", err)
			}
			packs := dupPacks(t, fs, "/src.git")
			if len(packs) == 0 {
				t.Fatal("source holds no pack after RepackObjects")
			}
			dst, err := src.Duplicate(t.Context(), "/other/copy.git")
			if err != nil {
				t.Fatalf("Duplicate: %v", err)
			}
			if got := dupPacks(t, fs, "/other/copy.git"); !slices.Equal(got, packs) {
				t.Errorf("copy packs = %v, want the source's %v", got, packs)
			}
			if got, err := dupFile(dst, "main"); err != nil || got != "one\n" {
				t.Errorf("copy main:file.txt = %q, %v; want %q", got, err, "one\n")
			}
		}},
		{"CopiesShallowMarker", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "main")
			root := dupCommit(t, src, "main", "one\n")
			dupCommit(t, src, "main", "two\n")
			shallow := root + "\n"
			if err := util.WriteFile(fs, "/src.git/shallow", []byte(shallow), 0o644); err != nil {
				t.Fatal(err)
			}
			refs := dupRefs(t, src)
			dst, err := src.Duplicate(t.Context(), "/other/copy.git")
			if err != nil {
				t.Fatalf("Duplicate: %v", err)
			}
			if got, err := util.ReadFile(fs, "/other/copy.git/shallow"); err != nil || string(got) != shallow {
				t.Errorf("copy shallow = %q, %v; want the source's %q", got, err, shallow)
			}
			if got := dupRefs(t, dst); !maps.Equal(got, refs) {
				t.Errorf("copy refs = %v, want the source's %v", got, refs)
			}
		}},
		{"RefusesTornRef", func(t *testing.T, fs billy.Filesystem) {
			src := dupInit(t, fs, "/src.git", "main")
			dupCommit(t, src, "main", "one\n")
			// What a copy reads mid-rewrite: go-git truncates a ref before writing it and cannot lock it through hfd's chroot.
			if err := util.WriteFile(fs, "/src.git/refs/heads/torn", nil, 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := src.Duplicate(t.Context(), "/other/copy.git")
			if err == nil {
				t.Fatal("Duplicate with an empty source refs/heads/torn succeeded, want an error naming it")
			}
			if !strings.Contains(err.Error(), "refs/heads/torn") {
				t.Errorf("Duplicate with an empty source refs/heads/torn = %v, want an error naming refs/heads/torn", err)
			}
			if _, err := fs.Stat("/other/copy.git"); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("Stat(/other/copy.git) after the refused Duplicate = %v, want ErrNotExist", err)
			}
			if IsRepository(fs, "/other/copy.git") {
				t.Error("IsRepository(/other/copy.git) = true after the refused Duplicate")
			}
			if got, err := dupFile(src, "main"); err != nil || got != "one\n" {
				t.Errorf("source main:file.txt after the refused Duplicate = %q, %v; want %q", got, err, "one\n")
			}
			if err := fs.Remove("/src.git/refs/heads/torn"); err != nil {
				t.Fatal(err)
			}
			if _, err := src.Duplicate(t.Context(), "/other/copy.git"); err != nil {
				t.Errorf("Duplicate once the empty refs/heads/torn is gone: %v", err)
			}
		}},
	}

	for _, fsys := range filesystems {
		for _, c := range cases {
			t.Run(fsys.name+"/"+c.name, func(t *testing.T) { c.run(t, fsys.newFS(t)) })
		}
	}
}
