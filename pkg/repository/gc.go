package repository

import (
	"context"
	"crypto"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/format/idxfile"
	"github.com/go-git/go-git/v6/plumbing/hash"
	"github.com/go-git/go-git/v6/plumbing/revlist"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/filesystem"
)

// GCResult describes one GC pass; DeletedBytes is object payload, not the disk space the repack freed.
type GCResult struct {
	DeletedObjects []plumbing.Hash // objects no longer stored, sorted
	DeletedBytes   int64           // uncompressed size of DeletedObjects
	ReclaimedBytes int64           // shrink of objects/ on disk, never negative; 0 in a dry run, which repacks nothing
}

// errGCPreviewUnsupported: the repository is in a state whose GC outcome cannot be predicted without running it.
var errGCPreviewUnsupported = errors.New("dry-run GC not supported for this repository")

// GC requires quiescent writes; a zero cutoff disables grace. A dry run predicts the engine's deletions from refs, loose objects and pack indexes and writes nowhere.
func (r *Repository) GC(ctx context.Context, cutoff time.Time, dryRun bool) (GCResult, error) {
	if err := ctx.Err(); err != nil {
		return GCResult{}, err
	}
	before, err := objectInventory(ctx, r.fs, r.repoPath)
	if err != nil {
		return GCResult{}, err
	}
	if dryRun {
		return r.previewGC(ctx, cutoff, before)
	}
	size, err := objectsSize(r.fs, r.repoPath)
	if err != nil {
		return GCResult{}, err
	}
	if dir := r.gitDir(); dir != "" {
		err = gcGit(ctx, dir, cutoff)
	} else {
		err = gcGoGit(ctx, r.repo, cutoff)
	}
	// Packs may have moved even when git failed, so the handle is refreshed either way.
	err = errors.Join(err, r.reindex())
	// Cached pack objects still point at the packs GC deleted.
	r.objectCache.Clear()
	if err = joinCtx(ctx, err); err != nil {
		return GCResult{}, err
	}
	return gcResult(ctx, r.fs, r.repoPath, before, size)
}

// previewGC diffs before against the objects r's engine would leave behind; what a repack would save on disk stays unknown.
func (r *Repository) previewGC(ctx context.Context, cutoff time.Time, before map[plumbing.Hash]int64) (GCResult, error) {
	// Native git may drop local copies of borrowed objects, so alternates have no exact preview.
	if _, err := r.fs.Stat(filepath.Join(r.repoPath, "objects", "info", "alternates")); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = fmt.Errorf("%w: objects/info/alternates", errGCPreviewUnsupported)
		}
		return GCResult{}, err
	}
	packs, err := packInfos(ctx, r.fs, r.repoPath)
	if err != nil {
		return GCResult{}, err
	}
	st := newStorer(r.fs, r.repoPath, cache.NewObjectLRUDefault())
	defer func() { _ = st.Close() }()
	var kept map[plumbing.Hash]bool
	if r.gitDir() != "" {
		kept, err = r.previewGit(ctx, st, cutoff, packs)
	} else {
		kept, err = previewGoGit(ctx, st, cutoff, packs)
	}
	if err = joinCtx(ctx, err); err != nil {
		return GCResult{}, err
	}
	return deletedFrom(before, kept), nil
}

// previewGoGit mirrors gcGoGit: Prune's candidates go; a repack then keeps what refs reach, the loose objects Prune spared and packs not older than cutoff.
func previewGoGit(ctx context.Context, st *filesystem.Storage, cutoff time.Time, packs []packInfo) (map[plumbing.Hash]bool, error) {
	repo, err := git.Open(st, nil)
	if err != nil {
		return nil, err
	}
	kept := map[plumbing.Hash]bool{}
	if err := st.ForEachObjectHash(func(h plumbing.Hash) error {
		kept[h] = true
		return ctx.Err()
	}); err != nil {
		return nil, err
	}
	err = repo.Prune(git.PruneOptions{OnlyObjectsOlderThan: cutoff, Handler: func(h plumbing.Hash) error {
		delete(kept, h)
		return ctx.Err()
	}})
	if err != nil {
		return nil, err
	}
	refs, err := repo.References()
	if err != nil {
		return nil, err
	}
	var wants []plumbing.Hash
	if err := refs.ForEach(func(ref *plumbing.Reference) error {
		if ref.Type() == plumbing.HashReference {
			wants = append(wants, ref.Hash())
		}
		return nil
	}); err != nil {
		return nil, err
	}
	repack := len(wants) > 0 && (len(kept) > 0 || len(packs) > 1)
	for _, p := range packs {
		if !repack || !cutoff.IsZero() && !p.mtime.Before(cutoff) {
			for _, h := range p.objects {
				kept[h] = true
			}
		}
	}
	if !repack {
		return kept, nil
	}
	reachable, err := revlist.Objects(st, wants, nil)
	if err != nil {
		return nil, err
	}
	for _, h := range reachable {
		kept[h] = true
	}
	return kept, nil
}

// previewGit mirrors native gc: refs, HEAD, reflogs and the index keep what they reach, unexpired garbage keeps what it reaches, and a .keep pack keeps everything in it.
func (r *Repository) previewGit(ctx context.Context, st *filesystem.Storage, cutoff time.Time, packs []packInfo) (map[plumbing.Hash]bool, error) {
	dir := r.gitDir()
	// Checked before any walk: even a read-only walk fetches into a promisor repository.
	if blocked, err := previewBlockedGit(ctx, dir); err != nil || blocked {
		if blocked {
			err = fmt.Errorf("%w: native Git configuration changes object retention", errGCPreviewUnsupported)
		}
		return nil, err
	}
	// gc expires reflog entries by age before it repacks, so reflogs make the roots unpredictable.
	if reflogs, err := hasFiles(r.fs, filepath.Join(r.repoPath, "logs")); err != nil || reflogs {
		if reflogs {
			err = fmt.Errorf("%w: reflogs present", errGCPreviewUnsupported)
		}
		return nil, err
	}
	kept := map[plumbing.Hash]bool{}
	var roots []plumbing.Hash
	for _, p := range packs {
		switch {
		case p.marker == "promisor" || p.marker != "" && !cutoff.IsZero():
			// Cruft .mtimes age each object on its own and a .keep pack roots gc's grace walk differently from prune's.
			return nil, fmt.Errorf("%w: pack-%s.%s", errGCPreviewUnsupported, p.hash, p.marker)
		case p.marker == "keep":
			for _, h := range p.objects {
				kept[h] = true
			}
		case !cutoff.IsZero() && p.mtime.Unix() > cutoff.Unix():
			roots = append(roots, p.objects...)
		}
	}
	if !cutoff.IsZero() {
		err := st.ForEachObjectHash(func(h plumbing.Hash) error {
			t, err := st.LooseObjectTime(h)
			if err != nil {
				return err
			}
			if t.Unix() > cutoff.Unix() {
				roots = append(roots, h)
			}
			return ctx.Err()
		})
		if err != nil {
			return nil, err
		}
	}
	reachable, err := revListGit(ctx, dir, roots)
	if err != nil {
		return nil, err
	}
	for _, h := range reachable {
		kept[h] = true
	}
	return kept, nil
}

// packInfo is one objects/pack/pack-<hash>.pack: its mtime, the git marker beside it (promisor, keep or mtimes, first found) and the objects its index names.
type packInfo struct {
	hash    plumbing.Hash
	mtime   time.Time
	marker  string
	objects []plumbing.Hash
}

// packInfos reads every pack index at path on fs, skipping badly named packs as go-git does.
func packInfos(ctx context.Context, fs billy.Filesystem, path string) ([]packInfo, error) {
	dir := filepath.Join(path, "objects", "pack")
	entries, err := fs.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	names := make(map[string]bool, len(entries))
	for _, e := range entries {
		names[e.Name()] = true
	}
	var packs []packInfo
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "pack-") || !strings.HasSuffix(name, ".pack") {
			continue
		}
		hex := name[5 : len(name)-5]
		h, ok := plumbing.FromHex(hex)
		if !ok || len(hex) != h.HexSize() {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		p := packInfo{hash: h, mtime: info.ModTime()}
		for _, ext := range []string{"promisor", "keep", "mtimes"} {
			if names["pack-"+hex+"."+ext] {
				p.marker = ext
				break
			}
		}
		if p.objects, err = packIndexObjects(fs, filepath.Join(dir, "pack-"+hex+".idx"), h); err != nil {
			return nil, err
		}
		packs = append(packs, p)
	}
	return packs, nil
}

// packIndexObjects lists the hashes named by the pack index at name.
func packIndexObjects(fs billy.Filesystem, name string, pack plumbing.Hash) ([]plumbing.Hash, error) {
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	hasher := hash.New(crypto.SHA1)
	if pack.Size() == crypto.SHA256.Size() {
		hasher = hash.New(crypto.SHA256)
	}
	idx := idxfile.NewMemoryIndex(pack.Size())
	if err := idxfile.NewDecoder(f, hasher).Decode(idx); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	iter, err := idx.Entries()
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()
	var objects []plumbing.Hash
	for {
		e, err := iter.Next()
		if errors.Is(err, io.EOF) {
			return objects, nil
		}
		if err != nil {
			return nil, err
		}
		// go-git's iterator pads entries sharing a fanout bucket with the next entry's bytes, which would break equality.
		h, _ := plumbing.FromBytes(e.Hash.Bytes())
		objects = append(objects, h)
	}
}

// hasFiles reports whether any regular file exists under dir on fs; a missing dir has none.
func hasFiles(fs billy.Filesystem, dir string) (bool, error) {
	found := false
	err := util.Walk(fs, dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			found = true
		}
		return err
	})
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return found, err
}

// gcResult diffs the objects still stored at path against before, measuring how much smaller objects/ got.
func gcResult(ctx context.Context, fs billy.Filesystem, path string, before map[plumbing.Hash]int64, size int64) (GCResult, error) {
	after, err := objectInventory(ctx, fs, path)
	if err != nil {
		return GCResult{}, err
	}
	now, err := objectsSize(fs, path)
	if err != nil {
		return GCResult{}, err
	}
	res := deletedFrom(before, after)
	res.ReclaimedBytes = max(size-now, 0)
	return res, nil
}

// deletedFrom lists the objects of before absent from kept, sized from before since they may no longer be readable.
func deletedFrom[V any](before map[plumbing.Hash]int64, kept map[plumbing.Hash]V) GCResult {
	var res GCResult
	for h, n := range before {
		if _, ok := kept[h]; !ok {
			res.DeletedObjects = append(res.DeletedObjects, h)
			res.DeletedBytes += n
		}
	}
	plumbing.HashesSort(res.DeletedObjects)
	return res
}

// objectInventory maps every loose or packed object at path to its payload size through a fresh storer, so cached objects cannot stand in for deleted ones.
func objectInventory(ctx context.Context, fs billy.Filesystem, path string) (map[plumbing.Hash]int64, error) {
	st := newStorer(fs, path, cache.NewObjectLRUDefault())
	defer func() { _ = st.Close() }()
	iter, err := st.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		return nil, err
	}
	objects := map[plumbing.Hash]int64{}
	err = iter.ForEach(func(o plumbing.EncodedObject) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		objects[o.Hash()] = o.Size()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return objects, nil
}

// joinCtx adds a cancellation the engine may have run past to err.
func joinCtx(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		return errors.Join(err, ctxErr)
	}
	return err
}

// objectsSize sums the files under objects/; LFS content lives elsewhere and never counts.
func objectsSize(fs billy.Filesystem, path string) (int64, error) {
	var total int64
	err := util.Walk(fs, filepath.Join(path, "objects"), func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// gcGoGit prunes unreachable loose objects, then repacks only when a ref exists and loose objects or several packs remain.
func gcGoGit(ctx context.Context, repo *git.Repository, cutoff time.Time) error {
	err := repo.Prune(git.PruneOptions{OnlyObjectsOlderThan: cutoff, Handler: func(h plumbing.Hash) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return repo.DeleteObject(h)
	}})
	if err != nil {
		return err
	}
	s, ok := repo.Storer.(interface {
		storer.LooseObjectStorer
		storer.PackedObjectStorer
	})
	if !ok {
		return git.ErrPackedObjectsNotSupported
	}
	refs, err := repo.References()
	if err != nil {
		return err
	}
	var hashRefs, loose bool
	if err := refs.ForEach(func(ref *plumbing.Reference) error {
		hashRefs = ref.Type() == plumbing.HashReference
		if hashRefs {
			return storer.ErrStop
		}
		return nil
	}); err != nil {
		return err
	}
	if err := s.ForEachObjectHash(func(plumbing.Hash) error {
		loose = true
		return storer.ErrStop
	}); err != nil {
		return err
	}
	packs, err := s.ObjectPacks()
	if err != nil {
		return err
	}
	if !hashRefs || (!loose && len(packs) <= 1) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return repo.RepackObjects(&git.RepackConfig{OnlyDeletePacksOlderThan: cutoff})
}
