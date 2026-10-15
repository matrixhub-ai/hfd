package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

var errMissingObject = errors.New("missing necessary objects")

// reachSet is an immutable snapshot of what the repository's refs reach.
type reachSet struct {
	commits map[plumbing.Hash]plumbing.Hash       // commit → its tree
	tags    map[plumbing.Hash]plumbing.Hash       // annotated tag → the object its chain ends at
	named   map[plumbing.Hash]plumbing.ObjectType // trees and blobs a ref or tag names
}

func newReachSet() *reachSet {
	return &reachSet{
		commits: map[plumbing.Hash]plumbing.Hash{},
		tags:    map[plumbing.Hash]plumbing.Hash{},
		named:   map[plumbing.Hash]plumbing.ObjectType{},
	}
}

func (s *reachSet) clone() *reachSet {
	return &reachSet{commits: maps.Clone(s.commits), tags: maps.Clone(s.tags), named: maps.Clone(s.named)}
}

// contains reports whether h is a commit or annotated tag object reachable from refs.
func (s *reachSet) contains(h plumbing.Hash) bool {
	_, commit := s.commits[h]
	_, tag := s.tags[h]
	return commit || tag
}

// byPrefix lists the commits and tag objects whose lowercase hex starts with hex, ascending.
func (s *reachSet) byPrefix(hex string) []plumbing.Hash {
	var found []plumbing.Hash
	for _, objects := range []map[plumbing.Hash]plumbing.Hash{s.commits, s.tags} {
		for h := range objects {
			if strings.HasPrefix(h.String(), hex) {
				found = append(found, h)
			}
		}
	}
	plumbing.HashesSort(found)
	return found
}

// peel maps a tag chain to its commit and a commit to itself; false when h is not contained or peels to a tree or blob.
func (s *reachSet) peel(h plumbing.Hash) (plumbing.Hash, bool) {
	if end, ok := s.tags[h]; ok {
		h = end
	}
	if _, ok := s.commits[h]; !ok {
		return plumbing.ZeroHash, false
	}
	return h, true
}

// end returns the object an indexed h ends at once its tag chain is peeled.
func (s *reachSet) end(h plumbing.Hash) (plumbing.Hash, bool) {
	if end, ok := s.tags[h]; ok {
		return end, true
	}
	_, commit := s.commits[h]
	_, named := s.named[h]
	return h, commit || named
}

// walkTrees walks every tree the snapshot reaches, handing each blob w has not seen yet to blob.
func (s *reachSet) walkTrees(ctx context.Context, w *treeWalk, blob func(plumbing.Hash) error) error {
	for _, tree := range s.commits {
		if err := w.walk(ctx, tree, blob); err != nil {
			return err
		}
	}
	for h, typ := range s.named {
		var err error
		if typ == plumbing.TreeObject {
			err = w.walk(ctx, h, blob)
		} else {
			err = w.leaf(h, blob)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// reachCache is a handle's last snapshot with the refs and shallow commits it was indexed from.
type reachCache struct {
	mu      sync.Mutex
	set     *reachSet
	refs    map[plumbing.ReferenceName]plumbing.Hash
	shallow map[plumbing.Hash]struct{}
}

// reachable refreshes the handle's index against the CURRENT refs and returns a snapshot.
func (r *Repository) reachable(ctx context.Context) (*reachSet, error) {
	c := &r.reach
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	refs, err := r.refTargets()
	if err != nil {
		return nil, err
	}
	list, err := r.repo.Storer.Shallow()
	if err != nil {
		return nil, fmt.Errorf("failed to read shallow commits: %w", err)
	}
	shallow := make(map[plumbing.Hash]struct{}, len(list))
	for _, h := range list {
		shallow[h] = struct{}{}
	}

	var set *reachSet
	// Another shallow list ends history elsewhere, which only a rebuild reflects.
	if c.set != nil && maps.Equal(shallow, c.shallow) {
		if maps.Equal(refs, c.refs) {
			return c.set, nil
		}
		set = c.set.clone()
		met, err := r.indexRefs(ctx, set, shallow, refs)
		if err != nil {
			return nil, err
		}
		// An old target no walk met may be unreachable now, which only a rebuild tells.
		for _, h := range c.refs {
			if _, ok := met[h]; !ok {
				set = nil
				break
			}
		}
	}
	if set == nil {
		set = newReachSet()
		if _, err := r.indexRefs(ctx, set, shallow, refs); err != nil {
			return nil, err
		}
	}
	c.set, c.refs, c.shallow = set, refs, shallow
	return set, nil
}

// refTargets maps every hash ref, a detached HEAD included, to its target as git's files backend lists them; a symbolic one is covered by its target's own entry.
func (r *Repository) refTargets() (map[plumbing.ReferenceName]plumbing.Hash, error) {
	refs := map[plumbing.ReferenceName]plumbing.Hash{}
	// Only a handle on an in-memory storer has no filesystem, and so no ref file to find torn or locked.
	if r.fs == nil {
		iter, err := r.repo.References()
		if err != nil {
			return nil, fmt.Errorf("failed to get references: %w", err)
		}
		err = iter.ForEach(func(ref *plumbing.Reference) error {
			if ref.Type() == plumbing.HashReference {
				refs[ref.Name()] = ref.Hash()
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to read references: %w", err)
		}
		return refs, nil
	}
	head, err := r.repo.Storer.Reference(plumbing.HEAD)
	switch {
	// go-git reads an empty HEAD as a missing one.
	case errors.Is(err, plumbing.ErrReferenceNotFound):
	case err != nil:
		return nil, fmt.Errorf("failed to read HEAD: %w", err)
	case head.Type() == plumbing.HashReference:
		refs[plumbing.HEAD] = head.Hash()
	}
	fs := chroot.New(r.fs, r.repoPath)
	// Loose refs first: pack-refs writes a ref to packed-refs before it deletes the loose file.
	if err := looseRefTargets(fs, "refs", refs); err != nil {
		return nil, fmt.Errorf("failed to read loose references: %w", err)
	}
	if err := packedRefTargets(fs, refs); err != nil {
		return nil, fmt.Errorf("failed to read packed-refs: %w", err)
	}
	return refs, nil
}

// looseRefTargets adds the hash each loose ref below dir holds to refs, skipping lock files.
func looseRefTargets(fs billy.Filesystem, dir string, refs map[plumbing.ReferenceName]plumbing.Hash) error {
	entries, err := fs.ReadDir(dir)
	// A directory may go with the last ref in it.
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := path.Join(dir, entry.Name())
		switch {
		case strings.HasSuffix(name, ".lock"):
		case entry.IsDir():
			if err := looseRefTargets(fs, name, refs); err != nil {
				return err
			}
		default:
			h, ok, err := readLooseRef(fs, name)
			if err != nil {
				return err
			}
			if ok {
				refs[plumbing.ReferenceName(name)] = h
			}
		}
	}
	return nil
}

// readLooseRef returns the hash the loose ref file name holds; false once the file is gone or, as git skips a symbolic or broken ref, holds no hash.
func readLooseRef(fs billy.Filesystem, name string) (plumbing.Hash, bool, error) {
	data, err := util.ReadFile(fs, name)
	// go-git rewrites a loose ref in place, so a concurrent writer leaves it empty for a moment.
	if err == nil && len(bytes.TrimSpace(data)) == 0 {
		time.Sleep(time.Millisecond)
		data, err = util.ReadFile(fs, name)
	}
	if errors.Is(err, os.ErrNotExist) {
		return plumbing.ZeroHash, false, nil
	}
	if err != nil {
		return plumbing.ZeroHash, false, err
	}
	h, ok := refHash(string(bytes.TrimSpace(data)))
	return h, ok, nil
}

// packedRefTargets adds each packed-refs entry no loose ref holds; comment, peel and malformed lines name no ref.
func packedRefTargets(fs billy.Filesystem, refs map[plumbing.ReferenceName]plumbing.Hash) error {
	data, err := util.ReadFile(fs, "packed-refs")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		name := plumbing.ReferenceName(fields[1])
		if h, ok := refHash(fields[0]); ok {
			if _, loose := refs[name]; !loose {
				refs[name] = h
			}
		}
	}
	return nil
}

// refHash parses a ref's target, which only a full SHA-1 hex string names.
func refHash(s string) (plumbing.Hash, bool) {
	if len(s) != formatcfg.SHA1HexSize {
		return plumbing.ZeroHash, false
	}
	return plumbing.FromHex(s)
}

// indexRefs adds what refs reach to set and returns the ref targets together with the indexed objects the walks stopped at.
func (r *Repository) indexRefs(ctx context.Context, set *reachSet, shallow map[plumbing.Hash]struct{}, refs map[plumbing.ReferenceName]plumbing.Hash) (map[plumbing.Hash]struct{}, error) {
	w := &reachWalk{store: r.repo.Storer, shallow: shallow, set: set, met: map[plumbing.Hash]struct{}{}}
	for _, h := range refs {
		if err := w.add(ctx, h); err != nil {
			return nil, err
		}
		w.met[h] = struct{}{}
	}
	return w.met, nil
}

// reachWalk indexes history into set without expanding the parents of shallow commits, noting in met the indexed objects it stops at.
type reachWalk struct {
	store   storer.EncodedObjectStorer
	shallow map[plumbing.Hash]struct{}
	set     *reachSet
	met     map[plumbing.Hash]struct{}
}

// add indexes what h reaches, peeling annotated tags, up to the objects already indexed.
func (w *reachWalk) add(ctx context.Context, h plumbing.Hash) error {
	var chain []plumbing.Hash
	for {
		if end, ok := w.set.end(h); ok {
			w.met[h] = struct{}{}
			w.tag(chain, end)
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		obj, err := object.GetObject(w.store, h)
		if err != nil {
			return fmt.Errorf("read object %s: %w", h, err)
		}
		switch obj := obj.(type) {
		case *object.Tag:
			chain = append(chain, h)
			h = obj.Target
			continue
		case *object.Commit:
			w.tag(chain, h)
			return w.history(ctx, obj)
		}
		w.tag(chain, h)
		w.set.named[h] = obj.Type()
		return nil
	}
}

func (w *reachWalk) tag(chain []plumbing.Hash, end plumbing.Hash) {
	for _, h := range chain {
		w.set.tags[h] = end
	}
}

// history indexes start and its ancestors on an explicit stack, every parent of a merge included.
func (w *reachWalk) history(ctx context.Context, start *object.Commit) error {
	stack := []plumbing.Hash{start.Hash}
	for len(stack) > 0 {
		h := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := w.set.commits[h]; ok {
			w.met[h] = struct{}{}
			continue
		}
		commit := start
		if h != start.Hash {
			if err := ctx.Err(); err != nil {
				return err
			}
			var err error
			if commit, err = object.GetCommit(w.store, h); err != nil {
				return fmt.Errorf("read commit %s: %w", h, err)
			}
		}
		w.set.commits[h] = commit.TreeHash
		// A shallow commit's parents are absent by design.
		if _, ok := w.shallow[h]; !ok {
			stack = append(stack, commit.ParentHashes...)
		}
	}
	return nil
}

// treeWalk visits each tree and blob below its roots once, skipping gitlinks; go-git's TreeWalker ends at a missing subtree instead of failing.
type treeWalk struct {
	store storer.EncodedObjectStorer
	seen  map[plumbing.Hash]struct{}
}

// walk reads every tree below root not seen yet, handing each new blob to blob unless that is nil.
func (w *treeWalk) walk(ctx context.Context, root plumbing.Hash, blob func(plumbing.Hash) error) error {
	if _, ok := w.seen[root]; ok {
		return nil
	}
	w.seen[root] = struct{}{}
	stack := []plumbing.Hash{root}
	for len(stack) > 0 {
		h := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if err := ctx.Err(); err != nil {
			return err
		}
		tree, err := object.GetTree(w.store, h)
		if err != nil {
			return fmt.Errorf("read tree %s: %w", h, err)
		}
		for _, entry := range tree.Entries {
			switch entry.Mode {
			case filemode.Submodule:
			case filemode.Dir:
				if _, ok := w.seen[entry.Hash]; !ok {
					w.seen[entry.Hash] = struct{}{}
					stack = append(stack, entry.Hash)
				}
			default:
				if err := w.leaf(entry.Hash, blob); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// leaf hands h to blob the first time the walk sees it.
func (w *treeWalk) leaf(h plumbing.Hash, blob func(plumbing.Hash) error) error {
	if _, ok := w.seen[h]; ok {
		return nil
	}
	w.seen[h] = struct{}{}
	if blob == nil {
		return nil
	}
	return blob(h)
}

// reachItem is an object a connectivity check has yet to judge, of the type its referrer gives; AnyObject when that is unknown.
type reachItem struct {
	hash plumbing.Hash
	typ  plumbing.ObjectType
}

// connectivity is the state of one checkConnectivity call.
type connectivity struct {
	store     storer.EncodedObjectStorer
	set       *reachSet
	delivered func(plumbing.Hash) bool
	seen      map[plumbing.Hash]struct{}
	commits   []reachItem     // commits, tags and untyped objects, all judged before any tree or blob
	objects   []reachItem     // trees and blobs
	parents   []plumbing.Hash // indexed parents of the commits the check expanded
	walked    int             // how many of parents known holds
	known     *treeWalk       // trees and blobs the parents reach, or once all is set, everything indexed does
	all       bool
}

// checkConnectivity verifies that every object tips reach is either delivered or already reachable from the repository's refs.
func (r *Repository) checkConnectivity(ctx context.Context, tips []plumbing.Hash, delivered func(plumbing.Hash) bool) error {
	set, err := r.reachable(ctx)
	if err != nil {
		return err
	}
	c := &connectivity{
		store:     r.repo.Storer,
		set:       set,
		delivered: delivered,
		seen:      map[plumbing.Hash]struct{}{},
		known:     &treeWalk{store: r.repo.Storer, seen: map[plumbing.Hash]struct{}{}},
	}
	for _, h := range tips {
		c.queue(h, plumbing.AnyObject)
	}
	for {
		var it reachItem
		switch {
		case len(c.commits) > 0:
			it, c.commits = c.commits[len(c.commits)-1], c.commits[:len(c.commits)-1]
		case len(c.objects) > 0:
			it, c.objects = c.objects[len(c.objects)-1], c.objects[:len(c.objects)-1]
		default:
			return nil
		}
		if _, ok := c.seen[it.hash]; ok {
			continue
		}
		c.seen[it.hash] = struct{}{}
		if err := c.check(ctx, it); err != nil {
			return err
		}
	}
}

// check expands a delivered object, or without a delivered set one the refs do not reach yet; any other object must already be reachable.
func (c *connectivity) check(ctx context.Context, it reachItem) error {
	strict := c.delivered != nil
	if !strict || !c.delivered(it.hash) {
		ok, err := c.reaches(ctx, it, strict)
		if err != nil || ok {
			return err
		}
		// Strict mode never reads what the push did not deliver, whatever the pool holds.
		if strict {
			return fmt.Errorf("%w: %s", errMissingObject, it.hash)
		}
	}
	err := c.expand(ctx, it)
	if !errors.Is(err, plumbing.ErrObjectNotFound) {
		return err
	}
	// Classic mode needs every indexed tree only here: an object it can read expands to the same verdict.
	if !strict {
		if ok, err := c.reaches(ctx, it, true); err != nil || ok {
			return err
		}
	}
	return fmt.Errorf("%w: %s", errMissingObject, it.hash)
}

// reaches reports whether the refs reach it: a commit or tag through the index, a tree or blob through the parents' trees or, with all, through every indexed tree.
func (c *connectivity) reaches(ctx context.Context, it reachItem, all bool) (bool, error) {
	switch it.typ {
	case plumbing.CommitObject, plumbing.TagObject:
		return c.set.contains(it.hash), nil
	case plumbing.TreeObject, plumbing.BlobObject:
	default:
		if c.set.contains(it.hash) {
			return true, nil
		}
	}
	for ; c.walked < len(c.parents); c.walked++ {
		if err := c.known.walk(ctx, c.set.commits[c.parents[c.walked]], nil); err != nil {
			return false, err
		}
	}
	if _, ok := c.known.seen[it.hash]; ok || !all || c.all {
		return ok, nil
	}
	c.all = true
	if err := c.set.walkTrees(ctx, c.known, nil); err != nil {
		return false, err
	}
	_, ok := c.known.seen[it.hash]
	return ok, nil
}

// expand reads it, a blob only for its existence, and queues what it names.
func (c *connectivity) expand(ctx context.Context, it reachItem) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if it.typ == plumbing.BlobObject {
		return c.store.HasEncodedObject(it.hash)
	}
	encoded, err := c.store.EncodedObject(it.typ, it.hash)
	if err != nil {
		return err
	}
	obj, err := object.DecodeObject(c.store, encoded)
	if err != nil {
		return err
	}
	switch obj := obj.(type) {
	case *object.Commit:
		c.queue(obj.TreeHash, plumbing.TreeObject)
		for _, p := range obj.ParentHashes {
			if _, ok := c.set.commits[p]; ok {
				c.parents = append(c.parents, p)
			}
			c.queue(p, plumbing.CommitObject)
		}
	case *object.Tag:
		// The index peels by the target's own type, so the claimed one must not hide a commit.
		c.queue(obj.Target, plumbing.AnyObject)
	case *object.Tree:
		for _, entry := range obj.Entries {
			switch entry.Mode {
			case filemode.Submodule:
			case filemode.Dir:
				c.queue(entry.Hash, plumbing.TreeObject)
			default:
				c.queue(entry.Hash, plumbing.BlobObject)
			}
		}
	}
	return nil
}

func (c *connectivity) queue(h plumbing.Hash, typ plumbing.ObjectType) {
	if typ == plumbing.TreeObject || typ == plumbing.BlobObject {
		c.objects = append(c.objects, reachItem{h, typ})
	} else {
		c.commits = append(c.commits, reachItem{h, typ})
	}
}
