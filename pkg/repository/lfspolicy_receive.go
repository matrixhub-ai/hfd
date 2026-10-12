package repository

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/idxfile"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp/sideband"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/filesystem/dotgit"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// preReceiveDeclined is git's status for every command of a push its pre-receive hook refused.
const preReceiveDeclined = "pre-receive hook declined"

// hookRelaySize is the most of a hook's output git relays in one side-band packet.
const hookRelaySize = 128

// pushedPack is the storage a push unpacks into under the LFS policy: it notes the pack the push wrote.
type pushedPack struct {
	storer.Storer
	packs  storer.PackedObjectStorer // nil when the packs stored before the push are unknown
	before []plumbing.Hash
	hash   plumbing.Hash
	index  *idxfile.MemoryIndex // nil when the push stored nothing
	err    error
}

func newPushedPack(st storer.Storer) *pushedPack {
	p := &pushedPack{Storer: st}
	packs, ok := st.(storer.PackedObjectStorer)
	if !ok {
		p.err = errors.New("storage keeps no packfiles")
		return p
	}
	if p.before, p.err = packs.ObjectPacks(); p.err == nil {
		p.packs = packs
	}
	return p
}

// PackfileWriter takes the index go-git builds while it writes the pack.
func (p *pushedPack) PackfileWriter() (io.WriteCloser, error) {
	// Without the packs stored before, discard could not drop the pack of a refused push, so none is stored.
	if p.err != nil {
		return nil, p.err
	}
	pw, ok := p.Storer.(storer.PackfileWriter)
	if !ok {
		return nil, errors.New("storage writes no packfiles")
	}
	w, err := pw.PackfileWriter()
	if err != nil {
		return nil, err
	}
	dw, ok := w.(*dotgit.PackWriter)
	if !ok {
		// Without seeing the pack, discard could not drop it were the push refused, so none is stored.
		return nil, errors.Join(fmt.Errorf("cannot see the pack %T writes", w), w.Close())
	}
	notify := dw.Notify
	dw.Notify = func(h plumbing.Hash, iw *idxfile.Writer) {
		if notify != nil {
			notify(h, iw)
		}
		index, err := iw.Index()
		p.hash, p.index, p.err = h, index, errors.Join(p.err, err)
	}
	return dw, nil
}

// discard deletes the pack the push added; a pack stored before the push stays.
func (p *pushedPack) discard() error {
	if p.packs == nil || p.index == nil || slices.Contains(p.before, p.hash) {
		return nil
	}
	return p.packs.DeleteOldObjectPackAndIndex(p.hash, time.Time{})
}

// dropPack deletes the pack of a push refused before its refs update, like git dropping its quarantine, so no later push can point a ref at objects never judged.
func (r *Repository) dropPack(ctx context.Context, pushed *pushedPack) {
	if err := pushed.discard(); err != nil {
		slog.ErrorContext(ctx, "deleting the pack of a refused push", "repository", r.repoPath, "error", err)
	}
	// Cached objects may still point into the deleted pack.
	r.objectCache.Clear()
}

// pushLocks holds a lock per repository that go-git pushes under the LFS policy take in turn. Keyed like lruCache, as
// an evicted Repository comes back as another value; hfd processes sharing one storage do not share these locks.
// Held across the pack's upload: one stalled push blocks every other go-git push to the repository until its request ends.
var pushLocks = struct {
	sync.Mutex
	m map[cacheKey]*pushLock
}{m: make(map[cacheKey]*pushLock)}

// pushLock is a repository's lock and the number of pushes holding or awaiting it.
type pushLock struct {
	sync.Mutex
	pushes int
}

// lockPushes waits for the earlier pushes to key and returns the unlock that lets the next one in.
func lockPushes(key cacheKey) (unlock func()) {
	pushLocks.Lock()
	l := pushLocks.m[key]
	if l == nil {
		l = &pushLock{}
		pushLocks.m[key] = l
	}
	l.pushes++
	pushLocks.Unlock()
	l.Lock()
	return func() {
		l.Unlock()
		pushLocks.Lock()
		if l.pushes--; l.pushes == 0 {
			delete(pushLocks.m, key)
		}
		pushLocks.Unlock()
	}
}

// lfsPolicyError judges every blob of the push's own pack: first those each command's tip reaches, depth first in tree
// entry order with a commit's parents after its tree, then the rest in index order. A blob stored before the push passes
// when a push references it without sending it, where native git judges whatever no ref reached yet.
func (r *Repository) lfsPolicyError(p lfs.Policy, cmds []*packp.Command, pushed *pushedPack) (*lfs.PolicyError, error) {
	if pushed.err != nil || pushed.index == nil {
		return nil, pushed.err
	}
	st := r.repo.Storer
	var large, binary []lfs.OffendingFile
	judge := func(obj plumbing.EncodedObject, file lfs.OffendingFile) error {
		v, err := blobViolation(p, obj)
		switch v {
		case lfs.ViolationLarge:
			large = append(large, file)
		case lfs.ViolationBinary:
			binary = append(binary, file)
		}
		return err
	}
	type entry struct {
		hash plumbing.Hash
		path string
	}
	seen := make(map[plumbing.Hash]bool)
	var first string // the ref of the first command that does not delete
	for _, cmd := range cmds {
		if cmd.Action() == packp.Delete {
			continue
		}
		if first == "" {
			first = cmd.Name.String()
		}
		// updateReference refuses a missing tip itself, as git does, with "missing necessary objects".
		if st.HasEncodedObject(cmd.New) != nil {
			continue
		}
		// Children go on the stack in reverse, so trees are walked depth first in entry order.
		stack := []entry{{hash: cmd.New}}
		for len(stack) > 0 {
			e := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if seen[e.hash] {
				continue
			}
			seen[e.hash] = true
			fresh, err := pushed.index.Contains(e.hash)
			if err != nil {
				return nil, err
			}
			if !fresh {
				// Stored before this push, and judged when it came; what was neither sent nor stored cannot be judged.
				if err := st.HasEncodedObject(e.hash); err != nil {
					return nil, err
				}
				continue
			}
			obj, err := st.EncodedObject(plumbing.AnyObject, e.hash)
			if err != nil {
				return nil, err
			}
			switch obj.Type() {
			case plumbing.TagObject:
				var tag object.Tag
				if err := tag.Decode(obj); err != nil {
					return nil, err
				}
				stack = append(stack, entry{hash: tag.Target})
			case plumbing.CommitObject:
				var commit object.Commit
				if err := commit.Decode(obj); err != nil {
					return nil, err
				}
				for _, parent := range slices.Backward(commit.ParentHashes) {
					stack = append(stack, entry{hash: parent})
				}
				stack = append(stack, entry{hash: commit.TreeHash})
			case plumbing.TreeObject:
				var tree object.Tree
				if err := tree.Decode(obj); err != nil {
					return nil, err
				}
				for _, child := range slices.Backward(tree.Entries) {
					if child.Mode == filemode.Submodule {
						continue
					}
					name := child.Name
					if e.path != "" {
						name = e.path + "/" + name
					}
					stack = append(stack, entry{hash: child.Hash, path: name})
				}
			case plumbing.BlobObject:
				if err := judge(obj, lfs.OffendingFile{Path: e.path, Ref: cmd.Name.String()}); err != nil {
					return nil, err
				}
			}
		}
	}
	// A hand-built pack can carry blobs no tip reaches, which a later push could point a ref at without sending them.
	entries, err := pushed.index.Entries()
	if err != nil {
		return nil, err
	}
	defer func() { _ = entries.Close() }()
	for {
		ie, err := entries.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		// The iterator may leave bytes set past the hash's size, which map keys compare.
		h, _ := plumbing.FromBytes(ie.Hash.Bytes())
		if seen[h] {
			continue
		}
		obj, err := st.EncodedObject(plumbing.AnyObject, h)
		if err != nil {
			return nil, err
		}
		if obj.Type() != plumbing.BlobObject {
			continue
		}
		// Only hfd refuses such a blob, which native git stores unjudged: the banner names it by ID under the first ref.
		if err := judge(obj, lfs.OffendingFile{Path: h.String(), Ref: first}); err != nil {
			return nil, err
		}
	}
	switch {
	case len(large) > 0:
		return &lfs.PolicyError{Kind: lfs.ViolationLarge, Files: large, Limit: p.MaxRegularFileSize}, nil
	case len(binary) > 0:
		return &lfs.PolicyError{Kind: lfs.ViolationBinary, Files: binary}, nil
	}
	return nil, nil
}

// blobViolation classifies a blob, reading at most its first lfs.BinarySniffSize bytes and only once its size breaks a limit.
func blobViolation(p lfs.Policy, obj plumbing.EncodedObject) (lfs.Violation, error) {
	size := obj.Size()
	over := func(limit int64) bool { return limit > 0 && size > limit }
	if !over(p.MaxRegularFileSize) && !over(p.MaxBinaryFileSize) {
		return lfs.ViolationNone, nil
	}
	rd, err := obj.Reader()
	if err != nil {
		return lfs.ViolationNone, err
	}
	defer func() { _ = rd.Close() }()
	head := make([]byte, min(size, lfs.BinarySniffSize))
	if _, err := io.ReadFull(rd, head); err != nil {
		return lfs.ViolationNone, err
	}
	return p.Check(size, head), nil
}

// refusePush declines every command as a failing pre-receive hook does, relaying perr's banner first; cause is why the push could not be judged.
func (r *Repository) refusePush(ctx context.Context, output io.Writer, input io.Reader, req *packp.UpdateRequests, perr *lfs.PolicyError, cause error) error {
	if cause != nil {
		slog.ErrorContext(ctx, "refusing a push the LFS policy cannot judge", "repository", r.repoPath, "error", cause)
	}
	if perr != nil && req.Capabilities.Supports(capability.Sideband64k) {
		var banner bytes.Buffer
		_ = perr.WriteBanner(&banner)
		mux := sideband.NewMuxer(sideband.Sideband64k, output)
		for b := banner.Bytes(); len(b) > 0; {
			n := min(len(b), hookRelaySize)
			if _, err := mux.WriteChannel(sideband.ProgressMessage, b[:n]); err != nil {
				return err
			}
			b = b[n:]
		}
	}
	return refuseCommands(output, input, req, preReceiveDeclined)
}

//go:embed pre-receive.sh
var preReceiveScript []byte

// policyHooks is this process's private hooks directory holding preReceiveScript as pre-receive.
var policyHooks struct {
	sync.Mutex
	dir string
}

// lfsPolicyHooks returns the hooks directory native git runs the policy from, writing a new one when the hook is gone or altered.
func lfsPolicyHooks(ctx context.Context) (string, error) {
	policyHooks.Lock()
	defer policyHooks.Unlock()
	// Checked per push, as tmp cleaners may delete the hook; git silently skips one deleted after this check.
	if policyHooks.dir != "" && hookIntact(filepath.Join(policyHooks.dir, "pre-receive")) {
		return policyHooks.dir, nil
	}
	// Absolute: git resolves a relative core.hooksPath against the repository, where it finds no hook.
	tmp, err := filepath.Abs(os.TempDir())
	if err != nil {
		return "", err
	}
	// Created private: a predictable path in a shared temporary directory could be planted by another user.
	dir, err := os.MkdirTemp(tmp, "hfd-hooks-")
	if err != nil {
		return "", err
	}
	if err := writeHook(ctx, filepath.Join(dir, "pre-receive")); err != nil {
		_ = os.RemoveAll(dir)
		return "", err
	}
	policyHooks.dir = dir
	return dir, nil
}

func hookIntact(path string) bool {
	fi, err := os.Lstat(path)
	if err != nil || !fi.Mode().IsRegular() || fi.Mode().Perm() != 0o755 {
		return false
	}
	b, err := os.ReadFile(path)
	return err == nil && bytes.Equal(b, preReceiveScript)
}

// writeHook writes the hook and runs it once without commands: git skips a hook it cannot execute, which would accept every push.
func writeHook(ctx context.Context, path string) error {
	if err := os.WriteFile(path, preReceiveScript, 0o755); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o755); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, path)
	cmd.Env = append(os.Environ(), "HFD_LFS_MAX_REGULAR_SIZE=0", "HFD_LFS_MAX_BINARY_SIZE=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("pre-receive hook does not run: %w: %s", err, bytes.TrimSpace(out))
	}
	return nil
}

// lfsPolicyEnv hands the hook the policy and the banners lfs.PolicyError renders, so both engines print the same bytes.
func lfsPolicyEnv(p lfs.Policy) []string {
	large := (&lfs.PolicyError{Kind: lfs.ViolationLarge, Limit: p.MaxRegularFileSize}).Banner()
	binary := (&lfs.PolicyError{Kind: lfs.ViolationBinary}).Banner()
	return []string{
		"HFD_GIT=" + gitBinary,
		"HFD_LFS_MAX_REGULAR_SIZE=" + strconv.FormatInt(p.MaxRegularFileSize, 10),
		"HFD_LFS_MAX_BINARY_SIZE=" + strconv.FormatInt(p.MaxBinaryFileSize, 10),
		"HFD_LFS_LARGE_BANNER=" + strings.Join(large, "\n"),
		"HFD_LFS_BINARY_BANNER=" + strings.Join(binary, "\n"),
	}
}
