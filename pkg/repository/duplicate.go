package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v6/util"
)

// Duplicate copies what a bare repository needs (HEAD, config, packed-refs, shallow, refs, objects) from r to newPath, skipping git's transient files.
func (r *Repository) Duplicate(ctx context.Context, newPath string) (*Repository, error) {
	if _, err := r.fs.Stat(newPath); err == nil {
		return nil, fmt.Errorf("path %q exists: %w", newPath, ErrRepositoryAlreadyExists)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := r.fs.MkdirAll(newPath, 0755); err != nil {
		return nil, err
	}
	if err := r.copyRepo(ctx, newPath); err != nil {
		return nil, errors.Join(fmt.Errorf("failed to copy repository: %w", err), util.RemoveAll(r.fs, newPath))
	}
	lruCache.Remove(cacheKey{r.fs, newPath})
	dst, err := Open(r.fs, newPath)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to open repository copy: %w", err), util.RemoveAll(r.fs, newPath))
	}
	return dst, nil
}

func (r *Repository) copyRepo(ctx context.Context, dstRoot string) error {
	// Refs before objects so a racing push leaves no ref to a missing object; refs/ before packed-refs so a racing pack-refs drops none.
	for _, name := range []string{"HEAD", "config", "refs", "packed-refs", "shallow", "objects"} {
		info, err := r.fs.Lstat(filepath.Join(r.repoPath, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := r.copyEntry(ctx, dstRoot, name, info); err != nil {
			return err
		}
	}
	return nil
}

// copyDir copies r's directory rel, minus git's transient files, to the same place under dstRoot.
func (r *Repository) copyDir(ctx context.Context, dstRoot, rel string) error {
	entries, err := r.fs.ReadDir(filepath.Join(r.repoPath, rel))
	if err != nil {
		return err
	}
	// Name order copies the loose objects/00..ff before objects/pack, so a racing repack and prune-packed loses no object.
	slices.SortFunc(entries, func(a, b os.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	var packDir map[string]bool
	if rel == filepath.Join("objects", "pack") {
		packDir = make(map[string]bool, len(entries))
		for _, e := range entries {
			packDir[e.Name()] = true
		}
	}
	for _, e := range entries {
		if transient(rel, e.Name(), packDir) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		if err := r.copyEntry(ctx, dstRoot, filepath.Join(rel, e.Name()), info); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) copyEntry(ctx context.Context, dstRoot, rel string, info os.FileInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dst := filepath.Join(dstRoot, rel)
	switch mode := info.Mode(); {
	case mode.IsDir():
		if err := r.fs.MkdirAll(dst, mode.Perm()); err != nil {
			return err
		}
		return r.copyDir(ctx, dstRoot, rel)
	case mode.IsRegular() && (rel == "HEAD" || rel == "config" || strings.HasPrefix(rel, "refs")):
		// go-git truncates refs and config in place before rewriting them, so an empty read is retried once and then fails the copy.
		src := filepath.Join(r.repoPath, rel)
		data, err := util.ReadFile(r.fs, src)
		if err == nil && len(data) == 0 {
			data, err = util.ReadFile(r.fs, src)
		}
		if err != nil {
			return err
		}
		if len(data) == 0 {
			return fmt.Errorf("%s is empty", rel)
		}
		return util.WriteFile(r.fs, dst, data, mode.Perm())
	case mode.IsRegular():
		return r.copyFile(filepath.Join(r.repoPath, rel), dst, mode.Perm())
	}
	return nil
}

// transient reports whether name, in directory rel, is a git lock file, a temp file or receive-pack quarantine under objects or, per the objects/pack listing packDir, part of an incomplete pack.
func transient(rel, name string, packDir map[string]bool) bool {
	if strings.HasSuffix(name, ".lock") || strings.HasPrefix(rel, "objects") && (strings.HasPrefix(name, "tmp_") || strings.HasPrefix(name, ".tmp-") || strings.HasPrefix(name, "incoming-")) {
		return true
	}
	base, _, ok := strings.Cut(name, ".")
	return ok && packDir != nil && strings.HasPrefix(base, "pack-") && !(packDir[base+".pack"] && packDir[base+".idx"])
}

func (r *Repository) copyFile(src, dst string, perm os.FileMode) error {
	in, err := r.fs.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := r.fs.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	return errors.Join(err, out.Close())
}
