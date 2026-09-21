package repository

import (
	"context"
	"crypto"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/idxfile"
	"github.com/go-git/go-git/v6/plumbing/hash"
)

// Usage includes unreachable Git objects but excludes LFS/Xet content.
type Usage struct {
	Objects ObjectUsage
	Other   ObjectUsage
}

type ObjectUsage struct {
	Count int64
	Bytes int64
}

// Usage skips vanished entries and is not an atomic snapshot.
func (r *Repository) Usage(ctx context.Context) (Usage, error) {
	if err := ctx.Err(); err != nil {
		return Usage{}, err
	}
	objects, err := r.objectUsage(ctx)
	if err != nil {
		return Usage{}, err
	}
	other, err := r.otherUsage(ctx)
	if err != nil {
		return Usage{}, err
	}
	return Usage{Objects: objects, Other: other}, nil
}

func (r *Repository) objectUsage(ctx context.Context) (ObjectUsage, error) {
	objects := filepath.Join(r.repoPath, "objects")
	info, err := r.fs.Lstat(objects)
	if err != nil {
		return ObjectUsage{}, err
	}
	if !info.IsDir() {
		if _, err := r.fs.Stat(objects); err != nil {
			return ObjectUsage{}, err
		}
		return ObjectUsage{}, fmt.Errorf("%s: not a directory", objects)
	}
	config, err := r.repo.Config()
	if err != nil {
		return ObjectUsage{}, err
	}
	hexSize := config.Extensions.ObjectFormat.HexSize()
	pack := filepath.Join(objects, "pack")
	var usage ObjectUsage
	seen := map[plumbing.Hash]struct{}{}
	err = r.walkFiles(ctx, objects, func(name string, info os.FileInfo) error {
		if info.IsDir() {
			return nil
		}
		usage.Bytes += info.Size()
		dir, base := filepath.Dir(name), filepath.Base(name)
		switch {
		case filepath.Dir(dir) == objects && len(filepath.Base(dir)) == 2:
			if id, ok := objectID(filepath.Base(dir)+base, hexSize); ok {
				seen[id] = struct{}{}
			}
		case dir == pack && strings.HasPrefix(base, "pack-") && strings.HasSuffix(base, ".idx"):
			if id, ok := objectID(strings.TrimSuffix(strings.TrimPrefix(base, "pack-"), ".idx"), hexSize); ok {
				return r.packIDs(name, id.Size(), seen)
			}
		}
		return nil
	})
	if err != nil {
		return ObjectUsage{}, err
	}
	usage.Count = int64(len(seen))
	return usage, nil
}

func objectID(hex string, size int) (plumbing.Hash, bool) {
	if len(hex) != size {
		return plumbing.Hash{}, false
	}
	return plumbing.FromHex(hex)
}

func (r *Repository) packIDs(name string, size int, seen map[plumbing.Hash]struct{}) error {
	if _, err := r.fs.Stat(strings.TrimSuffix(name, ".idx") + ".pack"); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	file, err := r.fs.Open(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = file.Close() }()
	hasher := hash.New(crypto.SHA1)
	if size == crypto.SHA256.Size() {
		hasher = hash.New(crypto.SHA256)
	}
	idx := idxfile.NewMemoryIndex(size)
	if err := idxfile.NewDecoder(file, hasher).Decode(idx); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	iter, err := idx.Entries()
	if err != nil {
		return err
	}
	for {
		entry, err := iter.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		// go-git alpha.5 index entries can carry non-hash padding.
		id, _ := plumbing.FromBytes(entry.Hash.Bytes())
		seen[id] = struct{}{}
	}
}

func (r *Repository) otherUsage(ctx context.Context) (ObjectUsage, error) {
	var usage ObjectUsage
	objects := filepath.Join(r.repoPath, "objects")
	err := r.walkFiles(ctx, r.repoPath, func(name string, info os.FileInfo) error {
		switch {
		case name == objects && info.IsDir():
			return filepath.SkipDir
		case !info.IsDir():
			usage.Count++
			usage.Bytes += info.Size()
		}
		return nil
	})
	if err != nil {
		return ObjectUsage{}, err
	}
	return usage, nil
}

func (r *Repository) walkFiles(ctx context.Context, root string, fn func(name string, info os.FileInfo) error) error {
	return util.Walk(r.fs, root, func(name string, info os.FileInfo, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if name != root && errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		return fn(name, info)
	})
}
