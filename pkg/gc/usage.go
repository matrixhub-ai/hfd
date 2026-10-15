package gc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// Usage sums the repositories' Git usage; it is not an atomic snapshot.
func (g *GC) Usage(ctx context.Context) (repository.Usage, error) {
	if err := ctx.Err(); err != nil {
		return repository.Usage{}, err
	}
	var total repository.Usage
	_, err := g.repos.Stat("/")
	switch {
	case errors.Is(err, fs.ErrNotExist):
		err = nil
	case err == nil:
		err = repository.Walk(ctx, g.repos, "/", func(path string) error {
			repo, err := repository.Open(g.repos, path)
			if err != nil {
				return fmt.Errorf("open %s: %w", path, err)
			}
			usage, err := repo.Usage(ctx)
			if err != nil {
				return fmt.Errorf("usage %s: %w", path, err)
			}
			total.Objects.Count += usage.Objects.Count
			total.Objects.Bytes += usage.Objects.Bytes
			total.Other.Count += usage.Other.Count
			total.Other.Bytes += usage.Other.Bytes
			return nil
		})
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return repository.Usage{}, err
	}
	return total, nil
}

// SharedObjectsUsage sums the loose objects of the store the repositories share, which no repository's Usage counts.
func (g *GC) SharedObjectsUsage(ctx context.Context) (repository.ObjectUsage, error) {
	if err := ctx.Err(); err != nil {
		return repository.ObjectUsage{}, err
	}
	var total repository.ObjectUsage
	err := repository.WalkSharedObjects(g.repos, func(_ plumbing.Hash, info fs.FileInfo) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		total.Count++
		total.Bytes += info.Size()
		return nil
	})
	if err != nil {
		return repository.ObjectUsage{}, err
	}
	return total, nil
}
