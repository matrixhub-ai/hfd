package gc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/repository"
)

type Usage struct {
	repository.Usage
	Xet xetstorage.Usage
}

// Usage is not an atomic snapshot.
func (c *Collector) Usage(ctx context.Context) (Usage, error) {
	if err := ctx.Err(); err != nil {
		return Usage{}, err
	}
	var total Usage
	if c.store != nil {
		xet, err := c.store.Usage(ctx)
		if err != nil {
			return Usage{}, fmt.Errorf("xet usage: %w", err)
		}
		total.Xet = xet
	}
	_, err := c.repos.Stat("/")
	switch {
	case errors.Is(err, fs.ErrNotExist):
		err = nil
	case err == nil:
		err = repository.Walk(ctx, c.repos, "/", func(path string) error {
			repo, err := repository.Open(c.repos, path)
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
		return Usage{}, err
	}
	return total, nil
}
