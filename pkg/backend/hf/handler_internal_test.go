package hf

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

func newStorage(t *testing.T, dir string) *storage.Storage {
	t.Helper()
	st, err := storage.NewStorage(storage.WithRootDir(dir))
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	return st
}

// TestOpenRepoInvalidNameSkipsHook pins that an unresolvable name fails as missing before the pre-open hook runs.
func TestOpenRepoInvalidNameSkipsHook(t *testing.T) {
	calls := 0
	h := NewHandler(WithPreOpenHookFunc(func(context.Context, string, bool) error {
		calls++
		return nil
	}))
	if _, err := h.openRepo(context.Background(), "x/../repo", true); !errors.Is(err, repository.ErrRepositoryNotExists) {
		t.Fatalf("openRepo error = %v, want ErrRepositoryNotExists", err)
	}
	if calls != 0 {
		t.Fatalf("hook calls = %d, want none", calls)
	}
}
