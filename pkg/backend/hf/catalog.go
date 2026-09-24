package hf

import (
	"context"
	"errors"
	"net/http"

	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// CreateRepoFunc creates repoName and returns the ref updates of its initial commit, if any.
type CreateRepoFunc func(ctx context.Context, repoName string, req CreateRepoRequest) ([]receive.RefUpdate, error)

// DeleteRepoFunc deletes repoName.
type DeleteRepoFunc func(ctx context.Context, repoName string) error

// MoveRepoFunc renames fromName to toName.
type MoveRepoFunc func(ctx context.Context, fromName, toName string) error

// UpdateRepoSettingsFunc applies settings to repoName.
type UpdateRepoSettingsFunc func(ctx context.Context, repoName string, settings RepoSettings) error

// ListReposFunc returns one page of repoType repositories and whether more follow.
type ListReposFunc func(ctx context.Context, repoType string, q ListQuery) ([]RepoListItem, bool, error)

// WhoamiFunc describes the authenticated caller.
type WhoamiFunc func(ctx context.Context) (*WhoamiResponse, error)

// ListQuery is the parsed repository list query; Offset comes from the cursor.
type ListQuery struct {
	Search     string
	Author     string
	FilterTags []string
	SortField  string
	Limit      int
	Offset     int
}

// WithCreateRepoFunc registers POST /api/repos/create.
func WithCreateRepoFunc(fn CreateRepoFunc) Option {
	return func(h *Handler) {
		h.createRepoFunc = fn
	}
}

// WithDeleteRepoFunc registers DELETE /api/repos/delete.
func WithDeleteRepoFunc(fn DeleteRepoFunc) Option {
	return func(h *Handler) {
		h.deleteRepoFunc = fn
	}
}

// WithMoveRepoFunc registers POST /api/repos/move.
func WithMoveRepoFunc(fn MoveRepoFunc) Option {
	return func(h *Handler) {
		h.moveRepoFunc = fn
	}
}

// WithUpdateRepoSettingsFunc registers PUT /api/{repoType}/{namespace}/{repo}/settings.
func WithUpdateRepoSettingsFunc(fn UpdateRepoSettingsFunc) Option {
	return func(h *Handler) {
		h.updateRepoSettingsFunc = fn
	}
}

// WithListReposFunc registers GET /api/{repoType}.
func WithListReposFunc(fn ListReposFunc) Option {
	return func(h *Handler) {
		h.listReposFunc = fn
	}
}

// WithWhoamiFunc registers GET /api/whoami-v2.
func WithWhoamiFunc(fn WhoamiFunc) Option {
	return func(h *Handler) {
		h.whoamiFunc = fn
	}
}

// respondCatalogError maps callback errors to HTTP status codes.
func respondCatalogError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repository.ErrRepositoryNotExists):
		responseJSON(w, err, http.StatusNotFound)
	case errors.Is(err, repository.ErrRepositoryAlreadyExists):
		responseJSON(w, err, http.StatusConflict)
	default:
		responseJSON(w, err, http.StatusInternalServerError)
	}
}
