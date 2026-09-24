package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v6"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	"github.com/matrixhub-ai/hfd/pkg/hfmeta"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// Hooks is the default hook set: mirror pull before opens (at most once per PullTTL per repository), logging on receive, push-mirroring after receive, and the HF catalog callbacks over the repositories on Storage.
type Hooks struct {
	Storage    *storage.Storage
	Mirror     *mirror.Mirror
	ProxyToken string
	PullTTL    time.Duration
	lastPull   sync.Map
}

// PreOpen syncs mirror sources before the repository is opened, at most once per PullTTL per repository.
func (h *Hooks) PreOpen(ctx context.Context, repoName string, write bool) error {
	if h.Mirror == nil {
		return nil
	}
	repoPath := repository.ResolvePath(repoName)
	if repoPath == "" {
		slog.WarnContext(ctx, "Cannot resolve repo path for push mirror", "repo", repoName)
		return nil
	}

	isMirror, err := h.Mirror.IsMirrorSource(ctx, repoName)
	if err != nil {
		return err
	}
	if !isMirror {
		return nil
	}

	if h.PullTTL > 0 {
		if last, ok := h.lastPull.Load(repoPath); ok && time.Since(last.(time.Time)) < h.PullTTL {
			return nil
		}
	}
	if err := h.Mirror.PullFromRemote(context.Background(), repoPath, repoName, nil); err != nil {
		return err
	}
	if h.PullTTL > 0 {
		h.lastPull.Store(repoPath, time.Now())
	}
	return nil
}

// PreReceive logs and allows every ref update.
func (h *Hooks) PreReceive(ctx context.Context, repoName string, updates []receive.RefUpdate) (bool, error) {
	for _, e := range updates {
		slog.InfoContext(ctx, "Pre-receive hook", "user", authenticate.IdentityFrom(ctx).Name(), "repo", repoName, "event", e.String(),
			"ref", e.RefName(), "old", e.OldRev(), "new", e.NewRev())
	}
	return true, nil
}

// PostReceive logs ref updates and pushes branch/tag changes to the push mirror.
func (h *Hooks) PostReceive(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
	for _, e := range updates {
		slog.InfoContext(ctx, "Post-receive hook", "user", authenticate.IdentityFrom(ctx).Name(), "repo", repoName, "event", e.String(),
			"ref", e.RefName(), "old", e.OldRev(), "new", e.NewRev())
	}

	if h.Mirror == nil {
		return nil
	}

	repoPath := repository.ResolvePath(repoName)
	if repoPath == "" {
		slog.WarnContext(ctx, "Cannot resolve repo path for push mirror", "repo", repoName)
		return nil
	}

	shouldPush := false
	for _, u := range updates {
		if strings.HasPrefix(u.RefName(), "refs/heads/") || strings.HasPrefix(u.RefName(), "refs/tags/") {
			shouldPush = true
			break
		}
	}
	if !shouldPush {
		slog.InfoContext(ctx, "Skip push mirror for non-branch/tag refs", "repo", repoName)
		return nil
	}

	return h.Mirror.PushToRemote(context.Background(), repoPath, repoName, nil)
}

// GitOutput provides the writer for mirror git command output.
func (h *Hooks) GitOutput(ctx context.Context, repoName string) io.Writer {
	slog.InfoContext(ctx, "Git command output", "user", authenticate.IdentityFrom(ctx).Name(), "repo", repoName)
	return os.Stdout
}

// SyncUserInfo supplies credentials for mirror syncs from the proxy token.
func (h *Hooks) SyncUserInfo(ctx context.Context, repoName string) (*url.Userinfo, error) {
	slog.InfoContext(ctx, "Get sync user info", "user", authenticate.IdentityFrom(ctx).Name(), "repo", repoName)
	if h.ProxyToken != "" {
		return url.UserPassword("git", h.ProxyToken), nil
	}
	return nil, nil
}

// MirrorRefFilter restricts mirror syncs to branches and tags.
func (h *Hooks) MirrorRefFilter(ctx context.Context, repoName string, remoteRefs []string) ([]string, error) {
	var filtered []string
	for _, ref := range remoteRefs {
		if strings.HasPrefix(ref, "refs/heads/") || strings.HasPrefix(ref, "refs/tags/") {
			filtered = append(filtered, ref)
		}
	}
	slog.InfoContext(ctx, "Mirror ref filter", "repo", repoName, "remoteRefs", remoteRefs, "filteredRefs", filtered)
	return filtered, nil
}

// CreateRepo initializes a bare repository with the default .gitattributes; an existing one is left as is.
func (h *Hooks) CreateRepo(ctx context.Context, repoName string, request backendhf.CreateRepoRequest) ([]receive.RefUpdate, error) {
	user := authenticate.IdentityFrom(ctx)
	slog.InfoContext(ctx, "Create repository", "user", user.Name(), "repo", repoName, "private", request.Private)
	fs := h.Storage.RepositoriesFS()
	repoPath := repository.ResolvePath(repoName)
	if repository.IsRepository(fs, repoPath) {
		return nil, nil
	}
	if err := fs.MkdirAll(filepath.Dir(repoPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create repository directory: %w", err)
	}

	defaultBranch := "main"
	repo, err := repository.Init(ctx, fs, repoPath, defaultBranch)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize repository: %w", err)
	}

	authorName, authorEmail := user.Name(), user.Email()
	if authenticate.IsAnonymous(user) {
		authorName, authorEmail = "HuggingFace", "hf@users.noreply.huggingface.co"
	}
	commitHash, err := repo.CreateCommit(context.Background(), defaultBranch, "Initial commit", authorName, authorEmail, []repository.CommitOperation{
		{
			Type:    repository.CommitOperationAdd,
			Path:    repository.GitattributesFileName,
			Content: repository.GitattributesText,
		},
	}, "")
	if err != nil {
		_ = repo.Remove()
		return nil, fmt.Errorf("failed to create initial commit: %w", err)
	}
	return []receive.RefUpdate{repo.RefUpdate(receive.ZeroHash, commitHash, "refs/heads/"+defaultBranch)}, nil
}

// openRepo opens repoName, naming it in the error like the hf backend's open path does.
func (h *Hooks) openRepo(repoName string) (*repository.Repository, error) {
	repo, err := repository.Open(h.Storage.RepositoriesFS(), repository.ResolvePath(repoName))
	if errors.Is(err, repository.ErrRepositoryNotExists) {
		return nil, fmt.Errorf("repository %q not found: %w", repoName, err)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open repository %q: %w", repoName, err)
	}
	return repo, nil
}

// DeleteRepo removes the repository.
func (h *Hooks) DeleteRepo(ctx context.Context, repoName string) error {
	slog.InfoContext(ctx, "Delete repository", "user", authenticate.IdentityFrom(ctx).Name(), "repo", repoName)
	repo, err := h.openRepo(repoName)
	if err != nil {
		return err
	}
	if err := repo.Remove(); err != nil {
		return fmt.Errorf("failed to delete repository %q: %w", repoName, err)
	}
	return nil
}

// MoveRepo renames the repository; an existing destination is a conflict.
func (h *Hooks) MoveRepo(ctx context.Context, fromName, toName string) error {
	slog.InfoContext(ctx, "Move repository", "user", authenticate.IdentityFrom(ctx).Name(), "from", fromName, "to", toName)
	repo, err := h.openRepo(fromName)
	if err != nil {
		return err
	}
	toPath := repository.ResolvePath(toName)
	if repository.IsRepository(h.Storage.RepositoriesFS(), toPath) {
		return fmt.Errorf("destination repository %q: %w", toName, repository.ErrRepositoryAlreadyExists)
	}
	if err := repo.Move(toPath); err != nil {
		return fmt.Errorf("failed to move repository: %w", err)
	}
	return nil
}

// UpdateRepoSettings accepts the payload; private and gated are not enforced here.
func (h *Hooks) UpdateRepoSettings(ctx context.Context, repoName string, _ backendhf.RepoSettings) error {
	slog.InfoContext(ctx, "Update repository settings", "user", authenticate.IdentityFrom(ctx).Name(), "repo", repoName)
	_, err := h.openRepo(repoName)
	return err
}

// ListRepos returns the repositories of repoType matching query, sorted and paged.
func (h *Hooks) ListRepos(ctx context.Context, repoType string, query backendhf.ListQuery) ([]backendhf.RepoListItem, bool, error) {
	slog.InfoContext(ctx, "List repositories", "user", authenticate.IdentityFrom(ctx).Name(), "repoType", repoType, "author", query.Author, "search", query.Search)
	isModel := repoType == "models"

	baseDir := "/"
	if !isModel {
		baseDir = filepath.Join("/", repoType)
	}

	items := listRepoItems(ctx, h.Storage.RepositoriesFS(), baseDir, isModel, query)

	sortRepoItems(items, query.SortField)

	if query.Offset >= len(items) {
		items = nil
	} else if query.Offset > 0 {
		items = items[query.Offset:]
	}

	more := query.Limit > 0 && len(items) > query.Limit
	if more {
		items = items[:query.Limit]
	}
	return items, more, nil
}

// Whoami reports the authenticated identity as a user holding a write token.
func (h *Hooks) Whoami(ctx context.Context) (*backendhf.WhoamiResponse, error) {
	user := authenticate.IdentityFrom(ctx)
	slog.InfoContext(ctx, "Whoami", "user", user.Name())
	return &backendhf.WhoamiResponse{
		Type:          "user",
		ID:            user.Name(),
		Name:          user.Name(),
		Fullname:      user.Name(),
		Email:         user.Email(),
		EmailVerified: false,
		IsPro:         false,
		CanPay:        false,
		Orgs:          []any{},
		Auth: backendhf.WhoamiAuth{
			AccessToken: backendhf.WhoamiAccessToken{
				DisplayName: "token",
				Role:        "write",
			},
		},
	}, nil
}

// listRepoItems walks the namespaces under baseDir (or only query.Author's) and returns the items passing the search and tag filters.
func listRepoItems(ctx context.Context, fs billy.Filesystem, baseDir string, isModel bool, query backendhf.ListQuery) []backendhf.RepoListItem {
	var roots []string
	if query.Author != "" {
		// An author is one path element; a slash or dot path would walk out of its namespace.
		if strings.Contains(query.Author, "/") || query.Author == "." || query.Author == ".." || (isModel && (query.Author == "datasets" || query.Author == "spaces" || query.Author == "kernels")) {
			return nil
		}
		roots = []string{filepath.Join(baseDir, query.Author)}
	} else {
		namespaces, err := fs.ReadDir(baseDir)
		if err != nil {
			return nil
		}
		for _, namespace := range namespaces {
			if !namespace.IsDir() {
				continue
			}
			// For models, skip the datasets/ and spaces/ directories
			if isModel && (namespace.Name() == "datasets" || namespace.Name() == "spaces" || namespace.Name() == "kernels") {
				continue
			}
			roots = append(roots, filepath.Join(baseDir, namespace.Name()))
		}
	}

	var items []backendhf.RepoListItem
	for _, root := range roots {
		// An unreadable directory ends this namespace's walk; the others still get listed.
		_ = repository.Walk(ctx, fs, root, func(path string) error {
			rel, _ := filepath.Rel(baseDir, path)
			fullName := strings.TrimSuffix(rel, ".git")
			if query.Search != "" && !strings.Contains(strings.ToLower(fullName), strings.ToLower(query.Search)) {
				return nil
			}

			item := backendhf.RepoListItem{
				RepoID: fullName,
			}
			if isModel {
				item.ModelID = fullName
			}

			if repo, err := repository.Open(fs, path); err == nil {
				meta := hfmeta.Collect(repo, repo.DefaultBranch())
				item.Tags = meta.Tags
				item.PipelineTag = meta.PipelineTag
				item.LibraryName = meta.LibraryName
			}

			if len(query.FilterTags) > 0 && !matchesAllTags(item.Tags, query.FilterTags) {
				return nil
			}

			items = append(items, item)
			return nil
		})
	}
	return items
}

// matchesAllTags checks if the repo tags contain all the filter tags.
func matchesAllTags(repoTags, filterTags []string) bool {
	tagSet := make(map[string]struct{}, len(repoTags))
	for _, tag := range repoTags {
		tagSet[tag] = struct{}{}
	}
	for _, filter := range filterTags {
		if _, ok := tagSet[filter]; !ok {
			return false
		}
	}
	return true
}

// sortRepoItems sorts the list by the given field.
func sortRepoItems(items []backendhf.RepoListItem, sortField string) {
	switch sortField {
	case "downloads":
		sort.Slice(items, func(i, j int) bool {
			return items[i].Downloads > items[j].Downloads
		})
	case "likes":
		sort.Slice(items, func(i, j int) bool {
			return items[i].Likes > items[j].Likes
		})
	case "trending_score", "trendingScore":
		sort.Slice(items, func(i, j int) bool {
			return items[i].TrendingScore > items[j].TrendingScore
		})
	default:
		// Default: sort alphabetically by ID
		sort.Slice(items, func(i, j int) bool {
			return items[i].RepoID < items[j].RepoID
		})
	}
}
