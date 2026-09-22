package hf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"path"
	"slices"
	"strings"

	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

const (
	maxPathsInfoBody  = 1 << 20
	maxPathsInfoPaths = 1000
)

type pathsInfoRequest struct {
	Paths  []string `json:"paths"`
	Expand bool     `json:"expand"`
}

// The web client posts JSON; huggingface_hub posts a form body.
func parsePathsInfo(w http.ResponseWriter, r *http.Request) (pathsInfoRequest, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxPathsInfoBody)
	var req pathsInfoRequest
	switch ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); ct {
	case "application/x-www-form-urlencoded", "multipart/form-data":
		var err error
		if ct == "multipart/form-data" {
			err = r.ParseMultipartForm(maxPathsInfoBody)
		} else {
			err = r.ParseForm()
		}
		if err == nil {
			// The multipart parser stops at the closing boundary, so the cap only applies to the epilogue if it is read too.
			_, err = io.Copy(io.Discard, r.Body)
		}
		if err != nil {
			return req, fmt.Errorf("invalid request body: %v", err)
		}
		req.Paths = r.PostForm["paths"]
		expand := r.PostForm.Get("expand")
		req.Expand = strings.EqualFold(expand, "true") || expand == "1"
	default:
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil {
			return req, fmt.Errorf("invalid request body: %v", err)
		}
		// Reading to EOF rejects trailing data and lets MaxBytesReader trip on oversized bodies.
		if _, err := dec.Token(); !errors.Is(err, io.EOF) {
			if err == nil {
				err = errors.New("trailing data after the JSON value")
			}
			return req, fmt.Errorf("invalid request body: %v", err)
		}
	}
	if len(req.Paths) > maxPathsInfoPaths {
		return req, fmt.Errorf("too many paths: %d > %d", len(req.Paths), maxPathsInfoPaths)
	}
	var paths []string
	for _, p := range req.Paths {
		clean := path.Clean(p)
		if clean == "." || clean == ".." || strings.HasPrefix(clean, "/") || strings.HasPrefix(clean, "../") {
			return req, fmt.Errorf("invalid path %q", p)
		}
		if !slices.Contains(paths, clean) {
			paths = append(paths, clean)
		}
	}
	req.Paths = paths
	return req, nil
}

// parentDir is path.Dir with "" for the root, the key treeAt uses for it.
func parentDir(p string) string {
	if parent := path.Dir(p); parent != "." {
		return parent
	}
	return ""
}

// 404 for unknown, invalid or exhausted revisions (main~999 runs out of parents as io.EOF).
func revisionStatus(err error) int {
	// go-git's invalid-revision error type is internal, so its message prefix is the only handle.
	if errors.Is(err, repository.ErrRevisionNotFound) || errors.Is(err, io.EOF) || strings.HasPrefix(err.Error(), "Revision invalid") {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

// handlePathsInfo handles POST /api/{repoType}/{namespace}/{repo}/paths-info/{rev}
// Each parent directory is listed once; paths that do not exist are omitted, not errors.
func (h *Handler) handlePathsInfo(w http.ResponseWriter, r *http.Request) {
	ri := getRepoInformation(r)
	rev := mux.Vars(r)["rev"]

	if !h.checkPermission(w, r, permission.OperationReadRepo, ri.RepoName, permission.Context{Ref: rev}) {
		return
	}
	// The body is validated before the pre-open hook so malformed requests never pull a mirror.
	req, err := parsePathsInfo(w, r)
	if err != nil {
		responseJSON(w, err, http.StatusBadRequest)
		return
	}
	repo, ok := h.openRepoChecked(w, r, ri.RepoName, false)
	if !ok {
		return
	}
	hash, err := repo.ResolveRevision(rev)
	if err != nil {
		responseJSON(w, fmt.Errorf("failed to resolve revision %q in repository %q: %v", rev, ri.RepoName, err), revisionStatus(err))
		return
	}

	wanted := map[string][]string{}
	for _, p := range req.Paths {
		parent := parentDir(p)
		wanted[parent] = append(wanted[parent], p)
	}
	found := map[string]treeEntry{}
	listed := map[string][]*repository.TreeEntry{}
	for _, parent := range slices.Sorted(maps.Keys(wanted)) {
		entries, err := treeAt(repo, hash, parent, listed)
		if err != nil {
			responseJSON(w, fmt.Errorf("failed to read %q at %q in repository %q: %v", parent, rev, ri.RepoName, err), http.StatusInternalServerError)
			return
		}
		for _, e := range entries {
			if !slices.Contains(wanted[parent], e.Path()) {
				continue
			}
			info, err := describePath(r.Context(), repo, hash, e, req.Expand)
			if err != nil {
				responseJSON(w, fmt.Errorf("failed to read %q at %q in repository %q: %v", e.Path(), rev, ri.RepoName, err), http.StatusInternalServerError)
				return
			}
			found[e.Path()] = info
		}
	}
	out := make([]treeEntry, 0, len(found))
	for _, p := range req.Paths {
		if info, ok := found[p]; ok {
			out = append(out, info)
		}
	}
	responseJSON(w, out, http.StatusOK)
}

// Descends from the root so a missing or file component yields no entries rather than an error.
func treeAt(repo *repository.Repository, hash, dir string, listed map[string][]*repository.TreeEntry) ([]*repository.TreeEntry, error) {
	if entries, ok := listed[dir]; ok {
		return entries, nil
	}
	if dir != "" {
		siblings, err := treeAt(repo, hash, parentDir(dir), listed)
		if err != nil {
			return nil, err
		}
		if i := slices.IndexFunc(siblings, func(e *repository.TreeEntry) bool { return e.Path() == dir }); i < 0 || siblings[i].Type() != repository.EntryTypeDirectory {
			listed[dir] = nil
			return nil, nil
		}
	}
	entries, err := repo.Tree(hash, dir, nil)
	if err != nil {
		return nil, err
	}
	listed[dir] = entries
	return entries, nil
}

func describePath(ctx context.Context, repo *repository.Repository, hash string, e *repository.TreeEntry, expand bool) (treeEntry, error) {
	info := treeEntry{OID: e.Hash().String(), Path: e.Path(), Type: e.Type()}
	last := e.LastCommit()
	if e.Type() == repository.EntryTypeFile {
		blob, err := e.Blob()
		if err != nil {
			return info, err
		}
		info.Size = blob.Size()
		if ptr, _ := blob.LFSPointer(); ptr != nil {
			info.LFS = &lfsPointer{OID: ptr.OID(), Size: ptr.Size(), PointerSize: blob.Size()}
			info.Size = ptr.Size()
		}
	} else if expand {
		var err error
		if last, err = directoryLastCommit(ctx, repo, hash, e.Path()); err != nil {
			return info, err
		}
	}
	if expand && last != nil {
		info.LastCommit = &treeLastCommit{ID: last.Hash().String(), Title: last.Title(), Date: last.Author().When().UTC().Format(repository.TimeFormat)}
	}
	return info, nil
}

// The per-entry LastCommit path filter only matches files, so directories walk the history themselves.
func directoryLastCommit(ctx context.Context, repo *repository.Repository, hash, dir string) (*repository.Commit, error) {
	all, err := repo.Commits(hash, nil)
	if err != nil {
		return nil, err
	}
	for i := range all {
		c := &all[i]
		changes, err := repo.Compare(ctx, c.Hash().String()+"^", c.Hash().String())
		if errors.Is(err, io.EOF) {
			// A root commit has no parent to diff against; whatever it holds under dir was added here.
			return c, nil
		}
		if err != nil {
			return nil, err
		}
		if slices.ContainsFunc(changes, func(ch *object.Change) bool {
			return strings.HasPrefix(ch.From.Name, dir+"/") || strings.HasPrefix(ch.To.Name, dir+"/")
		}) {
			return c, nil
		}
	}
	return nil, fmt.Errorf("history of %q reaches no root commit", dir)
}
