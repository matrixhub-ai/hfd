package gitd

import (
	"net/http"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/gorilla/mux"
)

func (h *Handler) registryManagement(r *mux.Router) {
	r.HandleFunc("/apis/repositories/{repo:.+}", h.requireAuth(h.handleCreateRepository)).Methods(http.MethodPost)
	r.HandleFunc("/apis/repositories/{repo:.+}", h.requireAuth(h.handleDeleteRepository)).Methods(http.MethodDelete)
}

func (h *Handler) handleCreateRepository(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoName := vars["repo"]

	repoPath := h.resolveRepoPath(repoName)
	if repoPath != "" {
		http.Error(w, "Repository already exists", http.StatusConflict)
		return
	}

	repoPath = filepath.Join(h.rootDir, repoName)

	base, dir := filepath.Split(repoPath)
	if err := os.MkdirAll(base, 0755); err != nil {
		http.Error(w, "Failed to create repository", http.StatusInternalServerError)
		return
	}

	cmd := exec.CommandContext(r.Context(),
		"git",
		"init",
		"--bare",
		dir)
	cmd.Dir = base
	cmd.Stdout = w
	err := cmd.Run()
	if err != nil {
		http.Error(w, "Failed to create repository", http.StatusInternalServerError)
		return
	}
}

func (h *Handler) handleDeleteRepository(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repoName := vars["repo"]

	repoPath := h.resolveRepoPath(repoName)
	if repoPath == "" {
		http.NotFound(w, r)
		return
	}

	err := os.RemoveAll(repoPath)
	if err != nil {
		http.Error(w, "Failed to delete repository", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
