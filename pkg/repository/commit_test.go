package repository

import (
	"strings"
	"testing"
)

func TestCommitDiff(t *testing.T) {
	repo := initTestRepo(t)

	first := mustCommit(t, repo, "main", "",
		CommitOperation{Type: CommitOperationAdd, Path: "README.md", Content: []byte("# Test\n")})

	t.Run("root", func(t *testing.T) {
		commits, err := repo.Commits("main", nil)
		if err != nil {
			t.Fatalf("commits: %v", err)
		}
		if len(commits) != 1 {
			t.Fatalf("got %d commits, want 1", len(commits))
		}
		diff, err := commits[0].Diff()
		if err != nil {
			t.Fatalf("diff: %v", err)
		}
		for _, want := range []string{
			"diff --git a/README.md b/README.md",
			"new file mode 100644",
			"--- /dev/null",
			"+++ b/README.md",
			"+# Test",
		} {
			if !strings.Contains(diff, want) {
				t.Errorf("root diff missing %q:\n%s", want, diff)
			}
		}
	})

	mustCommit(t, repo, "main", first,
		CommitOperation{Type: CommitOperationAdd, Path: "README.md", Content: []byte("# Changed\n")})

	t.Run("parent", func(t *testing.T) {
		commits, err := repo.Commits("main", &CommitsOptions{Limit: 1})
		if err != nil {
			t.Fatalf("commits: %v", err)
		}
		if len(commits) != 1 {
			t.Fatalf("got %d commits, want 1", len(commits))
		}
		diff, err := commits[0].Diff()
		if err != nil {
			t.Fatalf("diff: %v", err)
		}
		for _, want := range []string{"-# Test", "+# Changed"} {
			if !strings.Contains(diff, want) {
				t.Errorf("parent diff missing %q:\n%s", want, diff)
			}
		}
		if strings.Contains(diff, "new file mode") {
			t.Errorf("parent diff reports a new file:\n%s", diff)
		}
	})
}
