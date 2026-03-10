package receive

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseRefUpdates(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []RefUpdate
		wantErr bool
	}{
		{
			name:  "single branch push",
			input: pktLine("abc123 def456 refs/heads/main\n") + "0000" + "PACKDATA",
			want: []RefUpdate{
				refUpdate{oldRev: "abc123", newRev: "def456", refName: "refs/heads/main"},
			},
		},
		{
			name:  "single branch push with capabilities",
			input: pktLine("abc123 def456 refs/heads/main\x00report-status side-band-64k\n") + "0000" + "PACKDATA",
			want: []RefUpdate{
				refUpdate{oldRev: "abc123", newRev: "def456", refName: "refs/heads/main"},
			},
		},
		{
			name: "multiple ref updates",
			input: pktLine("abc123 def456 refs/heads/main\x00report-status\n") +
				pktLine("111111 222222 refs/heads/feature\n") +
				pktLine(ZeroHash+" 333333 refs/tags/v1.0\n") +
				"0000" + "PACKDATA",
			want: []RefUpdate{
				refUpdate{oldRev: "abc123", newRev: "def456", refName: "refs/heads/main"},
				refUpdate{oldRev: "111111", newRev: "222222", refName: "refs/heads/feature"},
				refUpdate{oldRev: ZeroHash, newRev: "333333", refName: "refs/tags/v1.0"},
			},
		},
		{
			name:  "branch create (old is zeros)",
			input: pktLine(ZeroHash+" def456 refs/heads/new-branch\n") + "0000",
			want: []RefUpdate{
				refUpdate{oldRev: ZeroHash, newRev: "def456", refName: "refs/heads/new-branch"},
			},
		},
		{
			name:  "branch delete (new is zeros)",
			input: pktLine("abc123 "+ZeroHash+" refs/heads/old-branch\n") + "0000",
			want: []RefUpdate{
				refUpdate{oldRev: "abc123", newRev: ZeroHash, refName: "refs/heads/old-branch"},
			},
		},
		{
			name:  "empty input",
			input: "0000",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.input)
			got, newReader := ParseRefUpdates(r, "test-repo-path")

			if len(got) != len(tt.want) {
				t.Fatalf("ParseRefUpdates() got %d updates, want %d", len(got), len(tt.want))
			}

			for i, w := range tt.want {
				if got[i].OldRev() != w.OldRev() {
					t.Errorf("update[%d].OldRev = %q, want %q", i, got[i].OldRev(), w.OldRev())
				}
				if got[i].NewRev() != w.NewRev() {
					t.Errorf("update[%d].NewRev = %q, want %q", i, got[i].NewRev(), w.NewRev())
				}
				if got[i].RefName() != w.RefName() {
					t.Errorf("update[%d].RefName = %q, want %q", i, got[i].RefName(), w.RefName())
				}
			}

			// Verify that the new reader replays the full input
			all, err := io.ReadAll(newReader)
			if err != nil {
				t.Fatalf("reading new reader: %v", err)
			}
			if string(all) != tt.input {
				t.Errorf("new reader content = %q, want %q", string(all), tt.input)
			}
		})
	}
}

func TestRawRefUpdateMethods(t *testing.T) {
	tests := []struct {
		name     string
		update   RefUpdate
		isBranch bool
		isTag    bool
		isCreate bool
		isDelete bool
		wantName string
	}{
		{
			name:     "branch push",
			update:   refUpdate{oldRev: "abc123", newRev: "def456", refName: "refs/heads/main"},
			isBranch: true,
			wantName: "main",
		},
		{
			name:     "branch create",
			update:   refUpdate{oldRev: ZeroHash, newRev: "def456", refName: "refs/heads/new-branch"},
			isBranch: true,
			isCreate: true,
			wantName: "new-branch",
		},
		{
			name:     "branch delete",
			update:   refUpdate{oldRev: "abc123", newRev: ZeroHash, refName: "refs/heads/old-branch"},
			isBranch: true,
			isDelete: true,
			wantName: "old-branch",
		},
		{
			name:     "tag create",
			update:   refUpdate{oldRev: ZeroHash, newRev: "abc123", refName: "refs/tags/v1.0"},
			isTag:    true,
			isCreate: true,
			wantName: "v1.0",
		},
		{
			name:     "tag delete",
			update:   refUpdate{oldRev: "abc123", newRev: ZeroHash, refName: "refs/tags/v1.0"},
			isTag:    true,
			isDelete: true,
			wantName: "v1.0",
		},
		{
			name:     "unknown ref",
			update:   refUpdate{oldRev: "abc123", newRev: "def456", refName: "refs/notes/commits"},
			wantName: "refs/notes/commits",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.update.IsBranch(); got != tt.isBranch {
				t.Errorf("IsBranch() = %v, want %v", got, tt.isBranch)
			}
			if got := tt.update.IsTag(); got != tt.isTag {
				t.Errorf("IsTag() = %v, want %v", got, tt.isTag)
			}
			if got := tt.update.IsCreate(); got != tt.isCreate {
				t.Errorf("IsCreate() = %v, want %v", got, tt.isCreate)
			}
			if got := tt.update.IsDelete(); got != tt.isDelete {
				t.Errorf("IsDelete() = %v, want %v", got, tt.isDelete)
			}
			if got := tt.update.Name(); got != tt.wantName {
				t.Errorf("Name() = %q, want %q", got, tt.wantName)
			}
		})
	}
}

func TestIsForcePush(t *testing.T) {
	// Create a real git repo for force push detection
	repoDir, err := os.MkdirTemp("", "test-force-push")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(repoDir)

	// Initialize bare repo with explicit default branch
	gitInit := exec.CommandContext(t.Context(), "git", "init", "--bare", "--initial-branch=main", repoDir)
	if out, err := gitInit.CombinedOutput(); err != nil {
		t.Fatalf("git init --bare failed: %v\n%s", err, out)
	}

	// Create a working clone to make commits
	workDir, err := os.MkdirTemp("", "test-force-push-work")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(workDir)

	// Init a non-bare repo and add the bare repo as remote
	gitCloneInit := exec.CommandContext(t.Context(), "git", "init", "--initial-branch=main", workDir)
	if out, err := gitCloneInit.CombinedOutput(); err != nil {
		t.Fatalf("git init failed: %v\n%s", err, out)
	}

	gitRemote := exec.CommandContext(t.Context(), "git", "remote", "add", "origin", repoDir)
	gitRemote.Dir = workDir
	if out, err := gitRemote.CombinedOutput(); err != nil {
		t.Fatalf("git remote add failed: %v\n%s", err, out)
	}

	// Configure git
	for _, args := range [][]string{
		{"config", "user.email", "test@test.com"},
		{"config", "user.name", "Test User"},
	} {
		cmd := exec.CommandContext(t.Context(), "git", args...)
		cmd.Dir = workDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git config failed: %v\n%s", err, out)
		}
	}

	// Create first commit
	if err := os.WriteFile(filepath.Join(workDir, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"add", "."},
		{"commit", "-m", "commit1"},
		{"push", "-u", "origin", "main"},
	} {
		cmd := exec.CommandContext(t.Context(), "git", args...)
		cmd.Dir = workDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
		}
	}

	// Get commit1 hash
	cmd := exec.CommandContext(t.Context(), "git", "rev-parse", "HEAD")
	cmd.Dir = workDir
	commit1Out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git rev-parse failed: %v", err)
	}
	commit1 := strings.TrimSpace(string(commit1Out))

	// Create second commit (fast-forward)
	if err := os.WriteFile(filepath.Join(workDir, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"add", "."},
		{"commit", "-m", "commit2"},
		{"push", "origin", "main"},
	} {
		cmd := exec.CommandContext(t.Context(), "git", args...)
		cmd.Dir = workDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
		}
	}

	cmd = exec.CommandContext(t.Context(), "git", "rev-parse", "HEAD")
	cmd.Dir = workDir
	commit2Out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git rev-parse failed: %v", err)
	}
	commit2 := strings.TrimSpace(string(commit2Out))

	// Create a branch from commit1 with a divergent commit
	for _, args := range [][]string{
		{"checkout", "-b", "divergent", commit1},
	} {
		cmd := exec.CommandContext(t.Context(), "git", args...)
		cmd.Dir = workDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	if err := os.WriteFile(filepath.Join(workDir, "file3.txt"), []byte("divergent"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"add", "."},
		{"commit", "-m", "divergent commit"},
		{"push", "origin", "divergent"},
	} {
		cmd := exec.CommandContext(t.Context(), "git", args...)
		cmd.Dir = workDir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
		}
	}

	cmd = exec.CommandContext(t.Context(), "git", "rev-parse", "HEAD")
	cmd.Dir = workDir
	divergentOut, err := cmd.Output()
	if err != nil {
		t.Fatalf("git rev-parse failed: %v", err)
	}
	divergent := strings.TrimSpace(string(divergentOut))

	t.Run("fast-forward push", func(t *testing.T) {
		update := refUpdate{oldRev: commit1, newRev: commit2, refName: "refs/heads/main"}
		if ok, err := isForce(t.Context(), repoDir, update); err != nil {
			t.Errorf("unexpected error: %v", err)
		} else if ok {
			t.Error("expected IsForcePush=false for fast-forward push")
		}
	})

	t.Run("force push", func(t *testing.T) {
		// divergent is NOT a descendant of commit2, so commit2->divergent is a force push
		update := refUpdate{oldRev: commit2, newRev: divergent, refName: "refs/heads/main"}
		if ok, err := isForce(t.Context(), repoDir, update); err != nil {
			t.Errorf("unexpected error: %v", err)
		} else if !ok {
			t.Error("expected IsForcePush=true for non-fast-forward push")
		}
	})

	t.Run("create is not force push", func(t *testing.T) {
		update := refUpdate{oldRev: ZeroHash, newRev: commit1, refName: "refs/heads/new-branch"}
		if ok, err := isForce(t.Context(), repoDir, update); err != nil {
			t.Errorf("unexpected error: %v", err)
		} else if ok {
			t.Error("expected IsForcePush=false for create")
		}
	})

	t.Run("delete is not force push", func(t *testing.T) {
		update := refUpdate{oldRev: commit1, newRev: ZeroHash, refName: "refs/heads/old-branch"}
		if ok, err := isForce(t.Context(), repoDir, update); err != nil {
			t.Errorf("unexpected error: %v", err)
		} else if ok {
			t.Error("expected IsForcePush=false for delete")
		}
	})

	t.Run("tag is not force push", func(t *testing.T) {
		update := refUpdate{oldRev: commit1, newRev: commit2, refName: "refs/tags/v1.0"}
		if ok, err := isForce(t.Context(), repoDir, update); err != nil {
			t.Errorf("unexpected error: %v", err)
		} else if ok {
			t.Error("expected IsForcePush=false for tag")
		}
	})
}

func TestRefUpdate_String(t *testing.T) {
	tests := []struct {
		update RefUpdate
		want   string
	}{
		{
			update: refUpdate{oldRev: "abc123", newRev: "def456", refName: "refs/heads/main"},
			want:   "branch_push:main",
		},
		{
			update: refUpdate{oldRev: ZeroHash, newRev: "def456", refName: "refs/heads/feature"},
			want:   "branch_create:feature",
		},
		{
			update: refUpdate{oldRev: "abc123", newRev: ZeroHash, refName: "refs/heads/old"},
			want:   "branch_delete:old",
		},
		{
			update: refUpdate{oldRev: ZeroHash, newRev: "abc123", refName: "refs/tags/v1.0"},
			want:   "tag_create:v1.0",
		},
		{
			update: refUpdate{oldRev: "abc123", newRev: ZeroHash, refName: "refs/tags/v1.0"},
			want:   "tag_delete:v1.0",
		},
		{
			update: refUpdate{oldRev: "abc123", newRev: "def456", refName: "refs/notes/commits"},
			want:   "ref_update:refs/notes/commits",
		},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.update.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// pktLine helper for tests - formats a string as a git pkt-line
func pktLine(s string) string {
	return fmt.Sprintf("%04x%s", len(s)+4, s)
}

func TestParseRefUpdatesReaderReplay(t *testing.T) {
	// Verify that the returned reader can replay all the original content
	input := pktLine("abc123 def456 refs/heads/main\x00report-status\n") +
		pktLine(ZeroHash+" 333333 refs/tags/v1.0\n") +
		"0000" +
		"PACKbinarydata\x00\x01\x02\x03"

	updates, newReader := ParseRefUpdates(strings.NewReader(input), "test-repo-path")

	if len(updates) != 2 {
		t.Fatalf("expected 2 updates, got %d", len(updates))
	}

	all, err := io.ReadAll(newReader)
	if err != nil {
		t.Fatalf("reading new reader: %v", err)
	}

	if !bytes.Equal([]byte(input), all) {
		t.Errorf("replayed content does not match original\ngot:  %q\nwant: %q", string(all), input)
	}
}

func TestDiffRefs(t *testing.T) {
	tests := []struct {
		name   string
		before map[string]string
		after  map[string]string
		want   map[string]string // refName -> expected FormatEvent output
	}{
		{
			name:   "new repo (all creates)",
			before: nil,
			after: map[string]string{
				"refs/heads/main":    "abc123",
				"refs/heads/develop": "def456",
				"refs/tags/v1.0":     "111111",
			},
			want: map[string]string{
				"refs/heads/main":    "branch_create:main",
				"refs/heads/develop": "branch_create:develop",
				"refs/tags/v1.0":     "tag_create:v1.0",
			},
		},
		{
			name: "no changes",
			before: map[string]string{
				"refs/heads/main": "abc123",
				"refs/tags/v1.0":  "111111",
			},
			after: map[string]string{
				"refs/heads/main": "abc123",
				"refs/tags/v1.0":  "111111",
			},
			want: map[string]string{},
		},
		{
			name: "branch deleted",
			before: map[string]string{
				"refs/heads/main":    "abc123",
				"refs/heads/feature": "def456",
			},
			after: map[string]string{
				"refs/heads/main": "abc123",
			},
			want: map[string]string{
				"refs/heads/feature": "branch_delete:feature",
			},
		},
		{
			name: "tag deleted",
			before: map[string]string{
				"refs/tags/v1.0": "abc123",
				"refs/tags/v2.0": "def456",
			},
			after: map[string]string{
				"refs/tags/v2.0": "def456",
			},
			want: map[string]string{
				"refs/tags/v1.0": "tag_delete:v1.0",
			},
		},
		{
			name: "branch updated",
			before: map[string]string{
				"refs/heads/main": "abc123",
			},
			after: map[string]string{
				"refs/heads/main": "def456",
			},
			want: map[string]string{
				"refs/heads/main": "branch_push:main",
			},
		},
		{
			name: "mixed: create, delete, update",
			before: map[string]string{
				"refs/heads/main":      "abc123",
				"refs/heads/to-delete": "111111",
				"refs/tags/old-tag":    "222222",
			},
			after: map[string]string{
				"refs/heads/main":       "def456",
				"refs/heads/new-branch": "333333",
				"refs/tags/new-tag":     "444444",
			},
			want: map[string]string{
				"refs/heads/main":       "branch_push:main",
				"refs/heads/new-branch": "branch_create:new-branch",
				"refs/tags/new-tag":     "tag_create:new-tag",
				"refs/heads/to-delete":  "branch_delete:to-delete",
				"refs/tags/old-tag":     "tag_delete:old-tag",
			},
		},
		{
			name:   "empty before and after",
			before: map[string]string{},
			after:  map[string]string{},
			want:   map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updates := DiffRefs(tt.before, tt.after, "test-repo-path")

			// Build a map of updates for easier comparison
			got := make(map[string]string)
			for _, u := range updates {
				got[u.RefName()] = u.String()
			}

			if len(got) != len(tt.want) {
				t.Fatalf("DiffRefs() got %d events, want %d\ngot: %v\nwant: %v", len(got), len(tt.want), got, tt.want)
			}

			for refName, wantEvent := range tt.want {
				gotEvent, ok := got[refName]
				if !ok {
					t.Errorf("missing event for ref %q, expected %q", refName, wantEvent)
					continue
				}
				if gotEvent != wantEvent {
					t.Errorf("event for ref %q = %q, want %q", refName, gotEvent, wantEvent)
				}
			}
		})
	}
}
