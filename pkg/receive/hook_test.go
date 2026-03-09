package receive

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRefUpdateIsBranch(t *testing.T) {
	tests := []struct {
		refName string
		want    bool
	}{
		{"refs/heads/main", true},
		{"refs/heads/feature/x", true},
		{"refs/tags/v1.0", false},
		{"refs/notes/commits", false},
	}
	for _, tt := range tests {
		u := RefUpdate{RefName: tt.refName}
		if got := u.IsBranch(); got != tt.want {
			t.Errorf("RefUpdate{RefName: %q}.IsBranch() = %v, want %v", tt.refName, got, tt.want)
		}
	}
}

func TestRefUpdateIsTag(t *testing.T) {
	tests := []struct {
		refName string
		want    bool
	}{
		{"refs/tags/v1.0", true},
		{"refs/tags/release/1.0", true},
		{"refs/heads/main", false},
	}
	for _, tt := range tests {
		u := RefUpdate{RefName: tt.refName}
		if got := u.IsTag(); got != tt.want {
			t.Errorf("RefUpdate{RefName: %q}.IsTag() = %v, want %v", tt.refName, got, tt.want)
		}
	}
}

func TestRefUpdateIsCreate(t *testing.T) {
	tests := []struct {
		oldRev string
		want   bool
	}{
		{zeroPad, true},
		{"abc123", false},
	}
	for _, tt := range tests {
		u := RefUpdate{OldRev: tt.oldRev}
		if got := u.IsCreate(); got != tt.want {
			t.Errorf("RefUpdate{OldRev: %q}.IsCreate() = %v, want %v", tt.oldRev, got, tt.want)
		}
	}
}

func TestRefUpdateIsDelete(t *testing.T) {
	tests := []struct {
		newRev string
		want   bool
	}{
		{zeroPad, true},
		{"abc123", false},
	}
	for _, tt := range tests {
		u := RefUpdate{NewRev: tt.newRev}
		if got := u.IsDelete(); got != tt.want {
			t.Errorf("RefUpdate{NewRev: %q}.IsDelete() = %v, want %v", tt.newRev, got, tt.want)
		}
	}
}

func TestRefUpdateName(t *testing.T) {
	tests := []struct {
		refName string
		want    string
	}{
		{"refs/heads/main", "main"},
		{"refs/heads/feature/x", "feature/x"},
		{"refs/tags/v1.0", "v1.0"},
		{"refs/notes/commits", "refs/notes/commits"},
	}
	for _, tt := range tests {
		u := RefUpdate{RefName: tt.refName}
		if got := u.Name(); got != tt.want {
			t.Errorf("RefUpdate{RefName: %q}.Name() = %q, want %q", tt.refName, got, tt.want)
		}
	}
}

func TestInstallHooks(t *testing.T) {
	dir := t.TempDir()
	if err := InstallHooks(dir); err != nil {
		t.Fatalf("InstallHooks failed: %v", err)
	}

	hookPath := filepath.Join(dir, "hooks", "post-receive")
	info, err := os.Stat(hookPath)
	if err != nil {
		t.Fatalf("post-receive hook not found: %v", err)
	}
	if info.Mode()&0111 == 0 {
		t.Errorf("post-receive hook is not executable: %v", info.Mode())
	}

	data, err := os.ReadFile(hookPath)
	if err != nil {
		t.Fatalf("Failed to read hook: %v", err)
	}
	if string(data) != postReceiveScript {
		t.Errorf("Hook content mismatch")
	}
}

func TestParseHookOutput(t *testing.T) {
	dir := t.TempDir()

	t.Run("FileNotExist", func(t *testing.T) {
		updates, err := ParseHookOutput(filepath.Join(dir, "nonexistent"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if updates != nil {
			t.Errorf("expected nil updates, got %v", updates)
		}
	})

	t.Run("EmptyFile", func(t *testing.T) {
		path := filepath.Join(dir, "empty")
		if err := os.WriteFile(path, []byte(""), 0644); err != nil {
			t.Fatal(err)
		}
		updates, err := ParseHookOutput(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(updates) != 0 {
			t.Errorf("expected 0 updates, got %d", len(updates))
		}
	})

	t.Run("SingleUpdate", func(t *testing.T) {
		path := filepath.Join(dir, "single")
		content := "0000000000000000000000000000000000000000 abc123def456abc123def456abc123def456abc1 refs/heads/main\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		updates, err := ParseHookOutput(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(updates) != 1 {
			t.Fatalf("expected 1 update, got %d", len(updates))
		}
		if !updates[0].IsCreate() {
			t.Error("expected create")
		}
		if !updates[0].IsBranch() {
			t.Error("expected branch")
		}
		if updates[0].Name() != "main" {
			t.Errorf("expected name 'main', got %q", updates[0].Name())
		}
	})

	t.Run("MultipleUpdates", func(t *testing.T) {
		path := filepath.Join(dir, "multi")
		content := "aaa111 bbb222 refs/heads/main\nccc333 ddd444 refs/tags/v1.0\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		updates, err := ParseHookOutput(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(updates) != 2 {
			t.Fatalf("expected 2 updates, got %d", len(updates))
		}
		if updates[0].RefName != "refs/heads/main" {
			t.Errorf("expected refs/heads/main, got %q", updates[0].RefName)
		}
		if updates[1].RefName != "refs/tags/v1.0" {
			t.Errorf("expected refs/tags/v1.0, got %q", updates[1].RefName)
		}
	})

	t.Run("MalformedLine", func(t *testing.T) {
		path := filepath.Join(dir, "malformed")
		content := "only-two-fields here\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		_, err := ParseHookOutput(path)
		if err == nil {
			t.Error("expected error for malformed line")
		}
	})
}
