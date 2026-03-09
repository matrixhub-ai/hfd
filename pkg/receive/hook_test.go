package receive

import (
	"strings"
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
		{"refs/other/foo", false},
	}
	for _, tt := range tests {
		r := RefUpdate{RefName: tt.refName}
		if got := r.IsBranch(); got != tt.want {
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
		{"refs/tags/release/1", true},
		{"refs/heads/main", false},
		{"refs/other/foo", false},
	}
	for _, tt := range tests {
		r := RefUpdate{RefName: tt.refName}
		if got := r.IsTag(); got != tt.want {
			t.Errorf("RefUpdate{RefName: %q}.IsTag() = %v, want %v", tt.refName, got, tt.want)
		}
	}
}

func TestRefUpdateIsCreate(t *testing.T) {
	r := RefUpdate{OldRev: ZeroHash, NewRev: "abc123"}
	if !r.IsCreate() {
		t.Error("expected IsCreate to be true for zero old rev")
	}
	r2 := RefUpdate{OldRev: "abc123", NewRev: "def456"}
	if r2.IsCreate() {
		t.Error("expected IsCreate to be false for non-zero old rev")
	}
}

func TestRefUpdateIsDelete(t *testing.T) {
	r := RefUpdate{OldRev: "abc123", NewRev: ZeroHash}
	if !r.IsDelete() {
		t.Error("expected IsDelete to be true for zero new rev")
	}
	r2 := RefUpdate{OldRev: "abc123", NewRev: "def456"}
	if r2.IsDelete() {
		t.Error("expected IsDelete to be false for non-zero new rev")
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
		{"refs/tags/release/1", "release/1"},
		{"refs/other/foo", "refs/other/foo"},
	}
	for _, tt := range tests {
		r := RefUpdate{RefName: tt.refName}
		if got := r.Name(); got != tt.want {
			t.Errorf("RefUpdate{RefName: %q}.Name() = %q, want %q", tt.refName, got, tt.want)
		}
	}
}

func TestParseRefUpdates(t *testing.T) {
	input := "abc123 def456 refs/heads/main\n" +
		"000000 aaa111 refs/tags/v1.0\n" +
		"bbb222 000000 refs/heads/old-branch\n"

	updates, err := ParseRefUpdates(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseRefUpdates() error = %v", err)
	}

	if len(updates) != 3 {
		t.Fatalf("ParseRefUpdates() returned %d updates, want 3", len(updates))
	}

	expected := []RefUpdate{
		{OldRev: "abc123", NewRev: "def456", RefName: "refs/heads/main"},
		{OldRev: "000000", NewRev: "aaa111", RefName: "refs/tags/v1.0"},
		{OldRev: "bbb222", NewRev: "000000", RefName: "refs/heads/old-branch"},
	}

	for i, want := range expected {
		got := updates[i]
		if got.OldRev != want.OldRev || got.NewRev != want.NewRev || got.RefName != want.RefName {
			t.Errorf("update[%d] = %+v, want %+v", i, got, want)
		}
	}
}

func TestParseRefUpdatesEmpty(t *testing.T) {
	updates, err := ParseRefUpdates(strings.NewReader(""))
	if err != nil {
		t.Fatalf("ParseRefUpdates() error = %v", err)
	}
	if len(updates) != 0 {
		t.Errorf("ParseRefUpdates() returned %d updates, want 0", len(updates))
	}
}

func TestParseRefUpdatesSkipsBlankLines(t *testing.T) {
	input := "abc123 def456 refs/heads/main\n\naaa111 bbb222 refs/tags/v1.0\n"
	updates, err := ParseRefUpdates(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseRefUpdates() error = %v", err)
	}
	if len(updates) != 2 {
		t.Errorf("ParseRefUpdates() returned %d updates, want 2", len(updates))
	}
}

func TestDiffRefs(t *testing.T) {
	before := map[string]string{
		"refs/heads/main":       "aaa111",
		"refs/heads/old-branch": "bbb222",
		"refs/tags/v1.0":        "ccc333",
	}
	after := map[string]string{
		"refs/heads/main":       "ddd444", // updated
		"refs/tags/v1.0":        "ccc333", // unchanged
		"refs/heads/new-branch": "eee555", // created
		// old-branch deleted
	}

	updates := DiffRefs(before, after)

	// We should have 3 updates: main updated, new-branch created, old-branch deleted
	if len(updates) != 3 {
		t.Fatalf("DiffRefs() returned %d updates, want 3", len(updates))
	}

	// Build a map for easier lookup
	updateMap := map[string]RefUpdate{}
	for _, u := range updates {
		updateMap[u.RefName] = u
	}

	// main should be updated
	if u, ok := updateMap["refs/heads/main"]; !ok {
		t.Error("expected update for refs/heads/main")
	} else {
		if u.OldRev != "aaa111" || u.NewRev != "ddd444" {
			t.Errorf("refs/heads/main update = %+v, want old=aaa111 new=ddd444", u)
		}
	}

	// new-branch should be created
	if u, ok := updateMap["refs/heads/new-branch"]; !ok {
		t.Error("expected update for refs/heads/new-branch")
	} else {
		if u.OldRev != ZeroHash || u.NewRev != "eee555" {
			t.Errorf("refs/heads/new-branch update = %+v, want old=%s new=eee555", u, ZeroHash)
		}
	}

	// old-branch should be deleted
	if u, ok := updateMap["refs/heads/old-branch"]; !ok {
		t.Error("expected update for refs/heads/old-branch")
	} else {
		if u.OldRev != "bbb222" || u.NewRev != ZeroHash {
			t.Errorf("refs/heads/old-branch update = %+v, want old=bbb222 new=%s", u, ZeroHash)
		}
	}
}

func TestDiffRefsEmpty(t *testing.T) {
	updates := DiffRefs(map[string]string{}, map[string]string{})
	if len(updates) != 0 {
		t.Errorf("DiffRefs() returned %d updates, want 0", len(updates))
	}
}

func TestDiffRefsNoChanges(t *testing.T) {
	refs := map[string]string{
		"refs/heads/main": "abc123",
	}
	updates := DiffRefs(refs, refs)
	if len(updates) != 0 {
		t.Errorf("DiffRefs() returned %d updates for identical refs, want 0", len(updates))
	}
}

func TestFormatEvent(t *testing.T) {
	tests := []struct {
		update RefUpdate
		want   string
	}{
		{
			RefUpdate{OldRev: ZeroHash, NewRev: "abc123", RefName: "refs/heads/feature"},
			"branch_create:feature",
		},
		{
			RefUpdate{OldRev: "abc123", NewRev: ZeroHash, RefName: "refs/heads/old"},
			"branch_delete:old",
		},
		{
			RefUpdate{OldRev: "abc123", NewRev: "def456", RefName: "refs/heads/main"},
			"branch_push:main",
		},
		{
			RefUpdate{OldRev: ZeroHash, NewRev: "abc123", RefName: "refs/tags/v1.0"},
			"tag_create:v1.0",
		},
		{
			RefUpdate{OldRev: "abc123", NewRev: ZeroHash, RefName: "refs/tags/v1.0"},
			"tag_delete:v1.0",
		},
		{
			RefUpdate{OldRev: "abc123", NewRev: "def456", RefName: "refs/other/foo"},
			"ref_update:refs/other/foo",
		},
	}
	for _, tt := range tests {
		if got := FormatEvent(tt.update); got != tt.want {
			t.Errorf("FormatEvent(%+v) = %q, want %q", tt.update, got, tt.want)
		}
	}
}
