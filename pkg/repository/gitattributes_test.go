package repository

import (
	"strings"
	"testing"
)

func TestGitAttributesIsLFS(t *testing.T) {
	defaults := string(GitattributesText)
	tests := []struct {
		name    string
		content string
		path    string
		want    bool
	}{
		{"default bin", defaults, "model.bin", true},
		{"default bin in dir", defaults, "dir/model.bin", true},
		{"default lfs infix", defaults, "x.lfs.txt", true},
		{"default tfevents", defaults, "events.out.tfevents.1", true},
		{"default saved_model doublestar", defaults, "saved_model/variables/variables.index", true},
		{"default readme", defaults, "README.md", false},
		{"trailing text=auto keeps filter", "*.bin filter=lfs diff=lfs merge=lfs -text\n* text=auto\n", "model.bin", true},
		{"later -filter unsets", "*.bin filter=lfs\n*.bin -filter\n", "model.bin", false},
		{"later filter=lfs sets", "*.bin -filter\n*.bin filter=lfs\n", "model.bin", true},
		{"later !filter unspecifies", "*.bin filter=lfs\n*.bin !filter\n", "model.bin", false},
		{"macro expansion", "[attr]lfs filter=lfs diff=lfs merge=lfs -text\n*.bin lfs\n", "model.bin", true},
		{"macro defined after use", "*.bin lfs\n[attr]lfs filter=lfs diff=lfs merge=lfs -text\n", "model.bin", true},
		{"same line macro then -filter", "[attr]lfs filter=lfs diff=lfs\n*.bin lfs -filter\n", "model.bin", false},
		{"same line -filter then macro", "[attr]lfs filter=lfs diff=lfs\n*.bin -filter lfs\n", "model.bin", true},
		{"same line later filter wins", "*.bin filter=lfs filter=other\n", "model.bin", false},
		{"tab separated", "*.bin\tfilter=lfs\tdiff=lfs\n", "model.bin", true},
		{"quoted pattern then tab", "\"my model.bin\"\tfilter=lfs\n", "my model.bin", true},
		{"root anchored matches root", "/model.bin filter=lfs\n", "model.bin", true},
		{"root anchored skips subdir", "/model.bin filter=lfs\n", "sub/model.bin", false},
		{"other filter", "*.txt filter=other\n", "a.txt", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ga, err := parseGitAttributesReader(strings.NewReader(tt.content))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := ga.IsLFS(tt.path); got != tt.want {
				t.Errorf("IsLFS(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestGitAttributesMalformedPattern(t *testing.T) {
	for _, content := range []string{
		"model.[bin filter=lfs\n",
		"model.[[:alpha filter=lfs\n",
		"model.[a- filter=lfs\n",
	} {
		ga, _ := parseGitAttributesReader(strings.NewReader(content))
		if ga.IsLFS("model.bin") {
			t.Errorf("%q: IsLFS(model.bin) = true, want false", content)
		}
	}
}
