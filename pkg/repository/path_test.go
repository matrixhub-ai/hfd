package repository

import "testing"

func TestResolvePath(t *testing.T) {
	tests := []struct {
		name    string
		urlPath string
		want    string
	}{
		{name: "empty", urlPath: "", want: ""},
		{name: "root", urlPath: "/", want: ""},
		{name: "repository", urlPath: "namespace/repo", want: "/namespace/repo.git"},
		{name: "leading slash", urlPath: "/namespace/repo", want: "/namespace/repo.git"},
		{name: "git suffix", urlPath: "namespace/repo.git", want: "/namespace/repo.git"},
		{name: "path traversal", urlPath: "namespace/../repo", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResolvePath(tt.urlPath); got != tt.want {
				t.Fatalf("ResolvePath(%q) = %q, want %q", tt.urlPath, got, tt.want)
			}
		})
	}
}
