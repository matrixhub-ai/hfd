package repository

import (
	"strings"

	"github.com/go-git/go-git/v5/plumbing/format/gitattributes"
)

// GitAttributes represents parsed .gitattributes content and provides
// methods to check if a file path matches LFS filter patterns.
type GitAttributes struct {
	matcher gitattributes.Matcher
}

// IsLFS returns true if the given file path matches an LFS filter pattern
// defined in the .gitattributes file.
func (g *GitAttributes) IsLFS(filePath string) bool {
	if g == nil || g.matcher == nil {
		return false
	}
	path := strings.Split(filePath, "/")
	results, matched := g.matcher.Match(path, []string{"filter"})
	if !matched {
		return false
	}
	attr, ok := results["filter"]
	return ok && attr.IsValueSet() && attr.Value() == "lfs"
}

// GitAttributes reads and parses the .gitattributes file from the repository
// at the given revision. Returns nil (not an error) if the file does not exist.
func (r *Repository) GitAttributes(ref string) (*GitAttributes, error) {
	blob, err := r.Blob(ref, ".gitattributes")
	if err != nil {
		return nil, nil
	}
	reader, err := blob.NewReader()
	if err != nil {
		return nil, nil
	}
	defer reader.Close()

	attrs, err := gitattributes.ReadAttributes(reader, nil, true)
	if err != nil {
		return nil, nil
	}
	return &GitAttributes{matcher: gitattributes.NewMatcher(attrs)}, nil
}
