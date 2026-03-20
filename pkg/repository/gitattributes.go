package repository

import (
	_ "embed"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/format/gitattributes"
	"github.com/matrixhub-ai/hfd/internal/lru"
)

// GitattributesFileName is the name of the .gitattributes file in the repository.
const GitattributesFileName = ".gitattributes"

// GitattributesText is the content of a default .gitattributes file that marks common large/binary
// file types to be tracked with Git LFS.
//
//go:embed gitattributes.txt
var GitattributesText []byte

// GitAttributes represents parsed .gitattributes content and provides
// methods to check if a file path matches LFS filter patterns.
type GitAttributes struct {
	matcher gitattributes.Matcher
}

// IsLFS returns true if the given file path matches an LFS filter pattern
// defined in the .gitattributes file.
func (g *GitAttributes) IsLFS(filePath string) bool {
	return g.filterIs(filePath, "lfs")
}

// IsXet returns true if the given file path matches a xet filter pattern
// defined in the .gitattributes file.
func (g *GitAttributes) IsXet(filePath string) bool {
	return g.filterIs(filePath, "xet")
}

// filterIs returns true if the given file path has a filter attribute
// matching the specified value in the .gitattributes file.
func (g *GitAttributes) filterIs(filePath string, filter string) bool {
	if g == nil || g.matcher == nil {
		return false
	}
	path := strings.Split(filePath, "/")
	results, matched := g.matcher.Match(path, []string{"filter"})
	if !matched {
		return false
	}
	attr, ok := results["filter"]
	return ok && attr.IsValueSet() && attr.Value() == filter
}

var lruGitattributesCache = lru.New[Hash, *GitAttributes](128)

// GitAttributes reads and parses the .gitattributes file from the repository
// at the given revision. Returns nil (not an error) if the file does not exist.
func (r *Repository) GitAttributes(rev string) (*GitAttributes, error) {
	blob, err := r.Blob(rev, GitattributesFileName)
	if err != nil {
		return nil, nil
	}

	ga, _ := lruGitattributesCache.GetOrNew(blob.Hash(), func() (*GitAttributes, bool) {
		parsed, parseErr := parseGitAttributes(blob)
		err = parseErr
		return parsed, parseErr == nil
	})
	return ga, err
}

func parseGitAttributes(blob *Blob) (*GitAttributes, error) {
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
