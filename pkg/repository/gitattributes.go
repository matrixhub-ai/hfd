package repository

import (
	_ "embed"
	"fmt"
	"io"

	"github.com/git-lfs/git-lfs/v3/git/gitattr"
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
	lines []gitattr.PatternLine
}

// IsLFS returns true if the given file path matches an LFS filter pattern
// defined in the .gitattributes file.
func (g *GitAttributes) IsLFS(filePath string) bool {
	if g == nil {
		return false
	}
	var lfs bool
	// Like git, matching lines apply in file order and the last filter assignment wins.
	for _, line := range g.lines {
		if !line.Pattern().Match(filePath) {
			continue
		}
		for _, attr := range line.Attrs() {
			if attr.K == "filter" {
				lfs = attr.V == "lfs"
			}
		}
	}
	return lfs
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

	ga, err := parseGitAttributesReader(reader)
	if err != nil {
		return nil, nil
	}
	return ga, nil
}

func parseGitAttributesReader(r io.Reader) (ga *GitAttributes, err error) {
	// wildmatch panics on malformed patterns such as an unclosed "[:class".
	defer func() {
		if p := recover(); p != nil {
			ga, err = nil, fmt.Errorf("parse %s: %v", GitattributesFileName, p)
		}
	}()
	lines, _, err := gitattr.ParseLines(r)
	if err != nil {
		return nil, err
	}
	return &GitAttributes{lines: gitattr.NewMacroProcessor().ProcessLines(lines, true)}, nil
}
