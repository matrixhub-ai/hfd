package repository

import (
	_ "embed"
	"fmt"
	"io"
	"strings"

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
	lines  []gitattr.PatternLine
	macros map[string][]*gitattr.Attr
}

// IsLFS returns true if the given file path matches an LFS filter pattern
// defined in the .gitattributes file.
func (g *GitAttributes) IsLFS(filePath string) bool {
	if g == nil {
		return false
	}
	// Like git's fill_one: later lines and later attributes win, each attribute is assigned once.
	known := map[string]bool{}
	for i := len(g.lines) - 1; i >= 0; i-- {
		if !g.lines[i].Pattern().Match(filePath) {
			continue
		}
		if v, ok := g.filter(g.lines[i].Attrs(), known); ok {
			return v == "lfs"
		}
	}
	return false
}

// filter resolves the filter attribute from attrs, expanding only set macros as git does.
func (g *GitAttributes) filter(attrs []*gitattr.Attr, known map[string]bool) (string, bool) {
	for i := len(attrs) - 1; i >= 0; i-- {
		a := attrs[i]
		if known[a.K] {
			continue
		}
		known[a.K] = true
		if a.K == "filter" {
			return a.V, true
		}
		if macro, ok := g.macros[a.K]; ok && a.V == "true" {
			if v, ok := g.filter(macro, known); ok {
				return v, true
			}
		}
	}
	return "", false
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
	content, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	lines, _, err := gitattr.ParseLines(strings.NewReader(normalizeAttrTabs(string(content))))
	if err != nil {
		return nil, err
	}
	ga = &GitAttributes{macros: map[string][]*gitattr.Attr{}}
	for _, line := range lines {
		switch l := line.(type) {
		case gitattr.PatternLine:
			ga.lines = append(ga.lines, l)
		case gitattr.MacroLine:
			ga.macros[l.Macro()] = l.Attrs()
		}
	}
	return ga, nil
}

// normalizeAttrTabs turns tabs into spaces outside quoted patterns; gitattr only splits on spaces.
func normalizeAttrTabs(content string) string {
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		start := 0
		if strings.HasPrefix(line, `"`) {
			start = strings.LastIndex(line, `"`)
		}
		lines[i] = line[:start] + strings.ReplaceAll(line[start:], "\t", " ")
	}
	return strings.Join(lines, "\n")
}
