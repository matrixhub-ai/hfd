package repository

import (
	_ "embed"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/go-git/go-git/v6/plumbing/format/gitattributes"
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
	rules  []gitattributes.MatchAttribute // pattern lines in file order
	macros map[string][]gitattributes.Attribute
}

// IsLFS returns true if the given file path matches an LFS filter pattern
// defined in the .gitattributes file.
func (g *GitAttributes) IsLFS(filePath string) bool {
	if g == nil {
		return false
	}
	segs := strings.Split(filePath, "/")
	// Like git's fill_one: later lines and later attributes win, each attribute is assigned once.
	known := map[string]bool{}
	for i := len(g.rules) - 1; i >= 0; i-- {
		if !matchPattern(g.rules[i].Name, segs) {
			continue
		}
		if lfs, ok := g.filter(g.rules[i].Attributes, known); ok {
			return lfs
		}
	}
	return false
}

// filter resolves whether attrs assign filter=lfs, expanding only set macros as git does.
func (g *GitAttributes) filter(attrs []gitattributes.Attribute, known map[string]bool) (lfs, ok bool) {
	for i := len(attrs) - 1; i >= 0; i-- {
		a := attrs[i]
		if known[a.Name()] {
			continue
		}
		known[a.Name()] = true
		if a.Name() == "filter" {
			return a.IsValueSet() && a.Value() == "lfs", true
		}
		if macro, isMacro := g.macros[a.Name()]; isMacro && a.IsSet() {
			if lfs, ok := g.filter(macro, known); ok {
				return lfs, true
			}
		}
	}
	return false, false
}

// matchPattern applies gitattributes glob rules to path segments: a slash-less pattern matches the
// basename, a slash anchors at the root, "**" spans directories (one or more when trailing) and a
// directory pattern never matches a file. Segments use path.Match, so cost stays linear.
func matchPattern(pattern string, segs []string) bool {
	if strings.HasSuffix(pattern, "/") {
		return false
	}
	if !strings.Contains(pattern, "/") {
		segs = segs[len(segs)-1:]
	}
	pat := strings.Split(strings.TrimPrefix(pattern, "/"), "/")
	if n := len(pat); n > 1 && pat[n-1] == "**" {
		pat = append(pat[:n-1], "*", "**")
	}
	// matched[j] reports whether the pattern segments so far match segs[:j].
	matched := make([]bool, len(segs)+1)
	matched[0] = true
	for _, p := range pat {
		if p == "**" {
			for j := 1; j <= len(segs); j++ {
				matched[j] = matched[j] || matched[j-1]
			}
			continue
		}
		p = strings.ReplaceAll(strings.ReplaceAll(p, "**", "*"), "[!", "[^")
		for j := len(segs); j > 0; j-- {
			ok, _ := path.Match(p, segs[j-1])
			matched[j] = matched[j-1] && ok
		}
		matched[0] = false
	}
	return matched[len(segs)]
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
	// go-git indexes the first field of a line holding only an empty quoted pattern.
	defer func() {
		if p := recover(); p != nil {
			ga, err = nil, fmt.Errorf("parse %s: %v", GitattributesFileName, p)
		}
	}()
	attrs, err := gitattributes.ReadAttributes(r, nil, true)
	if err != nil {
		return nil, err
	}
	ga = &GitAttributes{macros: map[string][]gitattributes.Attribute{}}
	for _, a := range attrs {
		if a.Pattern == nil {
			ga.macros[a.Name] = a.Attributes
		} else {
			ga.rules = append(ga.rules, a)
		}
	}
	return ga, nil
}
