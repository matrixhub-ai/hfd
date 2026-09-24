package repository

import (
	_ "embed"
	"io"
	"path"
	"strings"

	"github.com/matrixhub-ai/hfd/internal/lru"
)

// GitattributesFileName is the name of the .gitattributes file in the repository.
const GitattributesFileName = ".gitattributes"

// GitattributesText is the content of a default .gitattributes file that marks common large/binary
// file types to be tracked with Git LFS.
//
//go:embed gitattributes.txt
var GitattributesText []byte

// attribute is one assignment on a .gitattributes line: a bare name sets it, name=value gives it
// a value, and -name or !name clears it (value empty, not set).
type attribute struct {
	name, value string
	set         bool
}

type attrRule struct {
	pattern string
	attrs   []attribute
}

// GitAttributes represents parsed .gitattributes content and provides
// methods to check if a file path matches LFS filter patterns.
type GitAttributes struct {
	rules  []attrRule // pattern lines in file order
	macros map[string][]attribute
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
		if !matchPattern(g.rules[i].pattern, segs) {
			continue
		}
		if lfs, ok := g.filter(g.rules[i].attrs, known); ok {
			return lfs
		}
	}
	return false
}

// filter resolves whether attrs assign filter=lfs, expanding only set macros as git does.
func (g *GitAttributes) filter(attrs []attribute, known map[string]bool) (lfs, ok bool) {
	for i := len(attrs) - 1; i >= 0; i-- {
		a := attrs[i]
		if known[a.name] {
			continue
		}
		known[a.name] = true
		if a.name == "filter" {
			return a.value == "lfs", true
		}
		if macro, isMacro := g.macros[a.name]; isMacro && a.set {
			if lfs, ok := g.filter(macro, known); ok {
				return lfs, true
			}
		}
	}
	return false, false
}

// matchPattern applies gitattributes glob rules to path segments: a slash-less pattern matches the
// basename, a slash anchors at the root and "**" spans directories (one or more when trailing).
// Segments go through path.Match, which keeps the cost polynomial in the input sizes.
func matchPattern(pattern string, segs []string) bool {
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

func parseGitAttributesReader(r io.Reader) (*GitAttributes, error) {
	content, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	ga := &GitAttributes{macros: map[string][]attribute{}}
	for _, line := range strings.Split(string(content), "\n") {
		pattern, attrs, ok := parseAttrLine(line)
		switch macro, isMacro := strings.CutPrefix(pattern, "[attr]"); {
		case !ok:
		case isMacro:
			if validAttrName(macro) {
				ga.macros[macro] = attrs
			}
		default:
			ga.rules = append(ga.rules, attrRule{pattern, attrs})
		}
	}
	return ga, nil
}

// maxAttrLine is git's attribute line limit; longer lines are ignored.
const maxAttrLine = 2048

// parseAttrLine splits a line into pattern and attributes like git: blank, comment and overly long
// lines and lines naming an invalid attribute are skipped; a quoted pattern may contain spaces.
func parseAttrLine(line string) (pattern string, attrs []attribute, ok bool) {
	line = strings.TrimSpace(line)
	if line == "" || line[0] == '#' || len(line) >= maxAttrLine {
		return "", nil, false
	}
	rest := line
	if line[0] == '"' {
		if end := closingQuote(line); end > 0 {
			pattern, rest = line[1:end], line[end+1:]
		}
	}
	fields := strings.Fields(rest)
	if pattern == "" {
		if len(fields) == 0 {
			return "", nil, false
		}
		pattern, fields = fields[0], fields[1:]
	}
	for _, f := range fields {
		a := attribute{name: f, set: true}
		if f[0] == '-' || f[0] == '!' {
			a = attribute{name: f[1:]}
			a.name, _, _ = strings.Cut(a.name, "=")
		} else if name, value, found := strings.Cut(f, "="); found {
			a = attribute{name: name, value: value}
		}
		if !validAttrName(a.name) {
			return "", nil, false
		}
		attrs = append(attrs, a)
	}
	return pattern, attrs, true
}

// closingQuote returns the index of the quote closing the quoted pattern at line[0], or 0.
func closingQuote(line string) int {
	for i := 1; i < len(line); i++ {
		switch line[i] {
		case '\\':
			i++
		case '"':
			return i
		}
	}
	return 0
}

// validAttrName mirrors git: letters, digits, '-', '.' and '_', not starting with '-'.
func validAttrName(name string) bool {
	if name == "" || name[0] == '-' {
		return false
	}
	for _, c := range name {
		if c != '-' && c != '.' && c != '_' && (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}
