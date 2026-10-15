package repository

import (
	_ "embed"
	"errors"
	"io"
	"path"
	"strings"
	"unicode/utf8"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

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
		glob, ok := globSegment(p)
		if !ok {
			return false
		}
		for j := len(segs); j > 0; j-- {
			ok, _ := path.Match(glob, segs[j-1])
			matched[j] = matched[j-1] && ok
		}
		matched[0] = false
	}
	return matched[len(segs)]
}

// globSegment rewrites a wildmatch pattern segment into path.Match syntax: a run of '*' is one '*'
// within a segment and a bracket expression becomes a set of escaped members. It reports false for
// a segment git never matches.
func globSegment(p string) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		switch p[i] {
		case '\\':
			b.WriteString(p[i:min(i+2, len(p))])
			i++
		case '*':
			b.WriteByte('*')
			for i+1 < len(p) && p[i+1] == '*' {
				i++
			}
		case '[':
			set, n := bracketSet(p[i:])
			if n == 0 {
				return "", false
			}
			b.WriteString(set)
			i += n - 1
		default:
			b.WriteByte(p[i])
		}
	}
	return b.String(), true
}

// bracketSet translates the bracket expression opening p and returns it with the length of p it
// spans, 0 when it is not terminated. As in git, a leading ']' is a member, '-' spans a range only
// after a plain member, and POSIX classes such as [:space:] expand to their ranges. Unlike git,
// whose wildmatch never matches a pattern naming an unknown class, [:foo:] reads as plain members.
func bracketSet(p string) (string, int) {
	var b strings.Builder
	b.WriteByte('[')
	i := 1
	if i < len(p) && (p[i] == '!' || p[i] == '^') {
		b.WriteByte('^')
		i++
	}
	first, afterMember := i, false
	for i < len(p) {
		if ranges, n := posixClass(p[i:]); n > 0 {
			for ; ranges != ""; ranges = ranges[2:] {
				b.WriteString(`\` + ranges[:1] + `-\` + ranges[1:2])
			}
			i += n
			afterMember = false
			continue
		}
		switch {
		case p[i] == ']' && i > first:
			return b.String() + "]", i + 1
		case p[i] == '-' && afterMember && i+1 < len(p) && p[i+1] != ']':
			hi, n := setMember(p[i+1:])
			if n == 0 {
				return "", 0
			}
			b.WriteString("-" + hi)
			i += 1 + n
			afterMember = false
		default:
			member, n := setMember(p[i:])
			if n == 0 {
				return "", 0
			}
			b.WriteString(member)
			i += n
			afterMember = true
		}
	}
	return "", 0
}

// setMember reads the rune, escaped or not, at the start of p and returns it escaped for path.Match
// with its length in p, 0 when p is a lone backslash.
func setMember(p string) (string, int) {
	start := 0
	if p[0] == '\\' {
		start = 1
	}
	if start == len(p) {
		return "", 0
	}
	_, size := utf8.DecodeRuneInString(p[start:])
	return `\` + p[start:start+size], start + size
}

// posixClasses are git's bracket classes as lo-hi byte pairs; git's ctype is ASCII-only and its
// space omits \v and \f.
var posixClasses = map[string]string{
	"alnum":  "09AZaz",
	"alpha":  "AZaz",
	"blank":  "\t\t  ",
	"cntrl":  "\x00\x1f\x7f\x7f",
	"digit":  "09",
	"graph":  "!~",
	"lower":  "az",
	"print":  " ~",
	"punct":  "!/:@[`{~",
	"space":  "\t\n\r\r  ",
	"upper":  "AZ",
	"xdigit": "09AFaf",
}

// posixClass returns the ranges and length of the class "[:name:]" opening p; n is 0 for anything
// else, including an unknown name, which then reads as plain members.
func posixClass(p string) (ranges string, n int) {
	if !strings.HasPrefix(p, "[:") {
		return "", 0
	}
	before, _, found := strings.Cut(p[2:], "]")
	name, ok := strings.CutSuffix(before, ":")
	if !found || !ok || posixClasses[name] == "" {
		return "", 0
	}
	return posixClasses[name], len(before) + 3
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
		var reader io.ReadCloser
		if reader, err = blob.NewReader(); err != nil {
			return nil, false
		}
		defer reader.Close()
		var content []byte
		if content, err = io.ReadAll(reader); err != nil {
			return nil, false
		}
		return ParseGitAttributes(content), true
	})
	return ga, err
}

// BranchGitattributes returns the .gitattributes content at the tip of branch, nil when the branch is
// unborn or has none. Unlike Blob, it reports every other read failure instead of a missing revision.
func (r *Repository) BranchGitattributes(branch string) ([]byte, error) {
	ref, err := r.repo.Storer.Reference(plumbing.NewBranchReferenceName(branch))
	if errors.Is(err, plumbing.ErrReferenceNotFound) || (err == nil && ref.Hash().IsZero()) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	commit, err := r.repo.CommitObject(ref.Hash())
	if err != nil {
		return nil, err
	}
	file, err := commit.File(GitattributesFileName)
	if errors.Is(err, object.ErrFileNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	content, err := file.Contents()
	return []byte(content), err
}

// TrackLFS appends to .gitattributes content a line for each path it does not route to LFS yet, as
// huggingface.co's commit API does for LFS uploads; changed is false when every path is routed.
func TrackLFS(content []byte, paths ...string) (updated []byte, changed bool) {
	attrs := ParseGitAttributes(content)
	var lines strings.Builder
	seen := map[string]bool{}
	for _, p := range paths {
		if seen[p] || attrs.IsLFS(p) {
			continue
		}
		seen[p] = true
		lines.WriteString(lfsTrackLine(p))
	}
	if lines.Len() == 0 {
		return content, false
	}
	updated = make([]byte, 0, len(content)+1+lines.Len())
	updated = append(updated, content...)
	if len(content) > 0 && content[len(content)-1] != '\n' {
		updated = append(updated, '\n')
	}
	return append(updated, lines.String()...), true
}

// lfsTrackLine is the line huggingface.co appends for an LFS upload at filePath, escaped so that git
// matches it where the Hub's escaping does not (a leading '!' or '"', backslash, TAB, CR, LF).
func lfsTrackLine(filePath string) string {
	var b strings.Builder
	for i := 0; i < len(filePath); i++ {
		switch c := filePath[i]; {
		case strings.IndexByte(attrBlanks, c) >= 0:
			b.WriteString("[[:space:]]")
		case strings.IndexByte(`\[]*?#`, c) >= 0, i == 0 && (c == '!' || c == '"'):
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteString(" filter=lfs diff=lfs merge=lfs -text\n")
	return b.String()
}

// ParseGitAttributes parses .gitattributes content.
func ParseGitAttributes(content []byte) *GitAttributes {
	ga := &GitAttributes{macros: map[string][]attribute{}}
	for _, line := range strings.Split(strings.TrimPrefix(string(content), "\ufeff"), "\n") {
		pattern, attrs, ok := parseAttrLine(line)
		switch macro, isMacro := strings.CutPrefix(pattern, "[attr]"); {
		case !ok || len(attrs) == 0:
		case isMacro:
			if validAttrName(macro) {
				ga.macros[macro] = attrs
			}
		default:
			ga.rules = append(ga.rules, attrRule{pattern, attrs})
		}
	}
	return ga
}

// maxAttrLine is git's attribute line limit; longer lines are ignored.
const maxAttrLine = 2048

// attrBlanks are the only separators git recognizes in .gitattributes.
const attrBlanks = " \t\r\n"

// parseAttrLine splits a line into pattern and attributes like git: blank, comment, overly long and
// negative-pattern lines and lines naming an invalid attribute are skipped; a quoted pattern may
// contain spaces.
func parseAttrLine(line string) (pattern string, attrs []attribute, ok bool) {
	if len(line) >= maxAttrLine {
		return "", nil, false
	}
	line = strings.Trim(line, attrBlanks)
	if line == "" || line[0] == '#' {
		return "", nil, false
	}
	rest := line
	if line[0] == '"' {
		if end := closingQuote(line); end > 0 {
			pattern, rest = line[1:end], line[end+1:]
		}
	}
	fields := strings.FieldsFunc(rest, func(r rune) bool { return strings.ContainsRune(attrBlanks, r) })
	if pattern == "" {
		if len(fields) == 0 {
			return "", nil, false
		}
		pattern, fields = fields[0], fields[1:]
	}
	if pattern[0] == '!' {
		return "", nil, false
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

// validAttrName mirrors git: letters, digits, '-', '.' and '_', not starting with '-' or the
// reserved "builtin_" prefix.
func validAttrName(name string) bool {
	if name == "" || name[0] == '-' || strings.HasPrefix(name, "builtin_") {
		return false
	}
	for _, c := range name {
		if c != '-' && c != '.' && c != '_' && (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}
