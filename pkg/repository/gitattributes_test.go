package repository

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/util"
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
		{"unspecified macro keeps filter", "[attr]lfs filter=lfs\n*.bin filter=lfs !lfs\n", "model.bin", true},
		{"unspecified macro does not expand", "[attr]lfs filter=lfs\n*.bin filter=other !lfs\n", "model.bin", false},
		{"nested macro", "[attr]a filter=lfs\n[attr]b a\n*.bin b\n", "model.bin", true},
		{"later -macro blocks expansion", "[attr]lfs filter=lfs\n*.bin lfs\n*.bin -lfs\n", "model.bin", false},
		{"tab separated", "*.bin\tfilter=lfs\tdiff=lfs\n", "model.bin", true},
		{"quoted pattern then tab", "\"my model.bin\"\tfilter=lfs\n", "my model.bin", true},
		{"root anchored matches root", "/model.bin filter=lfs\n", "model.bin", true},
		{"root anchored skips subdir", "/model.bin filter=lfs\n", "sub/model.bin", false},
		{"doublestar backtracks", "**/foo/bar/** filter=lfs\n", "foo/bar/foo/baz", true},
		{"trailing doublestar needs content", "**/foo/bar/** filter=lfs\n", "foo/bar", false},
		{"in-segment doublestar", "a**b filter=lfs\n", "dir/axyb", true},
		{"directory pattern never matches", "dir/ filter=lfs\n", "dir/x.bin", false},
		{"bracket negation excludes", "[!a].bin filter=lfs\n", "a.bin", false},
		{"bracket negation includes", "[!a].bin filter=lfs\n", "b.bin", true},
		{"unbalanced quote is a literal pattern", "\"unbal filter=lfs\n", "\"unbal", true},
		{"trailing comment drops only its line", "*.bin filter=lfs\nREADME.md filter=lfs # c\n", "model.bin", true},
		{"trailing comment drops its line", "README.md filter=lfs # c\n", "README.md", false},
		{"builtin name drops its line", "*.bin filter=lfs builtin_foo\n", "model.bin", false},
		{"negative pattern is ignored", "!model.bin filter=lfs\n", "!model.bin", false},
		{"escaped exclamation is literal", "\\!model.bin filter=lfs\n", "!model.bin", true},
		{"non-ascii space is not a separator", "*.bin\u00a0filter=lfs\n", "model.bin", false},
		{"padding counts toward the line limit", strings.Repeat(" ", 2040) + "*.bin filter=lfs\n", "model.bin", false},
		{"byte order mark is skipped", "\ufeff*.bin filter=lfs\n", "model.bin", true},
		{"invalid attribute name drops only its line", "*.bin filter=lfs\n*.bin -filter bad@name\n", "model.bin", true},
		{"empty attribute name drops only its line", "*.bin filter=lfs\n*.bin =lfs -filter\n", "model.bin", true},
		{"unset with value clears", "*.bin filter=lfs\n*.bin -filter=lfs\n", "model.bin", false},
		{"unspecified with value clears", "*.bin filter=lfs\n*.bin !filter=lfs\n", "model.bin", false},
		{"unset with value on another pattern", "README.md -filter=lfs\n*.bin filter=lfs\n", "model.bin", true},
		{"empty quoted pattern drops only its line", "*.bin filter=lfs\n\"\"\n", "model.bin", true},
		{"overly long line is ignored", strings.Repeat("*", 2040) + " filter=lfs\n*.txt filter=lfs\n", "model.bin", false},
		{"line below the length limit applies", strings.Repeat("*", 2030) + " filter=lfs\n", "model.bin", true},
		{"macro value true is not set", "[attr]lfs filter=lfs\n*.bin lfs=true\n", "model.bin", false},
		{"other filter", "*.txt filter=other\n", "a.txt", false},
		// git never matches an unknown class; hfd keeps reading it as a plain set.
		{"unknown class reads as a plain set", "[[:foo:]].bin filter=lfs\n", "f].bin", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseGitAttributes([]byte(tt.content)).IsLFS(tt.path); got != tt.want {
				t.Errorf("IsLFS(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

// nativeFilters returns the filter attribute git check-attr reports for each path under a .gitattributes holding content.
func nativeFilters(t *testing.T, content string, paths ...string) []string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, GitattributesFileName), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) []byte {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", append([]string{"-c", "core.ignoreCase=false"}, args...)...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_ATTR_NOSYSTEM=1", "XDG_CONFIG_HOME="+dir)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, stderr.Bytes())
		}
		return out
	}
	git("init", "-q")
	fields := strings.Split(string(git(append([]string{"check-attr", "-z", "filter", "--"}, paths...)...)), "\x00")
	var values []string
	for i := 0; i+2 < len(fields); i += 3 {
		values = append(values, fields[i+2])
	}
	if len(values) != len(paths) {
		t.Fatalf("git check-attr reported %d paths, want %d: %q", len(values), len(paths), fields)
	}
	return values
}

// TestGitAttributesBracketExpressions pins wildmatch bracket expressions, POSIX classes included, against git check-attr.
func TestGitAttributesBracketExpressions(t *testing.T) {
	for _, tt := range []struct {
		pattern        string
		match, nomatch []string
	}{
		{"a[[:space:]]b.png", []string{"a b.png", "a\tb.png", "a\nb.png", "a\rb.png"}, []string{"ab.png", "a_b.png", "a\vb.png", "a\fb.png"}},
		{"dir/sub/p[[:space:]]2.png", []string{"dir/sub/p 2.png"}, []string{"p 2.png", "x/dir/sub/p 2.png", "dir/sub/p_2.png"}},
		{"[[:alpha:]].bin", []string{"a.bin", "Z.bin"}, []string{"1.bin", "_.bin", "é.bin"}},
		{"[[:digit:]].bin", []string{"0.bin", "9.bin"}, []string{"a.bin"}},
		{"[[:alnum:]].bin", []string{"a.bin", "Z.bin", "5.bin"}, []string{"-.bin", "_.bin"}},
		{"[[:upper:]].bin", []string{"A.bin", "Z.bin"}, []string{"a.bin", "1.bin"}},
		{"[[:lower:]].bin", []string{"a.bin", "z.bin"}, []string{"A.bin", "1.bin"}},
		{"[[:xdigit:]].bin", []string{"0.bin", "f.bin", "F.bin"}, []string{"g.bin", "G.bin"}},
		{"[[:blank:]].bin", []string{" .bin", "\t.bin"}, []string{"\n.bin", "a.bin"}},
		{"[[:cntrl:]].bin", []string{"\x01.bin", "\t.bin", "\x1f.bin", "\x7f.bin"}, []string{" .bin", "a.bin"}},
		{"[[:punct:]].bin", []string{"!.bin", "-.bin", "[.bin", `\.bin`, "`.bin", "~.bin"}, []string{"a.bin", " .bin", "0.bin"}},
		{"[[:print:]].bin", []string{" .bin", "a.bin", "~.bin"}, []string{"\x01.bin", "\x7f.bin"}},
		{"[[:graph:]].bin", []string{"!.bin", "a.bin", "~.bin"}, []string{" .bin", "\x7f.bin"}},
		{"[![:digit:]].bin", []string{"a.bin"}, []string{"1.bin"}},
		{"[^[:digit:]].bin", []string{"a.bin"}, []string{"1.bin"}},
		{"x[[:digit:]-]y", []string{"x5y", "x-y"}, []string{"xay"}},
		{"[[:digit:][:upper:]].bin", []string{"1.bin", "A.bin"}, []string{"a.bin"}},
		{"[a-c[:digit:]].bin", []string{"b.bin", "5.bin"}, []string{"d.bin"}},
		{"[[:alpha]].bin", []string{"a].bin", ":].bin"}, []string{"a.bin"}},
		{"[]a].bin", []string{"].bin", "a.bin"}, []string{"b.bin"}},
		{"[!]a].bin", []string{"b.bin"}, []string{"].bin", "a.bin"}},
		{"[a-].bin", []string{"a.bin", "-.bin"}, []string{"b.bin"}},
		{"[a-c-e].bin", []string{"b.bin", "-.bin", "e.bin"}, []string{"d.bin"}},
		{`\[!x\].png`, []string{"[!x].png"}, []string{"[^x].png", "x.png"}},
		{`a\**`, []string{"a*", "a*b"}, []string{"ab"}},
	} {
		t.Run(tt.pattern, func(t *testing.T) {
			content := tt.pattern + " filter=lfs\n"
			paths := slices.Concat(tt.match, tt.nomatch)
			attrs := ParseGitAttributes([]byte(content))
			for i, value := range nativeFilters(t, content, paths...) {
				want := i < len(tt.match)
				if got := attrs.IsLFS(paths[i]); got != want {
					t.Errorf("IsLFS(%q) = %v, want %v", paths[i], got, want)
				}
				if got := value == "lfs"; got != want {
					t.Errorf("git check-attr filter %q = %q, want lfs %v", paths[i], value, want)
				}
			}
		})
	}
}

func TestGitAttributesTrackLFS(t *testing.T) {
	const line = " filter=lfs diff=lfs merge=lfs -text\n"
	defaults := string(GitattributesText)
	tests := []struct {
		name    string
		content string
		paths   []string
		want    string
	}{
		{"uncovered path", defaults, []string{"c.png"}, defaults + "c.png" + line},
		{"covered path", defaults, []string{"model2.bin"}, defaults},
		{"no paths", defaults, nil, defaults},
		{"paths in order", "", []string{"p1.png", "dir/sub/p 2.png"}, "p1.png" + line + "dir/sub/p[[:space:]]2.png" + line},
		{"same path once", "", []string{"c.png", "c.png"}, "c.png" + line},
		{"missing final newline", "*.rst filter=lfs diff=lfs merge=lfs -text", []string{"doc.rst", "c.png"}, "*.rst filter=lfs diff=lfs merge=lfs -text\nc.png" + line},
		{"git semantics decide coverage", "*.md filter=lfs\n", []string{"doc.md"}, "*.md filter=lfs\n"},
		{"unset path gets a line", "*.png -filter\n", []string{"c.png"}, "*.png -filter\nc.png" + line},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, changed := TrackLFS([]byte(tt.content), tt.paths...)
			if string(got) != tt.want || changed != (tt.want != tt.content) {
				t.Fatalf("TrackLFS = %q, %v; want %q", got, changed, tt.want)
			}
			attrs := ParseGitAttributes(got)
			for _, p := range tt.paths {
				if !attrs.IsLFS(p) {
					t.Errorf("IsLFS(%q) = false after TrackLFS", p)
				}
			}
		})
	}
}

// TestGitAttributesTrackLFSEscapes pins the pattern each path is written as, and that IsLFS and git both route the path through it.
func TestGitAttributesTrackLFSEscapes(t *testing.T) {
	for _, tt := range []struct{ path, pattern string }{
		{"c.png", "c.png"},
		{"a b.png", "a[[:space:]]b.png"},
		{"tab\tx.png", "tab[[:space:]]x.png"},
		{"cr\rx.png", "cr[[:space:]]x.png"},
		{"lf\nx.png", "lf[[:space:]]x.png"},
		{"br[a].png", `br\[a\].png`},
		{"star*.png", `star\*.png`},
		{"q?.png", `q\?.png`},
		{"hash#.png", `hash\#.png`},
		{"#lead.png", `\#lead.png`},
		{"!bang.png", `\!bang.png`},
		{"mid!bang.png", "mid!bang.png"},
		{`back\slash.png`, `back\\slash.png`},
		{`"quote.png`, `\"quote.png`},
		{`mid"quote.png`, `mid"quote.png`},
		{"[!x].png", `\[!x\].png`},
		{"ü.png", "ü.png"},
		{"dir/sub/p 2.png", "dir/sub/p[[:space:]]2.png"},
	} {
		t.Run(tt.pattern, func(t *testing.T) {
			got, _ := TrackLFS(nil, tt.path)
			if want := tt.pattern + " filter=lfs diff=lfs merge=lfs -text\n"; string(got) != want {
				t.Fatalf("TrackLFS(%q) = %q, want %q", tt.path, got, want)
			}
			if !ParseGitAttributes(got).IsLFS(tt.path) {
				t.Errorf("IsLFS(%q) = false under %q", tt.path, got)
			}
			if value := nativeFilters(t, string(got), tt.path)[0]; value != "lfs" {
				t.Errorf("git check-attr filter %q = %q under %q, want lfs", tt.path, value, got)
			}
		})
	}
}

// TestGitAttributesBranch pins BranchGitattributes: an unborn branch or a missing file is nil, an unreadable branch an error.
func TestGitAttributesBranch(t *testing.T) {
	fs := memfs.New()
	repo, err := Init(t.Context(), fs, "/repo.git", "main")
	if err != nil {
		t.Fatalf("init repo: %v", err)
	}
	mustCommit(t, repo, "main", "", CommitOperation{Type: CommitOperationAdd, Path: GitattributesFileName, Content: []byte("*.bin filter=lfs\n")})
	mustCommit(t, repo, "plain", "", CommitOperation{Type: CommitOperationAdd, Path: "README.md", Content: []byte("# plain\n")})
	for _, tt := range []struct{ branch, want string }{
		{"main", "*.bin filter=lfs\n"},
		{"plain", ""},
		{"unborn", ""},
	} {
		if got, err := repo.BranchGitattributes(tt.branch); err != nil || string(got) != tt.want {
			t.Errorf("BranchGitattributes(%q) = %q, %v; want %q", tt.branch, got, err, tt.want)
		}
	}

	if err := fs.Remove("/repo.git/refs/heads/main"); err != nil {
		t.Fatalf("remove loose ref: %v", err)
	}
	// A truncated packed-refs line fails the ref read; go-git only reads it once the loose ref is gone.
	if err := util.WriteFile(fs, "/repo.git/packed-refs", []byte(strings.Repeat("1", 40)+"\n"), 0o644); err != nil {
		t.Fatalf("write packed-refs: %v", err)
	}
	if got, err := repo.BranchGitattributes("main"); err == nil {
		t.Fatalf("BranchGitattributes over an unreadable ref = %q, nil; want an error", got)
	}
}

func TestGitAttributesMalformedPattern(t *testing.T) {
	for _, content := range []string{
		"model.[bin filter=lfs\n",
		"model.[[:alpha filter=lfs\n",
		"model.[a- filter=lfs\n",
		"\"\"\n",
	} {
		if ParseGitAttributes([]byte(content)).IsLFS("model.bin") {
			t.Errorf("%q: IsLFS(model.bin) = true, want false", content)
		}
	}
}

// Both inputs come from repository writers and upload clients, so matching must stay cheap.
func TestGitAttributesPathologicalCost(t *testing.T) {
	for _, tt := range []struct {
		content, path string
		want          bool
	}{
		{strings.Repeat("*a", 7) + "*b filter=lfs\n", strings.Repeat("a", 30), false},
		{strings.Repeat("**/", 10) + "b.bin filter=lfs\n", strings.Repeat("d/", 30) + "a.bin", false},
		{strings.Repeat("*", 60000) + " filter=lfs\n", "model.bin", false},
	} {
		start := time.Now()
		if got := ParseGitAttributes([]byte(tt.content)).IsLFS(tt.path); got != tt.want {
			t.Errorf("IsLFS(%q) = %v, want %v", tt.path, got, tt.want)
		}
		if d := time.Since(start); d > 200*time.Millisecond {
			t.Errorf("pattern of %d bytes took %v", len(tt.content), d)
		}
	}
}
