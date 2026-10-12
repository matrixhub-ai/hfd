package hf_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/matrixhub-ai/hfd/pkg/backend/hf"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// uploadFile is one entry of a preupload request.
type uploadFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Sample string `json:"sample"`
}

// commitOp is one operation of a Hub commit: a "file" carrying content, an "lfsFile" or a "deletedFile".
type commitOp struct {
	key, path string
	content   []byte
}

func fileOp(path string, content []byte) commitOp { return commitOp{"file", path, content} }
func lfsFileOp(path string) commitOp              { return commitOp{key: "lfsFile", path: path} }
func deletedFileOp(path string) commitOp          { return commitOp{key: "deletedFile", path: path} }

// newUploadRepo returns a handler with huggingface.co's LFS policy, then opts, over org/repo whose main branch holds the default .gitattributes.
func newUploadRepo(t *testing.T, opts ...hf.Option) (http.Handler, *repository.Repository) {
	t.Helper()
	st := newStorage(t, t.TempDir())
	repo, err := repository.Init(t.Context(), st.RepositoriesFS(), repository.ResolvePath("org/repo"), "main")
	if err != nil {
		t.Fatalf("init repo: %v", err)
	}
	if _, err := repo.CreateCommit(t.Context(), "main", "init", "Test", "test@test.com", []repository.CommitOperation{
		{Type: repository.CommitOperationAdd, Path: repository.GitattributesFileName, Content: repository.GitattributesText},
	}, ""); err != nil {
		t.Fatalf("create commit: %v", err)
	}
	return hf.NewHandler(append([]hf.Option{hf.WithStorage(st), hf.WithLFSPolicy(lfs.DefaultPolicy())}, opts...)...), repo
}

// preuploadModes asks preupload on branch for files, replacing the repository's .gitattributes unless gitAttributes is nil.
func preuploadModes(t *testing.T, h http.Handler, branch string, gitAttributes *string, files ...uploadFile) []string {
	t.Helper()
	body, err := json.Marshal(struct {
		Files         []uploadFile `json:"files"`
		GitAttributes *string      `json:"gitAttributes,omitempty"`
	}{files, gitAttributes})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/models/org/repo/preupload/"+branch, bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("preupload status = %d, want 200: %s", rec.Code, rec.Body)
	}
	var resp preuploadResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode preupload response: %v", err)
	}
	var modes []string
	for _, f := range resp.Files {
		modes = append(modes, f.UploadMode)
	}
	return modes
}

// postCommit commits ops to branch through the NDJSON commit route.
func postCommit(t *testing.T, h http.Handler, branch string, ops ...commitOp) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	if err := enc.Encode(map[string]any{"key": "header", "value": map[string]any{"summary": "upload"}}); err != nil {
		t.Fatal(err)
	}
	for _, op := range ops {
		value := map[string]any{"path": op.path}
		switch op.key {
		case "file":
			value["content"], value["encoding"] = base64.StdEncoding.EncodeToString(op.content), "base64"
		case "lfsFile":
			value["algo"], value["oid"], value["size"] = "sha256", strings.Repeat("0", 64), 1024
		}
		if err := enc.Encode(map[string]any{"key": op.key, "value": value}); err != nil {
			t.Fatal(err)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/models/org/repo/commit/"+branch, &body))
	return rec
}

// readBlob returns the content of path at rev.
func readBlob(t *testing.T, repo *repository.Repository, rev, path string) []byte {
	t.Helper()
	blob, err := repo.Blob(rev, path)
	if err != nil {
		t.Fatalf("blob %s@%s: %v", path, rev, err)
	}
	r, err := blob.NewReader()
	if err != nil {
		t.Fatalf("blob reader %s@%s: %v", path, rev, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read blob %s@%s: %v", path, rev, err)
	}
	return data
}

// TestHuggingFacePreuploadLFSRules pins huggingface.co's preupload routing: the size rule, the binary
// rule on the sample above 100000 bytes, and a request-level gitAttributes replacing the repository's.
func TestHuggingFacePreuploadLFSRules(t *testing.T) {
	h, _ := newUploadRepo(t)
	random := make([]byte, 512)
	_, _ = rand.NewChaCha8([32]byte{}).Read(random)
	ascii := []byte(strings.Repeat("plain text\n", 40))
	utf16Text := []byte{0xFF, 0xFE}
	for _, r := range utf16.Encode([]rune(strings.Repeat("你好世界", 50))) {
		utf16Text = append(utf16Text, byte(r), byte(r>>8))
	}
	sample := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	attrs := func(s string) *string { return &s }
	tests := []struct {
		name          string
		gitAttributes *string
		file          uploadFile
		want          string
	}{
		{"text at the size limit", nil, uploadFile{"small.txt", 10485760, ""}, "regular"},
		{"text over the size limit", nil, uploadFile{"notes.txt", 10485761, ""}, "lfs"},
		{"binary sample over the binary limit", nil, uploadFile{"blob.dat", 100001, sample(random)}, "lfs"},
		{"binary sample at the binary limit", nil, uploadFile{"blob.dat", 100000, sample(random)}, "regular"},
		{"text sample over the binary limit", nil, uploadFile{"notes.txt", 100001, sample(ascii)}, "regular"},
		{"pdf sample", nil, uploadFile{"doc.txt", 100001, sample(append([]byte("%PDF-1.7\n"), ascii...))}, "lfs"},
		{"utf-16 sample libgit2 calls binary", nil, uploadFile{"utf16.txt", 100001, sample(utf16Text)}, "lfs"},
		{"no extension rule", nil, uploadFile{"image.png", 10, sample([]byte("image data"))}, "regular"},
		{"empty file under an lfs rule", nil, uploadFile{"model.bin", 0, ""}, "lfs"},
		{"empty file", nil, uploadFile{"a.txt", 0, ""}, "regular"},
		{"invalid sample counts as empty", nil, uploadFile{"blob.dat", 100001, "AAAA*"}, "regular"},
		{"empty request attributes", attrs(""), uploadFile{"model.bin", 100, ""}, "regular"},
		{"request attributes route", attrs("*.txt filter=lfs diff=lfs merge=lfs -text\n"), uploadFile{"a.txt", 10, ""}, "lfs"},
		{"request attributes replace the repository's", attrs("*.txt filter=lfs diff=lfs merge=lfs -text\n"), uploadFile{"model.bin", 10, ""}, "regular"},
		{"attributes cannot lower the size limit", attrs("*.txt -filter\n"), uploadFile{"big.txt", 10485761, ""}, "lfs"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := preuploadModes(t, h, "main", tt.gitAttributes, tt.file); len(got) != 1 || got[0] != tt.want {
				t.Errorf("uploadMode = %v, want %s", got, tt.want)
			}
		})
	}
}

// TestHuggingFaceCommitLFSPolicy pins huggingface.co's refusal of inline files larger than 10 MiB, or
// binary and larger than 100000 bytes: large files first, one entry per distinct content.
func TestHuggingFaceCommitLFSPolicy(t *testing.T) {
	const (
		large  = "Your push was rejected because it contains files larger than 10 MiB. Please use https://git-lfs.github.com/ to store large files. See also: https://hf.co/docs/hub/repositories-getting-started#terminal  Offending files:"
		binary = "Your push was rejected because it contains binary files. Please use https://huggingface.co/docs/hub/xet to store binary files. See also: https://huggingface.co/docs/hub/xet/using-xet-storage#git  Offending files:"
	)
	text := func(n int) []byte { return bytes.Repeat([]byte("a"), n) }
	nul := func(n int) []byte { return make([]byte, n) }
	entry := func(path string) string { return "   - " + path + " (ref: refs/heads/main)" }
	tests := []struct {
		name  string
		files []commitOp
		want  string // the refusal; empty when the commit is accepted
	}{
		{"text at the size limit", []commitOp{fileOp("b1.txt", text(10485760))}, ""},
		{"text over the size limit", []commitOp{fileOp("b2.txt", text(10485761))}, large + entry("b2.txt")},
		{"binary at the binary limit", []commitOp{fileOp("d1.txt", nul(100000))}, ""},
		{"binary over the binary limit", []commitOp{fileOp("d3-1.txt", nul(100001))}, binary + entry("d3-1.txt")},
		{"large wins over binary", []commitOp{fileOp("bin.dat", nul(100001)), fileOp("big.dat", nul(10485761))}, large + entry("big.dat")},
		{"one entry per content", []commitOp{fileOp("d3-1.txt", nul(100001)), fileOp("d3-2.txt", nul(100001)), fileOp("d3-3.txt", nul(100002))}, binary + entry("d3-1.txt") + entry("d3-3.txt")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, repo := newUploadRepo(t)
			before, err := repo.ResolveRevision("main")
			if err != nil {
				t.Fatalf("resolve main: %v", err)
			}
			rec := postCommit(t, h, "main", tt.files...)
			if tt.want == "" {
				if rec.Code != http.StatusOK {
					t.Fatalf("commit status = %d, want 200: %s", rec.Code, rec.Body)
				}
				for _, f := range tt.files {
					blob, err := repo.Blob("main", f.path)
					if err != nil {
						t.Fatalf("blob %s: %v", f.path, err)
					}
					if ptr, _ := blob.LFSPointer(); ptr != nil || blob.Size() != int64(len(f.content)) {
						t.Errorf("%s: size %d, pointer %v; want a regular %d-byte blob", f.path, blob.Size(), ptr, len(f.content))
					}
				}
				return
			}
			var resp struct {
				Error string `json:"error"`
			}
			if rec.Code != http.StatusBadRequest || json.Unmarshal(rec.Body.Bytes(), &resp) != nil || resp.Error != tt.want {
				t.Fatalf("commit = %d %s, want 400 with %q", rec.Code, rec.Body, tt.want)
			}
			if after, _ := repo.ResolveRevision("main"); after != before {
				t.Fatalf("main moved from %s to %s on a refused commit", before, after)
			}
		})
	}

	t.Run("disabled policy", func(t *testing.T) {
		h, _ := newUploadRepo(t, hf.WithLFSPolicy(lfs.Policy{}))
		if rec := postCommit(t, h, "main", fileOp("d3-1.txt", nul(100001))); rec.Code != http.StatusOK {
			t.Fatalf("commit status = %d, want 200: %s", rec.Code, rec.Body)
		}
	})
}

// TestHuggingFaceCommitTracksLFS pins the line a commit appends to .gitattributes for each lfsFile path
// the .gitattributes it produces does not route to LFS, and that preupload then routes the path to LFS.
func TestHuggingFaceCommitTracksLFS(t *testing.T) {
	const line = " filter=lfs diff=lfs merge=lfs -text\n"
	defaults := string(repository.GitattributesText)
	tests := []struct {
		name   string
		branch string
		ops    []commitOp
		want   string
	}{
		{"uncovered path", "main", []commitOp{lfsFileOp("c.png")}, defaults + "c.png" + line},
		{"covered path", "main", []commitOp{lfsFileOp("model2.bin")}, defaults},
		{"space", "main", []commitOp{lfsFileOp("a b.png")}, defaults + "a[[:space:]]b.png" + line},
		{"brackets", "main", []commitOp{lfsFileOp("br[a].png")}, defaults + `br\[a\].png` + line},
		{"star", "main", []commitOp{lfsFileOp("star*.png")}, defaults + `star\*.png` + line},
		{"question mark", "main", []commitOp{lfsFileOp("q?.png")}, defaults + `q\?.png` + line},
		{"hash", "main", []commitOp{lfsFileOp("hash#.png")}, defaults + `hash\#.png` + line},
		{"leading bang", "main", []commitOp{lfsFileOp("!bang.png")}, defaults + `\!bang.png` + line},
		{"backslash", "main", []commitOp{lfsFileOp(`back\slash.png`)}, defaults + `back\\slash.png` + line},
		{"two files in order", "main", []commitOp{lfsFileOp("p1.png"), lfsFileOp("dir/sub/p 2.png")}, defaults + "p1.png" + line + "dir/sub/p[[:space:]]2.png" + line},
		{"same path once", "main", []commitOp{lfsFileOp("c.png"), lfsFileOp("c.png")}, defaults + "c.png" + line},
		{"client .gitattributes kept", "main", []commitOp{
			fileOp(".gitattributes", []byte("*.rst filter=lfs diff=lfs merge=lfs -text")), lfsFileOp("doc.rst"), lfsFileOp("c.png"),
		}, "*.rst filter=lfs diff=lfs merge=lfs -text\nc.png" + line},
		{"client .gitattributes after the upload", "main", []commitOp{
			lfsFileOp("c.png"), fileOp(".gitattributes", []byte("*.rst filter=lfs diff=lfs merge=lfs -text\n")),
		}, "*.rst filter=lfs diff=lfs merge=lfs -text\nc.png" + line},
		{"git semantics decide coverage", "main", []commitOp{fileOp(".gitattributes", []byte("*.md filter=lfs\n")), lfsFileOp("doc.md")}, "*.md filter=lfs\n"},
		{"deleted .gitattributes", "main", []commitOp{deletedFileOp(".gitattributes"), lfsFileOp("c.png")}, "c.png" + line},
		{"last .gitattributes operation wins", "main", []commitOp{
			fileOp(".gitattributes", []byte("*.png filter=lfs\n")), deletedFileOp(".gitattributes"), lfsFileOp("c.png"),
		}, "c.png" + line},
		{"new branch", "fresh", []commitOp{lfsFileOp("c.png")}, "c.png" + line},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, repo := newUploadRepo(t)
			if rec := postCommit(t, h, tt.branch, tt.ops...); rec.Code != http.StatusOK {
				t.Fatalf("commit status = %d, want 200: %s", rec.Code, rec.Body)
			}
			if got := string(readBlob(t, repo, tt.branch, repository.GitattributesFileName)); got != tt.want {
				t.Fatalf(".gitattributes = %q, want %q", got, tt.want)
			}
			attrs, err := repo.GitAttributes(tt.branch)
			if err != nil {
				t.Fatalf("read .gitattributes: %v", err)
			}
			for _, op := range tt.ops {
				if op.key != "lfsFile" {
					continue
				}
				blob, err := repo.Blob(tt.branch, op.path)
				if err != nil {
					t.Fatalf("blob %s: %v", op.path, err)
				}
				if ptr, _ := blob.LFSPointer(); ptr == nil {
					t.Errorf("%s is not an LFS pointer", op.path)
				}
				if !attrs.IsLFS(op.path) {
					t.Errorf("IsLFS(%q) = false after the commit", op.path)
				}
				if modes := preuploadModes(t, h, tt.branch, nil, uploadFile{op.path, 10, base64.StdEncoding.EncodeToString([]byte("0123456789"))}); modes[0] != "lfs" {
					t.Errorf("preupload %q after the commit = %s, want lfs", op.path, modes[0])
				}
			}
		})
	}
}
