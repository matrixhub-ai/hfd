package hf

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/mirror/spool"
	xetserver "github.com/wzshiming/xet/server"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// newXETDataPlane assembles the xet data-plane pieces the way cmd/hfd does —
// file storage, client options, token scheme, and the ingest engine fed by
// upstreamURL when set — and builds the mirror over them, returning the
// mirror and the CAS-server composition. wrap, when set, decorates the
// storage everything is built over.
func newXETDataPlane(t *testing.T, upstreamURL string, wrap func(xetstorage.Storage) xetstorage.Storage) (*mirror.Mirror, http.Handler) {
	t.Helper()
	st := newStorage(t, newXETDataDir(t))
	xs := st.XETStorage()
	var wrapped xetstorage.Storage = xs
	if wrap != nil {
		wrapped = wrap(xs)
	}
	sp := newTestSpool(t, st, wrapped)
	issuer, err := auth.NewIssuer(nil, time.Hour, nil)
	if err != nil {
		t.Fatalf("create issuer: %v", err)
	}
	opts := []mirror.Option{
		mirror.WithXETStorage(wrapped),
		mirror.WithXETCache(xetclient.NewCache(st.XETMirrorDir(), 0, 0)),
		mirror.WithDataDir(st.XETDir()),
		mirror.WithMintToken(issuer.Sign),
	}
	if upstreamURL != "" {
		engine, err := xetmirror.NewMirror(
			xetmirror.WithStorage(wrapped),
			xetmirror.WithCacheDir(st.XETMirrorDir()),
			xetmirror.WithSpool(sp),
		)
		if err != nil {
			t.Fatalf("create xet mirror engine: %v", err)
		}
		opts = append(opts,
			mirror.WithXETMirror(engine),
			mirror.WithMirrorSourceFunc(func(ctx context.Context, repoName string) (string, bool, error) {
				return upstreamURL + "/" + repoName, true, nil
			}),
		)
	}
	cas := xetserver.NewHandler(
		xetserver.WithStorage(wrapped),
		xetserver.WithAuthorizer(issuer),
		xetserver.WithNext(http.NotFoundHandler()),
	)
	m, err := mirror.NewMirror(opts...)
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	// Background work must not outlive the temp data dir.
	t.Cleanup(m.Wait)
	return m, cas
}

// newXETDataDir returns a data dir removed best-effort: the engine's ingest
// finalize can outlive the test body, and strict t.TempDir cleanup races it.
func newXETDataDir(t *testing.T) string {
	t.Helper()
	dataDir, err := os.MkdirTemp("", "hf-xet-data")
	if err != nil {
		t.Fatalf("create xet data dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })
	return dataDir
}

// newTestSpool opens the spool where cmd/hfd keeps it under st, ingesting into xs.
func newTestSpool(t *testing.T, st *storage.Storage, xs xetstorage.Storage) *spool.Spool {
	t.Helper()
	sp, err := spool.NewSpool(st.XETSpoolDir(), xs)
	if err != nil {
		t.Fatalf("create xet spool: %v", err)
	}
	return sp
}

// newLFSRepo creates org/repo in the storage with the given LFS pointer
// files committed on main, returning the storage and the head commit hash.
func newLFSRepo(t *testing.T, pointers map[string]string) (*storage.Storage, string) {
	t.Helper()
	st := newStorage(t, t.TempDir())
	repo, err := repository.Init(context.Background(), st.RepositoriesFS(), repository.ResolvePath("org/repo"), "main")
	if err != nil {
		t.Fatalf("init repo: %v", err)
	}
	var head string
	for file, content := range pointers {
		head, err = repo.CreateCommit(context.Background(), "main", "commit "+file, "Test", "test@test.com",
			[]repository.CommitOperation{{Type: repository.CommitOperationAdd, Path: file, Content: []byte(content)}}, "")
		if err != nil {
			t.Fatalf("create commit: %v", err)
		}
	}
	return st, head
}

func hfLFSPointerText(oid string, size int) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)
}

// TestResolveLFSRedirectsIngested pins hub parity on the resolve route:
// fully ingested files answer with metadata plus a redirect to the sha256
// bridge, and un-ingested files are 404 without an ingest engine.
func TestResolveLFSRedirectsIngested(t *testing.T) {
	ctx := context.Background()

	data := bytes.Repeat([]byte("hf resolve redirect bytes. "), 2048)
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])
	missingOID := strings.Repeat("a", 64)

	st, _ := newLFSRepo(t, map[string]string{
		"model.bin":   hfLFSPointerText(oid, len(data)),
		"missing.bin": hfLFSPointerText(missingOID, 5),
	})

	m, cas := newXETDataPlane(t, "", nil)
	if err := m.PutObject(ctx, oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("put object: %v", err)
	}

	h := NewHandler(WithStorage(st), WithMirror(m))

	t.Run("Redirect", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/org/repo/resolve/main/model.bin", nil))
		if rec.Code != http.StatusFound {
			t.Fatalf("status = %d, want 302", rec.Code)
		}
		hd := rec.Result().Header
		loc := hd.Get("Location")
		if !strings.HasSuffix(loc, "/xet-bridge/"+oid) {
			t.Fatalf("Location = %q, want suffix /xet-bridge/%s", loc, oid)
		}
		if !strings.HasPrefix(loc, "http") {
			t.Fatalf("Location = %q, want absolute URL", loc)
		}
		if got := hd.Get("X-Linked-Etag"); got != `"`+oid+`"` {
			t.Fatalf("X-Linked-Etag = %q", got)
		}
		if got := hd.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
			t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
		}
		if hd.Get("X-Repo-Commit") == "" {
			t.Fatal("X-Repo-Commit not set")
		}
		fileHash := hd.Get("X-Xet-Hash")
		wantLink := fmt.Sprintf("<http://example.com/api/models/org/repo/xet-read-token/main>; rel=\"xet-auth\", <http://example.com/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", fileHash)
		if got := hd.Get("Link"); fileHash == "" || got != wantLink {
			t.Fatalf("Link = %q, want %q", got, wantLink)
		}

		bridge := httptest.NewRecorder()
		cas.ServeHTTP(bridge, httptest.NewRequest(http.MethodGet, "/xet-bridge/"+oid, nil))
		if bridge.Code != http.StatusOK {
			t.Fatalf("bridge status = %d, want 200", bridge.Code)
		}
		if !bytes.Equal(bridge.Body.Bytes(), data) {
			t.Fatal("bridge bytes mismatch")
		}
	})

	t.Run("MissWithoutEngine", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/org/repo/resolve/main/missing.bin", nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
	})

	// huggingface_hub >= 1.30 follows same-host redirects on the metadata
	// HEAD, so the probe must carry the metadata itself instead of a 302.
	t.Run("HeadAnswersMetadata", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/org/repo/resolve/main/model.bin", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		hd := rec.Result().Header
		if hd.Get("Location") != "" {
			t.Fatalf("Location = %q, want none", hd.Get("Location"))
		}
		if hd.Get("X-Repo-Commit") == "" || hd.Get("X-Xet-Hash") == "" {
			t.Fatalf("metadata headers missing: X-Repo-Commit=%q X-Xet-Hash=%q", hd.Get("X-Repo-Commit"), hd.Get("X-Xet-Hash"))
		}
		if got := hd.Get("X-Linked-Etag"); got != `"`+oid+`"` {
			t.Fatalf("X-Linked-Etag = %q", got)
		}
		if got, want := hd.Get("X-Linked-Size"), fmt.Sprint(len(data)); got != want || hd.Get("Content-Length") != want {
			t.Fatalf("X-Linked-Size = %q, Content-Length = %q, want %s", got, hd.Get("Content-Length"), want)
		}
	})
}

// zeroSizeStorage fakes the index entry for one OID whose reconstruction is
// empty. The upload pipeline never indexes zero-entry files, so the handler's
// zero-size answer cannot be reached with bytes ingested through it.
type zeroSizeStorage struct {
	xetstorage.Storage
	digest [32]byte
}

type emptyFile struct{ *bytes.Reader }

func (emptyFile) Close() error { return nil }

func (s zeroSizeStorage) GetFileHashBySHA256(ctx context.Context, digest [32]byte) (xet.FileHash, error) {
	if digest == s.digest {
		return xet.FileHash{}, nil
	}
	return s.Storage.GetFileHashBySHA256(ctx, digest)
}

func (s zeroSizeStorage) GetReconstructedFile(ctx context.Context, digest [32]byte) (io.ReadSeekCloser, error) {
	if digest == s.digest {
		return emptyFile{bytes.NewReader(nil)}, nil
	}
	return s.Storage.GetReconstructedFile(ctx, digest)
}

// TestResolveLFSZeroSize pins the hub-parity answer for ingested zero-size
// files: 200 with an explicit zero Content-Length instead of a redirect.
func TestResolveLFSZeroSize(t *testing.T) {
	emptySum := sha256.Sum256(nil)
	emptyOID := hex.EncodeToString(emptySum[:])

	st, _ := newLFSRepo(t, map[string]string{
		"empty.bin": hfLFSPointerText(emptyOID, 0),
	})

	m, _ := newXETDataPlane(t, "", func(xs xetstorage.Storage) xetstorage.Storage {
		return zeroSizeStorage{Storage: xs, digest: emptySum}
	})

	h := NewHandler(WithStorage(st), WithMirror(m))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/org/repo/resolve/main/empty.bin", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Result().Header.Get("Content-Length"); got != "0" {
		t.Fatalf("Content-Length = %q, want 0", got)
	}
	if got := rec.Result().Header.Get("X-Linked-Size"); got != "0" {
		t.Fatalf("X-Linked-Size = %q, want 0", got)
	}
}

// TestResolveEmptyRegularFile pins that an empty blob is a regular file, not
// the EmptyPointer git-lfs decodes from zero-length input: resolve serves it
// with its Git ETag and the tree entry has no lfs block, mirror or not.
func TestResolveEmptyRegularFile(t *testing.T) {
	const emptyBlob = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"

	st, _ := newLFSRepo(t, map[string]string{"empty.txt": ""})
	m, _ := newXETDataPlane(t, "", nil)

	handlers := []struct {
		name string
		h    *Handler
	}{
		{"NoMirror", NewHandler(WithStorage(st))},
		{"Mirror", NewHandler(WithStorage(st), WithMirror(m))},
	}
	for _, tc := range handlers {
		t.Run(tc.name, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				t.Run(method, func(t *testing.T) {
					rec := httptest.NewRecorder()
					tc.h.ServeHTTP(rec, httptest.NewRequest(method, "/org/repo/resolve/main/empty.txt", nil))
					if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
						t.Fatalf("status = %d, body = %q, want 200 with empty body", rec.Code, rec.Body.String())
					}
					hd := rec.Result().Header
					if got, want := hd.Get("ETag"), `"`+emptyBlob+`"`; got != want || hd.Get("Content-Length") != "0" {
						t.Fatalf("ETag = %q, Content-Length = %q, want %s and 0", got, hd.Get("Content-Length"), want)
					}
					for _, k := range []string{"X-Linked-Etag", "X-Linked-Size", "X-Xet-Hash", "Link"} {
						if got := hd.Get(k); got != "" {
							t.Fatalf("%s = %q, want unset", k, got)
						}
					}
				})
			}

			t.Run("Tree", func(t *testing.T) {
				rec := httptest.NewRecorder()
				tc.h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/models/org/repo/tree/main", nil))
				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d, want 200", rec.Code)
				}
				var entries []treeEntry
				if err := json.Unmarshal(rec.Body.Bytes(), &entries); err != nil {
					t.Fatalf("decode tree: %v", err)
				}
				if len(entries) != 1 || entries[0].Path != "empty.txt" || entries[0].Size != 0 || entries[0].LFS != nil {
					t.Fatalf("tree = %s, want one empty.txt entry of size 0 without lfs", rec.Body.String())
				}
			})
		})
	}
}

// TestResolveLFSStreamsFromEngine pins the ingest-on-miss path: a resolve
// for a not-yet-ingested object registers it and streams the upstream bytes
// straight off the ingest engine while they land in storage.
func TestResolveLFSStreamsFromEngine(t *testing.T) {
	data := []byte("hf resolve engine streaming bytes")
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])

	st, _ := newLFSRepo(t, map[string]string{
		"model.bin": hfLFSPointerText(oid, len(data)),
	})

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/resolve/") || !strings.HasSuffix(r.URL.Path, "/model.bin") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"`+oid+`"`)
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(upstream.Close)

	m, _ := newXETDataPlane(t, upstream.URL, nil)

	h := NewHandler(WithStorage(st), WithMirror(m))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/org/repo/resolve/main/model.bin", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("body = %q, want upstream bytes", rec.Body.String())
	}
	if got := rec.Result().Header.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
		t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
	}
}

// TestTreeHubShape pins the huggingface.co tree listing hf-mount mounts from:
// directories are entries of their own, no slot is ever blank, and ingested
// LFS files carry their xetHash.
func TestTreeHubShape(t *testing.T) {
	data := []byte("hf tree model bytes")
	sum := sha256.Sum256(data)
	modelOID := hex.EncodeToString(sum[:])
	missingOID := strings.Repeat("b", 64)
	const missingSize = 7

	// a.txt sorts before directory a in git tree order, but after it in a plain string sort.
	files := map[string]string{
		"top.txt":     "hello",
		"a.txt":       "a",
		"a/b.txt":     "b",
		"a/c/d.txt":   "d",
		"model.bin":   hfLFSPointerText(modelOID, len(data)),
		"missing.bin": hfLFSPointerText(missingOID, missingSize),
	}
	st, _ := newLFSRepo(t, files)
	m, _ := newXETDataPlane(t, "", nil)
	if err := m.PutObject(context.Background(), modelOID, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("put object: %v", err)
	}

	const root = "/api/models/org/repo/tree/main"
	noMirror := NewHandler(WithStorage(st))
	withMirror := NewHandler(WithStorage(st), WithMirror(m))
	handlers := []struct {
		name string
		h    *Handler
	}{
		{"NoMirror", noMirror},
		{"Mirror", withMirror},
	}
	for _, tc := range handlers {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("RootListsDirectory", func(t *testing.T) {
				const behavior = "root listing has directory a in tree order, after file a.txt"
				entries, body := hubTree(t, tc.h, root)
				requireTreePaths(t, behavior, body, entries, "a.txt", "a", "missing.bin", "model.bin", "top.txt")
				checkHubDirectory(t, behavior, body, entries[1])
			})

			t.Run("SubdirectoryListsDirectory", func(t *testing.T) {
				const behavior = "tree/main/a lists file a/b.txt and directory a/c"
				entries, body := hubTree(t, tc.h, root+"/a")
				requireTreePaths(t, behavior, body, entries, "a/b.txt", "a/c")
				if entries[0]["type"] != "file" {
					t.Errorf("%s: a/b.txt type = %v, want file\nbody: %s", behavior, entries[0]["type"], body)
				}
				checkHubDirectory(t, behavior, body, entries[1])
			})

			// Regression guard: passes today, as a/c holds no subdirectory to blank.
			t.Run("NestedSubdirectoryListsFile", func(t *testing.T) {
				const behavior = "tree/main/a/c lists file a/c/d.txt"
				entries, body := hubTree(t, tc.h, root+"/a/c")
				requireTreePaths(t, behavior, body, entries, "a/c/d.txt")
				if entries[0]["type"] != "file" {
					t.Errorf("%s: a/c/d.txt type = %v, want file\nbody: %s", behavior, entries[0]["type"], body)
				}
			})

			t.Run("RecursiveListsDirectories", func(t *testing.T) {
				const behavior = "recursive listing has directories a and a/c in tree order"
				entries, body := hubTree(t, tc.h, root+"?recursive=true")
				requireTreePaths(t, behavior, body, entries, "a.txt", "a", "a/b.txt", "a/c", "a/c/d.txt", "missing.bin", "model.bin", "top.txt")
				checkHubDirectory(t, behavior, body, entries[1])
				checkHubDirectory(t, behavior, body, entries[3])
			})

			t.Run("NoBlankEntries", func(t *testing.T) {
				const behavior = "no listing has a blank entry"
				for _, target := range []string{root, root + "/a", root + "/a/c", root + "?recursive=true"} {
					entries, body := hubTree(t, tc.h, target)
					checkNoBlankEntries(t, behavior, target, body, entries)
				}
			})

			t.Run("ExpandLastCommit", func(t *testing.T) {
				const behavior = "expand=true gives every root entry, directory a included, a lastCommit"
				entries, body := hubTree(t, tc.h, root+"?expand=true")
				requireTreePaths(t, behavior, body, entries, "a.txt", "a", "missing.bin", "model.bin", "top.txt")
				for _, e := range entries {
					lc, _ := e["lastCommit"].(map[string]any)
					id, _ := lc["id"].(string)
					title, _ := lc["title"].(string)
					date, _ := lc["date"].(string)
					if _, err := time.Parse(repository.TimeFormat, date); id == "" || title == "" || err != nil {
						t.Errorf("%s: %v lastCommit = %v, want id, title and a %s date\nbody: %s", behavior, e["path"], e["lastCommit"], repository.TimeFormat, body)
					}
				}
				plain, plainBody := hubTree(t, tc.h, root)
				for _, e := range plain {
					if _, ok := e["lastCommit"]; ok {
						t.Errorf("%s: %v has lastCommit without expand\nbody: %s", behavior, e["path"], plainBody)
					}
				}
			})

			t.Run("PlainFile", func(t *testing.T) {
				const behavior = "plain file top.txt has its byte size and no lfs or xetHash"
				entries, body := hubTree(t, tc.h, root)
				e := treeEntryAt(t, behavior, body, entries, "top.txt")
				if e["type"] != "file" || e["size"] != float64(len("hello")) {
					t.Errorf("%s: top.txt = %v, want type file and size 5\nbody: %s", behavior, e, body)
				}
				for _, k := range []string{"lfs", "xetHash"} {
					if _, ok := e[k]; ok {
						t.Errorf("%s: top.txt carries %q\nbody: %s", behavior, k, body)
					}
				}
			})

			t.Run("LFSPointerFiles", func(t *testing.T) {
				const behavior = "pointer files carry lfs and the pointed-to size"
				entries, body := hubTree(t, tc.h, root)
				for _, want := range []struct {
					path string
					oid  string
					size int
				}{
					{"model.bin", modelOID, len(data)},
					{"missing.bin", missingOID, missingSize},
				} {
					e := treeEntryAt(t, behavior, body, entries, want.path)
					block, _ := e["lfs"].(map[string]any)
					pointerSize := len(hfLFSPointerText(want.oid, want.size))
					if e["type"] != "file" || e["size"] != float64(want.size) ||
						block["oid"] != want.oid || block["size"] != float64(want.size) || block["pointerSize"] != float64(pointerSize) {
						t.Errorf("%s: %s = %v, want size %d and lfs {oid %s, size %d, pointerSize %d}\nbody: %s",
							behavior, want.path, e, want.size, want.oid, want.size, pointerSize, body)
					}
				}
			})
		})
	}

	listings := []string{root, root + "?recursive=true"}

	t.Run("IngestedLFSCarriesXetHash", func(t *testing.T) {
		const behavior = "with a mirror, ingested LFS file model.bin carries its xetHash"
		want := m.FileHash(context.Background(), modelOID)
		if want == "" {
			t.Fatalf("%s: mirror has no file hash for ingested model.bin", behavior)
		}
		head := httptest.NewRecorder()
		withMirror.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/org/repo/resolve/main/model.bin", nil))
		if got := strings.TrimSpace(head.Result().Header.Get("X-Xet-Hash")); head.Code != http.StatusOK || got != want {
			t.Fatalf("%s: HEAD resolve model.bin = %d with X-Xet-Hash %q, want 200 with the mirror's %q", behavior, head.Code, got, want)
		}
		for _, target := range listings {
			entries, body := hubTree(t, withMirror, target)
			e := treeEntryAt(t, behavior, body, entries, "model.bin")
			raw, ok := e["xetHash"]
			got, _ := raw.(string)
			switch {
			case !ok:
				t.Errorf("%s: GET %s model.bin has no xetHash, want %q\nbody: %s", behavior, target, want, body)
			case len(got) != 64 || strings.Trim(got, "0123456789abcdef") != "":
				t.Errorf("%s: GET %s model.bin xetHash = %#v, want 64 lowercase hex\nbody: %s", behavior, target, raw, body)
			case got != want:
				t.Errorf("%s: GET %s model.bin xetHash = %q, want %q as in X-Xet-Hash\nbody: %s", behavior, target, got, want, body)
			}
			block, _ := e["lfs"].(map[string]any)
			if e["size"] != float64(len(data)) || block["size"] != float64(len(data)) {
				t.Errorf("%s: GET %s model.bin size = %v and lfs.size = %v, want both %d\nbody: %s", behavior, target, e["size"], block["size"], len(data), body)
			}
		}
	})

	// Regression guards: pass today and pin where xetHash stays absent.
	t.Run("NoMirrorOmitsXetHash", func(t *testing.T) {
		const behavior = "without a mirror, LFS file model.bin has lfs but no xetHash"
		for _, target := range listings {
			entries, body := hubTree(t, noMirror, target)
			checkLFSWithoutXetHash(t, behavior, target, body, treeEntryAt(t, behavior, body, entries, "model.bin"))
		}
	})

	t.Run("UningestedLFSOmitsXetHash", func(t *testing.T) {
		const behavior = "un-ingested LFS file missing.bin has lfs but no xetHash"
		for _, tc := range handlers {
			t.Run(tc.name, func(t *testing.T) {
				for _, target := range listings {
					entries, body := hubTree(t, tc.h, target)
					checkLFSWithoutXetHash(t, behavior, target, body, treeEntryAt(t, behavior, body, entries, "missing.bin"))
				}
			})
		}
	})

	t.Run("NonLFSEntriesOmitXetHash", func(t *testing.T) {
		const behavior = "with a mirror, plain file top.txt and directories a and a/c carry no xetHash"
		const target = root + "?recursive=true"
		entries, body := hubTree(t, withMirror, target)
		for _, path := range []string{"top.txt", "a", "a/c"} {
			if v, ok := treeEntryAt(t, behavior, body, entries, path)["xetHash"]; ok {
				t.Errorf("%s: GET %s %s carries xetHash %#v\nbody: %s", behavior, target, path, v, body)
			}
		}
	})

	t.Run("UnreadableBlobOmitted", func(t *testing.T) {
		const behavior = "top.txt, whose blob cannot be read, is left out instead of leaving a blank slot"
		// A repository of its own: the shared one must keep top.txt readable.
		st, _ := newLFSRepo(t, files)
		const helloBlob = "b6fc4c620b67d95f953a5c1c1230aaab5db5a1b0" // git blob id of "hello"
		loose := filepath.Join(repository.ResolvePath("org/repo"), "objects", helloBlob[:2], helloBlob[2:])
		if err := st.RepositoriesFS().Remove(loose); err != nil {
			t.Fatalf("remove the loose blob of top.txt: %v", err)
		}
		h := NewHandler(WithStorage(st))
		for _, want := range []struct {
			name, target string
			paths        []string
		}{
			{"Root", root, []string{"a.txt", "a", "missing.bin", "model.bin"}},
			{"Subdirectory", root + "/a", []string{"a/b.txt", "a/c"}},
			{"Recursive", root + "?recursive=true", []string{"a.txt", "a", "a/b.txt", "a/c", "a/c/d.txt", "missing.bin", "model.bin"}},
		} {
			t.Run(want.name, func(t *testing.T) {
				entries, body := hubTree(t, h, want.target)
				checkNoBlankEntries(t, behavior, want.target, body, entries)
				requireTreePaths(t, behavior, body, entries, want.paths...)
			})
		}
	})
}

// hubTree GETs a tree route and decodes the raw JSON, so absent keys stay absent.
func hubTree(t *testing.T, h *Handler, target string) ([]map[string]any, string) {
	t.Helper()
	rec := serveGet(h, target)
	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200\nbody: %s", target, rec.Code, body)
	}
	var entries []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &entries); err != nil {
		t.Fatalf("GET %s: decode tree: %v\nbody: %s", target, err, body)
	}
	return entries, body
}

// requireTreePaths stops the test unless the listing holds exactly want, in order.
func requireTreePaths(t *testing.T, behavior, body string, entries []map[string]any, want ...string) {
	t.Helper()
	got := make([]string, len(entries))
	for i, e := range entries {
		got[i], _ = e["path"].(string)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s: paths = %q, want %q\nbody: %s", behavior, got, want, body)
	}
}

// checkNoBlankEntries reports each entry listed at target with an empty path, type or oid.
func checkNoBlankEntries(t *testing.T, behavior, target, body string, entries []map[string]any) {
	t.Helper()
	for i, e := range entries {
		for _, k := range []string{"path", "type", "oid"} {
			if s, _ := e[k].(string); s == "" {
				t.Errorf("%s: GET %s entry %d has an empty %s: %v\nbody: %s", behavior, target, i, k, e, body)
				break
			}
		}
	}
}

// checkHubDirectory reports how an unexpanded entry departs from a hub directory.
func checkHubDirectory(t *testing.T, behavior, body string, e map[string]any) {
	t.Helper()
	oid, _ := e["oid"].(string)
	if e["type"] != "directory" || e["size"] != float64(0) || len(oid) != 40 || strings.Trim(oid, "0123456789abcdef") != "" {
		t.Errorf("%s: %v = %v, want type directory, size 0 and a 40-hex oid\nbody: %s", behavior, e["path"], e, body)
	}
	for _, k := range []string{"lfs", "xetHash", "lastCommit"} {
		if _, ok := e[k]; ok {
			t.Errorf("%s: directory %v carries %q\nbody: %s", behavior, e["path"], k, body)
		}
	}
}

// checkLFSWithoutXetHash reports an entry listed at target that lacks lfs or carries xetHash.
func checkLFSWithoutXetHash(t *testing.T, behavior, target, body string, e map[string]any) {
	t.Helper()
	if _, ok := e["lfs"].(map[string]any); !ok {
		t.Errorf("%s: GET %s %v has no lfs block\nbody: %s", behavior, target, e["path"], body)
	}
	if v, ok := e["xetHash"]; ok {
		t.Errorf("%s: GET %s %v carries xetHash %#v\nbody: %s", behavior, target, e["path"], v, body)
	}
}

// treeEntryAt returns the entry listed at path, stopping the test when there is none.
func treeEntryAt(t *testing.T, behavior, body string, entries []map[string]any, path string) map[string]any {
	t.Helper()
	for _, e := range entries {
		if e["path"] == path {
			return e
		}
	}
	t.Fatalf("%s: no entry at %s\nbody: %s", behavior, path, body)
	return nil
}

// TestResolveRegularFile pins resolve's wire answers for a regular file go-git
// holds in memory and one above repository.LargeObjectThreshold streamed from disk.
func TestResolveRegularFile(t *testing.T) {
	small := patternBytes(4096)
	weights := patternBytes(repository.LargeObjectThreshold + 1)
	st, head := newLFSRepo(t, map[string]string{"small.json": string(small), "weights": string(weights)})
	srv := httptest.NewServer(NewHandler(WithStorage(st)))
	t.Cleanup(srv.Close)
	client := srv.Client()

	files := []struct {
		name        string
		content     []byte
		contentType string // empty leaves a sniffed type unpinned
	}{
		{"small.json", small, "application/json"},
		{"weights", weights, ""},
	}
	for _, f := range files {
		t.Run(f.name, func(t *testing.T) {
			target := srv.URL + "/org/repo/resolve/main/" + f.name
			size := len(f.content)
			etag := `"` + gitBlobID(f.content) + `"`

			t.Run("GET", func(t *testing.T) {
				resp, body := fetch(t, client, http.MethodGet, target, nil)
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("status = %d, want 200", resp.StatusCode)
				}
				if !bytes.Equal(body, f.content) {
					t.Errorf("body differs from the %d committed bytes (got %d bytes)", size, len(body))
				}
				checkHeader(t, resp.Header, "Content-Length", fmt.Sprint(size))
				checkHeader(t, resp.Header, "ETag", etag)
				checkHeader(t, resp.Header, "X-Repo-Commit", head)
				cd := resp.Header.Get("Content-Disposition")
				if _, params, err := mime.ParseMediaType(cd); err != nil || params["filename"] != f.name {
					t.Errorf("Content-Disposition = %q, want filename %q", cd, f.name)
				}
				if resp.Header.Get("Last-Modified") == "" {
					t.Error("Last-Modified not set")
				}
			})

			t.Run("HEAD", func(t *testing.T) {
				resp, body := fetch(t, client, http.MethodHead, target, nil)
				if resp.StatusCode != http.StatusOK || len(body) != 0 {
					t.Fatalf("status = %d with %d body bytes, want 200 with none", resp.StatusCode, len(body))
				}
				checkHeader(t, resp.Header, "Content-Length", fmt.Sprint(size))
				checkHeader(t, resp.Header, "ETag", etag)
				checkHeader(t, resp.Header, "X-Repo-Commit", head)
			})

			t.Run("AcceptRanges", func(t *testing.T) {
				for _, method := range []string{http.MethodGet, http.MethodHead} {
					t.Run(method, func(t *testing.T) {
						resp, _ := fetch(t, client, method, target, nil)
						checkHeader(t, resp.Header, "Accept-Ranges", "bytes")
					})
				}
			})

			for _, rg := range []struct {
				name, spec  string
				first, last int // inclusive offsets served
			}{
				{"RangeBounded", "bytes=100-199", 100, 199},
				{"RangeOpenEnded", fmt.Sprintf("bytes=%d-", size-300), size - 300, size - 1},
				{"RangeSuffix", "bytes=-100", size - 100, size - 1},
			} {
				t.Run(rg.name, func(t *testing.T) {
					resp, body := fetch(t, client, http.MethodGet, target, http.Header{"Range": {rg.spec}})
					if resp.StatusCode != http.StatusPartialContent {
						t.Fatalf("Range %s: status = %d, want 206", rg.spec, resp.StatusCode)
					}
					checkHeader(t, resp.Header, "Content-Range", fmt.Sprintf("bytes %d-%d/%d", rg.first, rg.last, size))
					if !bytes.Equal(body, f.content[rg.first:rg.last+1]) {
						t.Errorf("Range %s: body differs from bytes %d-%d (got %d bytes)", rg.spec, rg.first, rg.last, len(body))
					}
				})
			}

			t.Run("RangeMultiOutOfOrder", func(t *testing.T) {
				// The second span precedes the first, so a forward-only stream must reopen.
				spans := [][2]int{{size - 200, size - 101}, {0, 99}}
				spec := fmt.Sprintf("bytes=%d-%d,%d-%d", spans[0][0], spans[0][1], spans[1][0], spans[1][1])
				resp, body := fetch(t, client, http.MethodGet, target, http.Header{"Range": {spec}})
				ct := resp.Header.Get("Content-Type")
				mediaType, params, err := mime.ParseMediaType(ct)
				if resp.StatusCode != http.StatusPartialContent || err != nil || mediaType != "multipart/byteranges" {
					t.Fatalf("Range %s: status = %d, Content-Type = %q, want 206 multipart/byteranges", spec, resp.StatusCode, ct)
				}
				mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
				for i, s := range spans {
					part, err := mr.NextPart()
					if err != nil {
						t.Fatalf("Range %s: part %d: %v", spec, i, err)
					}
					if got, want := part.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", s[0], s[1], size); got != want {
						t.Errorf("Range %s: part %d Content-Range = %q, want %q", spec, i, got, want)
					}
					data, err := io.ReadAll(part)
					if err != nil {
						t.Fatalf("Range %s: read part %d: %v", spec, i, err)
					}
					if !bytes.Equal(data, f.content[s[0]:s[1]+1]) {
						t.Errorf("Range %s: part %d body differs from bytes %d-%d (got %d bytes)", spec, i, s[0], s[1], len(data))
					}
				}
				if _, err := mr.NextPart(); err != io.EOF {
					t.Errorf("Range %s: NextPart after %d parts = %v, want io.EOF", spec, len(spans), err)
				}
			})

			t.Run("RangeNotSatisfiable", func(t *testing.T) {
				spec := fmt.Sprintf("bytes=%d-", size)
				resp, _ := fetch(t, client, http.MethodGet, target, http.Header{"Range": {spec}})
				if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
					t.Fatalf("Range %s: status = %d, want 416", spec, resp.StatusCode)
				}
				checkHeader(t, resp.Header, "Content-Range", fmt.Sprintf("bytes */%d", size))
			})

			t.Run("IfNoneMatch", func(t *testing.T) {
				resp, body := fetch(t, client, http.MethodGet, target, http.Header{"If-None-Match": {etag}})
				if resp.StatusCode != http.StatusNotModified || len(body) != 0 {
					t.Fatalf("If-None-Match %s: status = %d with %d body bytes, want 304 with none", etag, resp.StatusCode, len(body))
				}
			})

			if f.contentType != "" {
				t.Run("ContentType", func(t *testing.T) {
					resp, _ := fetch(t, client, http.MethodGet, target, nil)
					checkHeader(t, resp.Header, "Content-Type", f.contentType)
				})
			}
		})
	}
}

// patternBytes returns n deterministic bytes that do not sniff as text.
func patternBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 3)
	}
	return b
}

// gitBlobID returns the SHA-1 Git object id of a blob holding content.
func gitBlobID(content []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(content))
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

// fetch sends method to target with header over client and returns the response and its whole body.
func fetch(t *testing.T, client *http.Client, method, target string, header http.Header) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, target, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", method, target, err)
	}
	return resp, body
}

// checkHeader reports a header whose value is not want.
func checkHeader(t *testing.T, h http.Header, key, want string) {
	t.Helper()
	if got := h.Get(key); got != want {
		t.Errorf("%s = %q, want %q", key, got, want)
	}
}
