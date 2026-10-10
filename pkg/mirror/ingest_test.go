package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	xetclient "github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/mirror/spool"

	"github.com/matrixhub-ai/hfd/pkg/storage"
)

const ingestSize = 96 << 10

// newTestSpool opens the engine's spool where cmd/hfd keeps it under st.
func newTestSpool(t *testing.T, st *storage.Storage) *spool.Spool {
	t.Helper()
	sp, err := spool.NewSpool(filepath.Join(st.XETDir(), "mirror", "spool"), st.XETStorage())
	if err != nil {
		t.Fatalf("create xet spool: %v", err)
	}
	return sp
}

func newDataDirMirror(t *testing.T, dataDir string) *Mirror {
	t.Helper()
	st, err := storage.NewStorage(storage.WithRootDir(t.TempDir()))
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	m, err := NewMirror(WithXETStorage(st.XETStorage()), WithXETCache(xetclient.NewCache(filepath.Join(st.XETDir(), "chunks"), 0, 0)), WithDataDir(dataDir))
	if err != nil {
		t.Fatalf("new mirror: %v", err)
	}
	return m
}

// newIngestMirror returns an engine-less mirror over fresh storage and the directory its uploads are staged in.
func newIngestMirror(t *testing.T) (*Mirror, string) {
	t.Helper()
	dataDir := t.TempDir()
	return newDataDirMirror(t, dataDir), filepath.Join(dataDir, "uploads")
}

func randomContent(t *testing.T, n int) ([]byte, string) {
	t.Helper()
	data := make([]byte, n)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("random content: %v", err)
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:])
}

// bodyReader counts the bytes read through it and runs atEOF once drained.
type bodyReader struct {
	r     io.Reader
	n     int64
	atEOF func()
}

func (b *bodyReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	if err == io.EOF && b.atEOF != nil {
		b.atEOF()
		b.atEOF = nil
	}
	return n, err
}

func uploadFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, uploadPattern))
	if err != nil {
		t.Fatalf("list upload files: %v", err)
	}
	return files
}

func assertObject(t *testing.T, m *Mirror, oid string, data []byte) {
	t.Helper()
	ctx := context.Background()
	if !m.HasObject(ctx, oid) {
		t.Fatalf("HasObject(%s) = false, want the put object", oid)
	}
	rs, size, err := m.OpenObject(ctx, oid)
	if err != nil {
		t.Fatalf("open object: %v", err)
	}
	defer func() { _ = rs.Close() }()
	got, err := io.ReadAll(rs)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if size != int64(len(data)) || !bytes.Equal(got, data) {
		t.Fatalf("object = %d bytes (size %d), want the %d put bytes", len(got), size, len(data))
	}
}

// startGatedPut starts a put of data whose body stalls after its first half,
// returning once that half was read; finish delivers the rest.
func startGatedPut(t *testing.T, ctx context.Context, m *Mirror, oid string, data []byte) (finish func(), done <-chan error) {
	t.Helper()
	pr, pw := io.Pipe()
	ch := make(chan error, 1)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		err := m.PutObject(ctx, oid, pr, int64(len(data)))
		_ = pr.Close() // unblocks the feeder if the put ended early
		ch <- err
	}()
	// A failed test must not leave the put running into the temp dir's removal.
	t.Cleanup(func() {
		_ = pw.CloseWithError(errors.New("test ended"))
		<-returned
	})
	half := len(data) / 2
	if _, err := pw.Write(data[:half]); err != nil {
		t.Fatalf("gated put ended before its first half was read: %v", <-ch)
	}
	return func() {
		_, _ = pw.Write(data[half:])
		_ = pw.Close()
	}, ch
}

func TestPutObject(t *testing.T) {
	data, oid := randomContent(t, ingestSize)
	other, _ := randomContent(t, ingestSize)
	for _, tc := range []struct {
		name    string
		oid     string
		body    []byte
		size    int64
		wantErr string // "" for success
	}{
		{name: "RoundTrip", oid: oid, body: data, size: int64(len(data))},
		{name: "UnknownSize", oid: oid, body: data, size: -1},
		{name: "UppercaseOID", oid: strings.ToUpper(oid), body: data, size: int64(len(data))},
		{name: "SHA256Mismatch", oid: oid, body: other, size: int64(len(other)), wantErr: "content hash does not match"},
		{name: "SizeMismatch", oid: oid, body: data, size: int64(len(data)) + 1, wantErr: "content size does not match"},
		{name: "InvalidOID", oid: oid[:63], body: data, size: int64(len(data)), wantErr: "invalid OID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := newIngestMirror(t)
			err := m.PutObject(context.Background(), tc.oid, bytes.NewReader(tc.body), tc.size)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("PutObject = %v, want nil", err)
				}
				assertObject(t, m, oid, data)
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("PutObject = %v, want an error containing %q", err, tc.wantErr)
			}
			if m.HasObject(context.Background(), oid) {
				t.Fatalf("HasObject(%s) = true after a failed put", oid)
			}
		})
	}
}

// TestPutObjectLeavesNoFile pins that a put stages in exactly one file of its
// own and removes it before returning, whether it landed or was rejected.
func TestPutObjectLeavesNoFile(t *testing.T) {
	m, dir := newIngestMirror(t)
	data, oid := randomContent(t, ingestSize)
	other, _ := randomContent(t, ingestSize)
	for _, tc := range []struct {
		name    string
		body    []byte
		wantErr bool
	}{
		{name: "Landed", body: data},
		{name: "Corrupt", body: other, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var staged []string
			body := &bodyReader{r: bytes.NewReader(tc.body), atEOF: func() { staged = uploadFiles(t, dir) }}
			if err := m.PutObject(context.Background(), oid, body, int64(len(tc.body))); (err != nil) != tc.wantErr {
				t.Fatalf("PutObject = %v, want error %v", err, tc.wantErr)
			}
			if len(staged) != 1 {
				t.Fatalf("upload files once the body drained = %v, want the put's own", staged)
			}
			if left := uploadFiles(t, dir); len(left) != 0 {
				t.Fatalf("upload files left behind: %v", left)
			}
		})
	}
}

// TestPutObjectCanceled pins that the request context bounds a put: a
// canceled put stops reading, stores nothing and removes its staging file.
func TestPutObjectCanceled(t *testing.T) {
	t.Run("Before", func(t *testing.T) {
		m, dir := newIngestMirror(t)
		data, oid := randomContent(t, ingestSize)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		body := &bodyReader{r: bytes.NewReader(data)}
		if err := m.PutObject(ctx, oid, body, int64(len(data))); !errors.Is(err, context.Canceled) {
			t.Fatalf("PutObject with a canceled context = %v, want context.Canceled", err)
		}
		if body.n != 0 {
			t.Fatalf("PutObject read %d body bytes with a canceled context, want none", body.n)
		}
		if m.HasObject(context.Background(), oid) {
			t.Fatal("HasObject = true after a canceled put")
		}
		if left := uploadFiles(t, dir); len(left) != 0 {
			t.Fatalf("upload files left behind: %v", left)
		}
	})

	t.Run("Stalled", func(t *testing.T) {
		m, dir := newIngestMirror(t)
		data, oid := randomContent(t, ingestSize)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, done := startGatedPut(t, ctx, m, oid, data)
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled put = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("canceled put kept waiting for its stalled body")
		}
		if m.HasObject(context.Background(), oid) {
			t.Fatal("HasObject = true after a canceled put")
		}
		if left := uploadFiles(t, dir); len(left) != 0 {
			t.Fatalf("upload files left behind: %v", left)
		}
	})
}

// TestPutObjectConcurrentSameOID pins that concurrent puts of one object are
// independent: each stages its own file and both land.
func TestPutObjectConcurrentSameOID(t *testing.T) {
	m, dir := newIngestMirror(t)
	data, oid := randomContent(t, ingestSize)
	finishFirst, first := startGatedPut(t, context.Background(), m, oid, data)
	finishSecond, second := startGatedPut(t, context.Background(), m, oid, data)

	if staged := uploadFiles(t, dir); len(staged) < 2 {
		t.Fatalf("upload files while both puts are open = %v, want one per put", staged)
	}
	finishFirst()
	finishSecond()
	if err := <-first; err != nil {
		t.Fatalf("first PutObject = %v, want nil", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second PutObject = %v, want nil", err)
	}
	assertObject(t, m, oid, data)
	if left := uploadFiles(t, dir); len(left) != 0 {
		t.Fatalf("upload files left behind: %v", left)
	}
}

// TestNewMirrorRemovesStaleUploads pins the crash cleanup: staging files of
// interrupted uploads go, anything else in the directory stays, and a data
// directory named with glob metacharacters is still matched literally.
func TestNewMirrorRemovesStaleUploads(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data[1]")
	dir := filepath.Join(dataDir, "uploads")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create upload dir: %v", err)
	}
	stale := strings.TrimSuffix(uploadPattern, "*") + "stale"
	for _, name := range []string{stale, "other.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	newDataDirMirror(t, dataDir)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read upload dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 1 || names[0] != "other.txt" {
		t.Fatalf("upload dir after NewMirror = %v, want only other.txt", names)
	}
}
