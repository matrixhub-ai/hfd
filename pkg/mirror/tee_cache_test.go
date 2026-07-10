package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/wzshiming/ioswmr"
)

func TestDoDownload_FallbackToBasicWhenXETFails(t *testing.T) {
	t.Parallel()

	data := []byte("hello from fallback")
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(data)
	}))
	defer server.Close()

	cache := newTeeCache(
		lfs.NewLocal(filepath.Join(t.TempDir(), "objects")),
		1,
		true,
		false,
		filepath.Join(t.TempDir(), "cache"),
		0,
		nil,
		nil,
	)
	cache.xetClient = nil
	cache.xetClientErr = errors.New("auth request failed with status 401")

	action := lfs.Action{Href: server.URL + "/" + oid}
	swmr := ioswmr.NewSWMR(nil)
	writer := swmr.Writer()

	err := cache.doDownload(context.Background(), "https://huggingface.co/org/repo", oid, int64(len(data)), action, writer)
	if err != nil {
		t.Fatalf("doDownload returned error: %v", err)
	}
	_ = writer.Close()

	reader := swmr.NewReadSeeker(0, len(data))
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read downloaded data: %v", err)
	}

	if string(got) != string(data) {
		t.Fatalf("unexpected data, got %q want %q", string(got), string(data))
	}
}
