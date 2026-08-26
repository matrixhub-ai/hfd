package e2e_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	s3fs "github.com/wzshiming/go-billy-s3fs"
	xetclient "github.com/wzshiming/xet/client"
	xetmirror "github.com/wzshiming/xet/mirror"
	xetserver "github.com/wzshiming/xet/server"
	xetstorage "github.com/wzshiming/xet/storage"

	backendcas "github.com/matrixhub-ai/hfd/pkg/backend/cas"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

const testS3Bucket = "hfd-e2e"

// testS3Client talks to an in-process fake S3 server during the S3 pass;
// storages then keep repositories and LFS objects in the bucket, mirroring
// the production S3 storage mode. It is nil during the local pass.
var testS3Client *s3.Client

// TestMain runs the whole suite twice: once with storage on the local
// filesystem and once with storage in S3.
func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	fmt.Println("=== e2e pass 1/2: local storage ===")
	if code := m.Run(); code != 0 {
		return code
	}

	backend := s3mem.New()
	if err := backend.CreateBucket(testS3Bucket); err != nil {
		fmt.Fprintln(os.Stderr, "create S3 bucket:", err)
		return 1
	}
	fakeS3 := httptest.NewServer(gofakes3.New(backend).Server())
	defer fakeS3.Close()

	testS3Client = s3.New(s3.Options{
		Region:                     "us-east-1",
		BaseEndpoint:               aws.String(fakeS3.URL),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider("test-access-key", "test-secret-key", ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})

	fmt.Println("=== e2e pass 2/2: S3 storage ===")
	return m.Run()
}

// testEnv strips host proxy/ssh overrides that must not leak into client subprocesses.
// It also pins git-lfs to the basic transfer, so custom transfer adapters from the
// host gitconfig (e.g. lfs.customtransfer.xet) cannot hijack test uploads.
func testEnv() []string {
	hostEnv := os.Environ()
	env := make([]string, 0, len(hostEnv)+5)
	for _, kv := range hostEnv {
		name, _, _ := strings.Cut(kv, "=")
		switch strings.ToUpper(name) {
		case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "GIT_SSH_COMMAND":
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"GIT_TERMINAL_PROMPT=0",
		"NO_PROXY=*",
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=lfs.basictransfersonly",
		"GIT_CONFIG_VALUE_0=true",
	)
}

// newDataDir returns a fresh server data directory on the host.
func newDataDir(t *testing.T, pattern string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", pattern)
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// newTestStorage returns a Storage rooted at dataDir. During the S3 pass the
// storage filesystem lives in the fake S3 bucket under a per-directory unique
// prefix, handing out presigned URLs like production.
func newTestStorage(t *testing.T, dataDir string) *storage.Storage {
	t.Helper()
	opts := []storage.Option{storage.WithRootDir(dataDir)}
	if testS3Client != nil {
		opts = append(opts, storage.WithFilesystem(
			s3fs.New(testS3Bucket,
				s3fs.WithClient(testS3Client),
				s3fs.WithPresignClient(s3.NewPresignClient(testS3Client)),
				s3fs.WithPrefix(filepath.Base(dataDir)))))
	}
	return storage.NewStorage(opts...)
}

// newTestMirror assembles the xet data-plane pieces the way cmd/hfd does —
// client, storage, token scheme, and the CAS-server composition — under
// dataDir/xet and builds the shared mirror over them with gitOpts appended,
// returning the mirror and the chain tail: the write-token backend over the
// composition. upstreamURL enables the
// xet mirror handler; s3Storage puts the xet storage in the fake S3 bucket
// like production. Background work is waited out on cleanup.
func newTestMirror(t *testing.T, dataDir, upstreamURL string, s3Storage bool, gitOpts ...mirror.Option) (*mirror.Mirror, http.Handler) {
	t.Helper()
	xetDir := filepath.Join(dataDir, "xet")
	chunksDir := filepath.Join(xetDir, "chunks")
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		t.Fatalf("create xet chunk cache dir: %v", err)
	}
	client, err := xetclient.NewClient(xetclient.WithCacheDir(chunksDir))
	if err != nil {
		t.Fatalf("create xet client: %v", err)
	}
	var xs xetstorage.Storage
	if s3Storage {
		xs, err = xetstorage.NewS3Storage(t.Context(),
			xetstorage.WithS3Client(testS3Client),
			xetstorage.WithS3Bucket(testS3Bucket),
			xetstorage.WithS3Prefix(filepath.Base(dataDir)+"/xet"),
		)
	} else {
		xs, err = xetstorage.NewFileStorage(
			xetstorage.WithBasePath(filepath.Join(xetDir, "storage")),
		)
	}
	if err != nil {
		t.Fatalf("create xet storage: %v", err)
	}
	mint, authFn, err := mirror.NewXETTokenScheme(nil)
	if err != nil {
		t.Fatalf("create token scheme: %v", err)
	}
	casNext := http.Handler(http.NotFoundHandler())
	var mirrorHandler *xetmirror.Handler
	if upstreamURL != "" {
		mirrorHandler, err = xetmirror.NewHandler(
			xetmirror.WithStorage(xs),
			xetmirror.WithUpstream(upstreamURL),
			xetmirror.WithCacheDir(filepath.Join(xetDir, "mirror")),
			xetmirror.WithClient(client),
			xetmirror.WithMintToken(mint),
			xetmirror.WithNext(http.NotFoundHandler()),
		)
		if err != nil {
			t.Fatalf("create xet mirror handler: %v", err)
		}
		casNext = mirrorHandler
	}
	dataPlane := xetserver.NewHandler(
		xetserver.WithStorage(xs),
		xetserver.WithAuthFunc(authFn),
		xetserver.WithNext(casNext),
	)
	opts := []mirror.Option{
		mirror.WithXETStorage(xs),
		mirror.WithXETClient(client),
		mirror.WithMirrorHandler(mirrorHandler),
		mirror.WithMintToken(mint),
		mirror.WithDataDir(xetDir),
	}
	m, err := mirror.NewMirror(append(opts, gitOpts...)...)
	if err != nil {
		t.Fatalf("create mirror: %v", err)
	}
	t.Cleanup(m.Wait)
	// The write-token backend fronts the composition the way cmd/hfd
	// mounts it, so upload clients can mint CAS credentials.
	tail := backendcas.NewHandler(
		backendcas.WithMirror(m),
		backendcas.WithNext(dataPlane),
	)
	return m, tail
}
