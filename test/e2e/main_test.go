package e2e_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	s3fs "github.com/wzshiming/go-billy-s3fs"

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

// mountDataPlane splits the xet data-plane routes to the mirror and waits
// out its background work on cleanup, the same way cmd/hfd wires it.
func mountDataPlane(t *testing.T, m *mirror.Mirror, next http.Handler) http.Handler {
	t.Helper()
	t.Cleanup(m.Wait)
	dp := m.DataPlane()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mirror.IsDataPlanePath(r.URL.Path) {
			dp.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
