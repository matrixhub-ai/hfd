package lfs_test

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	s3fs "github.com/wzshiming/go-billy-s3fs"

	"github.com/matrixhub-ai/hfd/pkg/lfs"
)

// newFakeS3FS returns an S3-backed filesystem with presigning, talking to an
// in-process fake S3 server.
func newFakeS3FS(t *testing.T) *s3fs.S3FS {
	t.Helper()
	const bucket = "lfs-test"
	backend := s3mem.New()
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(server.Close)
	client := s3.New(s3.Options{
		Region:                     "us-east-1",
		BaseEndpoint:               aws.String(server.URL),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	return s3fs.New(bucket,
		s3fs.WithClient(client),
		s3fs.WithPresignClient(s3.NewPresignClient(client)),
	)
}

// TestPresignRoundTrip uploads through a presigned PUT URL and downloads
// through a presigned GET URL, the redirect paths used by the LFS handlers.
// The storage sees the bucket through a chroot, like production.
func TestPresignRoundTrip(t *testing.T) {
	fs := newFakeS3FS(t)
	lfsFS, err := fs.Chroot("/lfs")
	if err != nil {
		t.Fatal(err)
	}
	storage := lfs.New(lfsFS)

	data := []byte("presigned content")
	oid := oidOf(data)

	putter, ok := storage.(lfs.SignPutter)
	if !ok {
		t.Fatal("S3-backed storage must implement SignPutter")
	}
	putURL, putHeader, err := putter.SignPut(oid)
	if err != nil {
		t.Fatalf("SignPut failed: %v", err)
	}
	rawOID, err := hex.DecodeString(oid)
	if err != nil {
		t.Fatal(err)
	}
	var checksum string
	for k, v := range putHeader {
		if strings.EqualFold(k, "x-amz-checksum-sha256") {
			checksum = v
		}
	}
	if want := base64.StdEncoding.EncodeToString(rawOID); checksum != want {
		t.Fatalf("checksum header = %q, want %q", checksum, want)
	}
	req, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range putHeader {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("presigned PUT failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("presigned PUT status = %d", resp.StatusCode)
	}

	// the upload lands in the staging area, invisible until verified
	if storage.Exists(oid) {
		t.Fatal("object must not be visible before verify")
	}
	if _, err := fs.Stat(filepath.Join("/lfs/tmp", oid)); err != nil {
		t.Fatalf("staged object missing: %v", err)
	}

	verifier, ok := storage.(lfs.PutVerifier)
	if !ok {
		t.Fatal("S3-backed storage must implement PutVerifier")
	}
	if err := verifier.VerifyPut(oid, int64(len(data))+1); err == nil {
		t.Fatal("VerifyPut must reject a size mismatch")
	}
	if err := verifier.VerifyPut(oid, int64(len(data))); err != nil {
		t.Fatalf("VerifyPut failed: %v", err)
	}

	if !storage.Exists(oid) {
		t.Fatal("object must exist after verify")
	}
	// the promoted key must include the prefix the chroot hides
	if _, err := fs.Stat(filepath.Join("/lfs", oid[0:2], oid[2:4], oid[4:])); err != nil {
		t.Fatalf("object not at the expected bucket key: %v", err)
	}
	if err := verifier.VerifyPut(oid, int64(len(data))); err != nil {
		t.Fatalf("VerifyPut must be idempotent: %v", err)
	}
	if info, err := storage.Info(oid); err != nil || info.Size() != int64(len(data)) {
		t.Fatalf("Info = %v, %v", info, err)
	}

	getter, ok := storage.(lfs.SignGetter)
	if !ok {
		t.Fatal("S3-backed storage must implement SignGetter")
	}
	getURL, getHeader, err := getter.SignGet(oid)
	if err != nil {
		t.Fatalf("SignGet failed: %v", err)
	}
	if len(getHeader) != 0 {
		t.Fatalf("SignGet header = %v, want none (redirects cannot carry headers)", getHeader)
	}
	getResp, err := http.Get(getURL)
	if err != nil {
		t.Fatalf("presigned GET failed: %v", err)
	}
	defer getResp.Body.Close()
	got, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("presigned GET data = %q, want %q", got, data)
	}
}

// TestNoPresignCapabilityOnPlainFS ensures sign capabilities are absent when
// the filesystem cannot presign, so handlers fall back to direct serving.
func TestNoPresignCapabilityOnPlainFS(t *testing.T) {
	storage := lfs.New(osfs.New(t.TempDir()))
	if _, ok := storage.(lfs.SignGetter); ok {
		t.Fatal("plain filesystem storage must not implement SignGetter")
	}
	if _, ok := storage.(lfs.SignPutter); ok {
		t.Fatal("plain filesystem storage must not implement SignPutter")
	}
}

// TestPresignEndpointSplit verifies that URLs are signed against the
// client-facing endpoint (--s3-sign-endpoint) while the server keeps talking
// to its own endpoint.
func TestPresignEndpointSplit(t *testing.T) {
	const bucket = "lfs-test"
	backend := s3mem.New()
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(server.Close)
	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})

	const signEndpoint = "http://public.example:9000"
	fs := s3fs.New(bucket,
		s3fs.WithClient(client),
		s3fs.WithPresignClient(s3.NewPresignClient(client, s3fs.WithPresignEndpoint(signEndpoint))),
	)
	lfsFS, err := fs.Chroot("/lfs")
	if err != nil {
		t.Fatal(err)
	}
	storage := lfs.New(lfsFS)

	data := []byte("endpoint split")
	oid := oidOf(data)
	if err := storage.Put(oid, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("Put via server endpoint failed: %v", err)
	}

	getURL, _, err := storage.(lfs.SignGetter).SignGet(oid)
	if err != nil {
		t.Fatalf("SignGet failed: %v", err)
	}
	if !strings.HasPrefix(getURL, signEndpoint+"/") {
		t.Fatalf("SignGet URL = %q, want prefix %q", getURL, signEndpoint)
	}
	putURL, _, err := storage.(lfs.SignPutter).SignPut(oid)
	if err != nil {
		t.Fatalf("SignPut failed: %v", err)
	}
	if !strings.HasPrefix(putURL, signEndpoint+"/") {
		t.Fatalf("SignPut URL = %q, want prefix %q", putURL, signEndpoint)
	}
}
