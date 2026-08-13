package lfs

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3Storage struct {
	client            *s3.Client
	presign           *s3.PresignClient
	signPresign       *s3.PresignClient
	basePath          string
	bucket            string
	expire            time.Duration
	checksumAlgorithm types.ChecksumAlgorithm
}

var (
	_ Storage    = (*s3Storage)(nil)
	_ SignGetter = (*s3Storage)(nil)
	_ SignPutter = (*s3Storage)(nil)
)

// NewS3 creates a new S3-backed Store. The basePath is a prefix for all object keys in the bucket.
func NewS3(basePath, endpoint, accessKey, secretKey, bucket string, forcePathStyle bool, s3SignEndpoint string) Storage {
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = forcePathStyle
	})

	if s3SignEndpoint == "" {
		s3SignEndpoint = endpoint
	}

	signClient := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3SignEndpoint)
		o.UsePathStyle = forcePathStyle
	})

	return &s3Storage{
		basePath:          basePath,
		client:            client,
		presign:           s3.NewPresignClient(client),
		signPresign:       s3.NewPresignClient(signClient),
		bucket:            bucket,
		expire:            60 * time.Minute,
		checksumAlgorithm: types.ChecksumAlgorithmSha256,
	}
}

func (s *s3Storage) SignGet(oid string) (string, error) {
	key := path.Join(s.basePath, transformKey(oid))
	req, err := s.signPresign.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}, s3.WithPresignExpires(s.expire))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

func hexToBase64(hexStr string) (string, error) {
	bin, err := hex.DecodeString(hexStr)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(bin), nil
}

func (s *s3Storage) SignPut(oid string) (string, error) {
	sha256, err := hexToBase64(oid)
	if err != nil {
		return "", err
	}
	key := path.Join(s.basePath, transformKey(oid))
	req, err := s.signPresign.PresignPutObject(context.Background(), &s3.PutObjectInput{
		Bucket:            &s.bucket,
		Key:               &key,
		ChecksumAlgorithm: s.checksumAlgorithm,
		ChecksumSHA256:    &sha256,
	}, s3.WithPresignExpires(s.expire))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

func (s *s3Storage) Put(oid string, r io.Reader, size int64) error {
	sha256, err := hexToBase64(oid)
	if err != nil {
		return err
	}

	key := path.Join(s.basePath, transformKey(oid))
	req, err := s.presign.PresignPutObject(context.Background(), &s3.PutObjectInput{
		Bucket:            &s.bucket,
		Key:               &key,
		ContentLength:     &size,
		ChecksumAlgorithm: s.checksumAlgorithm,
		ChecksumSHA256:    &sha256,
	}, s3.WithPresignExpires(s.expire))
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequest(http.MethodPut, req.URL, r)
	if err != nil {
		return err
	}
	// the presigned URL is only valid when all signed headers are sent
	for k, vs := range req.SignedHeader {
		for _, v := range vs {
			httpReq.Header.Add(k, v)
		}
	}
	httpReq.ContentLength = size
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to upload object, status code: %d", resp.StatusCode)
	}
	return nil
}

func (s *s3Storage) Info(oid string) (os.FileInfo, error) {
	key := path.Join(s.basePath, transformKey(oid))
	output, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFoundError(err) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return &s3FileInfo{
		key:          key,
		size:         aws.ToInt64(output.ContentLength),
		lastModified: aws.ToTime(output.LastModified),
	}, nil
}

type s3FileInfo struct {
	key          string
	size         int64
	lastModified time.Time
}

func (f *s3FileInfo) Name() string {
	return f.key
}

func (f *s3FileInfo) Size() int64 {
	return f.size
}

func (f *s3FileInfo) Mode() os.FileMode {
	return 0444
}

func (f *s3FileInfo) ModTime() (t time.Time) {
	return f.lastModified
}

func (f *s3FileInfo) IsDir() bool {
	return false
}

func (f *s3FileInfo) Sys() any {
	return nil
}

// Exists returns true if the object exists in S3.
func (s *s3Storage) Exists(oid string) bool {
	_, err := s.Info(oid)
	return err == nil
}

func isNotFoundError(err error) bool {
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	// S3-compatible stores may return a bare 404 without the NotFound error code
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound
}
