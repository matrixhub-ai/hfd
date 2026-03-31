package lfs

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"             //nolint:staticcheck
	"github.com/aws/aws-sdk-go/aws/credentials" //nolint:staticcheck
	"github.com/aws/aws-sdk-go/aws/session"     //nolint:staticcheck
	"github.com/aws/aws-sdk-go/service/s3"      //nolint:staticcheck
)

type s3Storage struct {
	s3                *s3.S3
	signS3            *s3.S3
	basePath          string
	bucket            string
	expire            time.Duration
	checksumAlgorithm string
}

// NewS3 creates a new S3-backed Store. The basePath is a prefix for all object keys in the bucket.
func NewS3(basePath, endpoint, accessKey, secretKey, bucket string, forcePathStyle bool, s3SignEndpoint string) Storage {
	sess := session.Must(session.NewSession(&aws.Config{
		Endpoint:         &endpoint,
		Region:           aws.String("us-east-1"),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		S3ForcePathStyle: &forcePathStyle,
	}))

	if s3SignEndpoint == "" {
		s3SignEndpoint = endpoint
	}

	signSess := session.Must(session.NewSession(&aws.Config{
		Endpoint:         &s3SignEndpoint,
		Region:           aws.String("us-east-1"),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		S3ForcePathStyle: &forcePathStyle,
	}))

	return &s3Storage{
		basePath:          basePath,
		s3:                s3.New(sess),
		signS3:            s3.New(signSess),
		bucket:            bucket,
		expire:            60 * time.Minute,
		checksumAlgorithm: "SHA256",
	}
}

func (s *s3Storage) SignGet(oid string) (string, error) {
	key := path.Join(s.basePath, transformKey(oid))
	req, _ := s.signS3.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	urlStr, err := req.Presign(s.expire)
	if err != nil {
		return "", err
	}
	return urlStr, nil
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
	req, _ := s.signS3.PutObjectRequest(&s3.PutObjectInput{
		Bucket:            &s.bucket,
		Key:               &key,
		ChecksumAlgorithm: &s.checksumAlgorithm,
		ChecksumSHA256:    &sha256,
	})
	urlStr, err := req.Presign(s.expire)
	if err != nil {
		return "", err
	}
	return urlStr, nil
}

func (s *s3Storage) Put(oid string, r io.Reader, size int64) error {
	sha256, err := hexToBase64(oid)
	if err != nil {
		return err
	}

	key := path.Join(s.basePath, transformKey(oid))
	req, _ := s.s3.PutObjectRequest(&s3.PutObjectInput{
		Bucket:            &s.bucket,
		Key:               &key,
		ContentLength:     &size,
		ChecksumAlgorithm: &s.checksumAlgorithm,
		ChecksumSHA256:    &sha256,
	})
	urlStr, err := req.Presign(s.expire)
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequest(http.MethodPut, urlStr, r)
	if err != nil {
		return err
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
	output, err := s.s3.HeadObject(&s3.HeadObjectInput{
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
		size:         *output.ContentLength,
		lastModified: *output.LastModified,
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
	if aerr, ok := err.(s3.RequestFailure); ok {
		if aerr.StatusCode() == 404 {
			return true
		}
	}
	return false
}

// SignMultipartPut initiates a multipart upload and returns presigned URLs for each part.
func (s *s3Storage) SignMultipartPut(oid string, size int64) (*MultipartUpload, error) {
	const minPartSize = int64(100 * 1024 * 1024) // 100MB per part (per spec)
	const maxParts = 10000                        // S3 limit

	key := path.Join(s.basePath, transformKey(oid))

	// Initiate multipart upload
	createInput := &s3.CreateMultipartUploadInput{
		Bucket:            &s.bucket,
		Key:               &key,
		ChecksumAlgorithm: &s.checksumAlgorithm,
	}

	result, err := s.s3.CreateMultipartUpload(createInput)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	uploadID := *result.UploadId

	// Calculate number of parts
	partSize := minPartSize
	numParts := int((size + partSize - 1) / partSize)

	// Ensure we don't exceed S3's max parts limit
	if numParts > maxParts {
		partSize = (size + maxParts - 1) / maxParts
		numParts = maxParts
	}

	// Generate presigned URLs for each part
	parts := make([]MultipartPart, numParts)
	for i := 0; i < numParts; i++ {
		partNum := i + 1
		pos := int64(i) * partSize
		partSizeForThisPart := partSize

		// Last part might be smaller
		if i == numParts-1 {
			partSizeForThisPart = size - pos
		}

		req, _ := s.signS3.UploadPartRequest(&s3.UploadPartInput{
			Bucket:     &s.bucket,
			Key:        &key,
			PartNumber: aws.Int64(int64(partNum)),
			UploadId:   &uploadID,
		})

		urlStr, err := req.Presign(s.expire)
		if err != nil {
			// Abort the upload since we can't generate all URLs
			_ = s.AbortMultipartUpload(oid, uploadID)
			return nil, fmt.Errorf("failed to presign part %d: %w", partNum, err)
		}

		parts[i] = MultipartPart{
			PartNumber: partNum,
			URL:        urlStr,
			Pos:        pos,
			Size:       partSizeForThisPart,
		}
	}

	return &MultipartUpload{
		UploadID: uploadID,
		Parts:    parts,
	}, nil
}

// CompleteMultipartUpload completes a multipart upload.
func (s *s3Storage) CompleteMultipartUpload(oid string, uploadID string, partETags map[int]string) error {
	key := path.Join(s.basePath, transformKey(oid))

	// Build the list of completed parts
	completedParts := make([]*s3.CompletedPart, 0, len(partETags))
	for partNum, etag := range partETags {
		completedParts = append(completedParts, &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(int64(partNum)),
		})
	}

	// Sort by part number (S3 requires this)
	// Using a simple bubble sort since parts are usually small
	for i := 0; i < len(completedParts); i++ {
		for j := i + 1; j < len(completedParts); j++ {
			if *completedParts[i].PartNumber > *completedParts[j].PartNumber {
				completedParts[i], completedParts[j] = completedParts[j], completedParts[i]
			}
		}
	}

	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   &s.bucket,
		Key:      &key,
		UploadId: &uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	_, err := s.s3.CompleteMultipartUpload(completeInput)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

// AbortMultipartUpload aborts a multipart upload and cleans up any uploaded parts.
func (s *s3Storage) AbortMultipartUpload(oid string, uploadID string) error {
	key := path.Join(s.basePath, transformKey(oid))

	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   &s.bucket,
		Key:      &key,
		UploadId: &uploadID,
	}

	_, err := s.s3.AbortMultipartUpload(abortInput)
	if err != nil {
		return fmt.Errorf("failed to abort multipart upload: %w", err)
	}

	return nil
}
