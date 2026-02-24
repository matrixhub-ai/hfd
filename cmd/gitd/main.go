// Command gitd is a git server that uses the git binary to serve repositories over HTTP.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/wzshiming/gitd/internal/handlers"
	backendgit "github.com/wzshiming/gitd/pkg/backend/git"
	backendhttp "github.com/wzshiming/gitd/pkg/backend/http"
	"github.com/wzshiming/gitd/pkg/lfs"
	"github.com/wzshiming/gitd/pkg/s3fs"
)

var (
	addr           = ":8080"
	gitAddr        = ""
	dataDir        = "./data"
	s3Repositories = false
	s3SignEndpoint = ""
	s3Endpoint     = ""
	s3AccessKey    = ""
	s3SecretKey    = ""
	s3Bucket       = ""
	s3UsePathStyle = false
)

func init() {
	flag.StringVar(&addr, "addr", ":8080", "HTTP server address")
	flag.StringVar(&gitAddr, "git-addr", "", "Git protocol server address (e.g. :9418)")
	flag.StringVar(&dataDir, "data", "./data", "Directory containing git repositories")
	flag.BoolVar(&s3Repositories, "s3-repositories", false, "Store repositories in S3")
	flag.StringVar(&s3Endpoint, "s3-endpoint", "", "S3 endpoint")
	flag.StringVar(&s3SignEndpoint, "s3-sign-endpoint", "", "S3 signing endpoint (if different from s3-endpoint)")
	flag.StringVar(&s3AccessKey, "s3-access-key", "", "S3 access key")
	flag.StringVar(&s3SecretKey, "s3-secret-key", "", "S3 secret key")
	flag.StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket name")
	flag.BoolVar(&s3UsePathStyle, "s3-use-path-style", false, "Use path style for S3 URLs")
	flag.Parse()
}

func main() {
	absRootDir, err := filepath.Abs(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting absolute path of repo directory: %v\n", err)
		os.Exit(1)
	}

	opts := []backendhttp.Option{
		backendhttp.WithRootDir(absRootDir),
	}

	log.Printf("Starting matrixhub server on %s, serving repositories from %s\n", addr, absRootDir)

	if s3Endpoint != "" && s3Bucket != "" {
		if s3Repositories {
			repositoriesDir := filepath.Join(absRootDir, "repositories")
			log.Printf("Mounting S3 bucket %s at %s\n", s3Bucket, repositoriesDir)
			err := s3fs.Mount(
				context.Background(),
				repositoriesDir,
				s3Endpoint,
				s3AccessKey,
				s3SecretKey,
				s3Bucket,
				"/repositories/",
				s3UsePathStyle,
			)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error mounting S3 bucket: %v\n", err)
				os.Exit(1)
			}
			defer func() {
				log.Printf("Unmounting S3 bucket from %s\n", repositoriesDir)
				if err := s3fs.Unmount(context.Background(), repositoriesDir); err != nil {
					fmt.Fprintf(os.Stderr, "Error unmounting S3 bucket: %v\n", err)
				}
			}()
		}

		opts = append(opts,
			backendhttp.WithLFSS3(
				lfs.NewS3(
					"lfs",
					s3Endpoint,
					s3AccessKey,
					s3SecretKey,
					s3Bucket,
					s3UsePathStyle,
					s3SignEndpoint,
				),
			),
		)
	}

	var handler http.Handler
	handler = backendhttp.NewHandler(
		opts...,
	)

	handler = handlers.CompressHandler(handler)
	handler = handlers.LoggingHandler(os.Stderr, handler)

	if gitAddr != "" {
		repositoriesDir := filepath.Join(absRootDir, "repositories")
		gitServer := backendgit.NewServer(repositoriesDir)
		log.Printf("Starting git protocol server on %s\n", gitAddr)
		go func() {
			if err := gitServer.ListenAndServe(gitAddr); err != nil {
				fmt.Fprintf(os.Stderr, "Error starting git protocol server on %s: %v\n", gitAddr, err)
				os.Exit(1)
			}
		}()
	}

	if err := http.ListenAndServe(addr, handler); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %v\n", err)
		os.Exit(1)
	}
}
