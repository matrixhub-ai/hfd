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
	"github.com/wzshiming/gitd/pkg/authenticate"
	backendgit "github.com/wzshiming/gitd/pkg/backend/git"
	backendhttp "github.com/wzshiming/gitd/pkg/backend/http"
	"github.com/wzshiming/gitd/pkg/backend/huggingface"
	backendlfs "github.com/wzshiming/gitd/pkg/backend/lfs"
	backendssh "github.com/wzshiming/gitd/pkg/backend/ssh"
	backendweb "github.com/wzshiming/gitd/pkg/backend/web"
	"github.com/wzshiming/gitd/pkg/lfs"
	"github.com/wzshiming/gitd/pkg/s3fs"
	"github.com/wzshiming/gitd/pkg/storage"
	"github.com/wzshiming/gitd/web"
)

var (
	gitAddr        = ""
	addr           = ":8080"
	sshAddr        = ":2222"
	sshHostKeyFile = ""
	dataDir        = "./data"
	s3Repositories = false
	s3SignEndpoint = ""
	s3Endpoint     = ""
	s3AccessKey    = ""
	s3SecretKey    = ""
	s3Bucket       = ""
	s3UsePathStyle = false

	// Authentication flags
	sshAuthorizedKey = ""
	httpUsername     = ""
	httpPassword     = ""
)

func init() {
	flag.StringVar(&gitAddr, "git-addr", "", "Git protocol server address (e.g. :9418)")
	flag.StringVar(&addr, "addr", ":8080", "HTTP server address")
	flag.StringVar(&sshAddr, "ssh-addr", ":2222", "SSH protocol server address")
	flag.StringVar(&sshHostKeyFile, "ssh-host-key", "", "Path to SSH host key file (PEM format); if empty, a key is generated")
	flag.StringVar(&dataDir, "data", "./data", "Directory containing git repositories")
	flag.BoolVar(&s3Repositories, "s3-repositories", false, "Store repositories in S3")
	flag.StringVar(&s3Endpoint, "s3-endpoint", "", "S3 endpoint")
	flag.StringVar(&s3SignEndpoint, "s3-sign-endpoint", "", "S3 signing endpoint (if different from s3-endpoint)")
	flag.StringVar(&s3AccessKey, "s3-access-key", "", "S3 access key")
	flag.StringVar(&s3SecretKey, "s3-secret-key", "", "S3 secret key")
	flag.StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket name")
	flag.BoolVar(&s3UsePathStyle, "s3-use-path-style", false, "Use path style for S3 URLs")

	// Authentication flags
	flag.StringVar(&sshAuthorizedKey, "ssh-authorized-key", "", "Path to SSH authorized_keys file for public key authentication")
	flag.StringVar(&httpUsername, "http-username", "", "Username for HTTP basic authentication")
	flag.StringVar(&httpPassword, "http-password", "", "Password for HTTP basic authentication")

	flag.Parse()
}

func main() {
	absRootDir, err := filepath.Abs(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting absolute path of repo directory: %v\n", err)
		os.Exit(1)
	}

	storageOpts := []storage.Option{
		storage.WithRootDir(absRootDir),
	}

	log.Printf("Starting gitd server on %s, serving repositories from %s\n", addr, absRootDir)

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

		lfss3 := lfs.NewS3(
			"lfs",
			s3Endpoint,
			s3AccessKey,
			s3SecretKey,
			s3Bucket,
			s3UsePathStyle,
			s3SignEndpoint,
		)
		storageOpts = append(storageOpts,
			storage.WithLFSS3(
				lfss3,
			),
		)

	}

	storage := storage.NewStorage(storageOpts...)
	var handler http.Handler

	handler = backendweb.NewHandler(
		backendweb.WithStorage(storage), backendweb.WithNext(web.Web),
	)

	handler = huggingface.NewHandler(
		huggingface.WithStorage(storage), huggingface.WithNext(handler),
	)

	handler = backendlfs.NewHandler(
		backendlfs.WithStorage(storage), backendlfs.WithNext(handler),
	)

	handler = backendhttp.NewHandler(
		backendhttp.WithStorage(storage), backendhttp.WithNext(handler),
	)

	if httpUsername != "" {
		handler = authenticate.Authenticate(httpUsername, httpPassword, handler)
	}

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

	if sshAddr != "" {
		repositoriesDir := filepath.Join(absRootDir, "repositories")
		var hostKeySigner backendssh.Signer
		hostKeyPath := sshHostKeyFile
		if hostKeyPath == "" {
			hostKeyPath = filepath.Join(absRootDir, "ssh_host_ed25519_key")
		}
		data, err := os.ReadFile(hostKeyPath)
		if err == nil {
			hostKeySigner, err = backendssh.ParseHostKeyFile(data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error parsing SSH host key file: %v\n", err)
				os.Exit(1)
			}
			log.Printf("Loaded SSH host key from %s\n", hostKeyPath)
		} else if sshHostKeyFile != "" {
			fmt.Fprintf(os.Stderr, "Error reading SSH host key file: %v\n", err)
			os.Exit(1)
		} else {
			hostKeySigner, err = backendssh.GenerateAndSaveHostKey(hostKeyPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error generating SSH host key: %v\n", err)
				os.Exit(1)
			}
			log.Printf("Generated SSH host key and saved to %s\n", hostKeyPath)
		}
		var sshOpts []backendssh.Option
		if sshAuthorizedKey != "" {
			authKeysData, err := os.ReadFile(sshAuthorizedKey)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error reading SSH authorized keys file: %v\n", err)
				os.Exit(1)
			}
			authorizedKeys, err := backendssh.ParseAuthorizedKeys(authKeysData)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error parsing SSH authorized keys: %v\n", err)
				os.Exit(1)
			}
			sshOpts = append(sshOpts, backendssh.WithPublicKeyCallback(backendssh.AuthorizedKeysCallback(authorizedKeys)))
			log.Printf("SSH public key authentication enabled with %d key(s)\n", len(authorizedKeys))
		}
		sshServer := backendssh.NewServer(repositoriesDir, hostKeySigner, sshOpts...)
		log.Printf("Starting SSH protocol server on %s\n", sshAddr)
		go func() {
			if err := sshServer.ListenAndServe(sshAddr); err != nil {
				fmt.Fprintf(os.Stderr, "Error starting SSH protocol server on %s: %v\n", sshAddr, err)
				os.Exit(1)
			}
		}()
	}

	if err := http.ListenAndServe(addr, handler); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %v\n", err)
		os.Exit(1)
	}
}
