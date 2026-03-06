package git

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/wzshiming/hfd/internal/utils"
	"github.com/wzshiming/hfd/pkg/authenticate"
	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/repository"
)

// Server implements the git protocol (git://) server.
type Server struct {
	repositoriesDir string
	proxyManager    *repository.ProxyManager
	permissionHook  permission.PermissionHook
	tokenSignValidator     authenticate.TokenSignValidator
	lfsURL          string
}

// Option configures the git protocol server.
type Option func(*Server)

// WithProxyManager sets the proxy manager for the git protocol server.
func WithProxyManager(pm *repository.ProxyManager) Option {
	return func(s *Server) {
		s.proxyManager = pm
	}
}

// WithPermissionHookFunc sets the permission hook for verifying operations.
func WithPermissionHookFunc(hook permission.PermissionHook) Option {
	return func(s *Server) {
		s.permissionHook = hook
	}
}

// WithLFSURL sets the base HTTP URL for the server, used by git-lfs-authenticate
// to tell LFS clients the LFS API endpoint. For example: "http://localhost:8080".
func WithLFSURL(lfsURL string) Option {
	return func(s *Server) {
		s.lfsURL = lfsURL
	}
}

// WithTokenSignValidator configures the git protocol server to include authentication
// headers in git-lfs-authenticate responses so that LFS clients can authenticate
// with the HTTP server.
func WithTokenSignValidator(auth authenticate.TokenSignValidator) Option {
	return func(s *Server) {
		s.tokenSignValidator = auth
	}
}

// NewServer creates a new git protocol server.
func NewServer(repositoriesDir string, opts ...Option) *Server {
	s := &Server{
		repositoriesDir: repositoriesDir,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Serve accepts connections on the listener and handles them.
func (s *Server) Serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go s.handleConnection(conn)
	}
}

// ListenAndServe listens on the given address and serves git protocol requests.
func (s *Server) ListenAndServe(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	return s.Serve(listener)
}

// handleConnection handles a single git protocol connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	service, repoPath, err := readRequest(conn)
	if err != nil {
		log.Printf("git protocol: error reading request: %v\n", err)
		return
	}

	switch service {
	case repository.GitUploadPack, repository.GitReceivePack:
		s.executeGitCommand(conn, service, repoPath)
	case repository.GitLFSAuthenticate:
		s.handleLFSAuthenticate(conn, repoPath)
	case repository.GitLFSTransfer:
		log.Printf("git protocol: git-lfs-transfer is not supported, clients should fall back to git-lfs-authenticate\n")
	default:
		log.Printf("git protocol: unsupported service: %s\n", service)
	}
}

// executeGitCommand runs a git service command and pipes I/O through the connection.
func (s *Server) executeGitCommand(conn net.Conn, service string, repoPath string) {
	fullPath := repository.ResolvePath(s.repositoriesDir, repoPath)
	if fullPath == "" {
		log.Printf("git protocol: repository not found: %s\n", repoPath)
		return
	}

	ctx := context.Background()

	if s.permissionHook != nil {
		op := permission.OperationReadRepo
		if service == repository.GitReceivePack {
			op = permission.OperationUpdateRepo
		}
		if err := s.permissionHook(ctx, op, repoPath, permission.Context{}); err != nil {
			log.Printf("git protocol: auth hook denied %s on %s: %v\n", service, repoPath, err)
			return
		}
	}

	repo, err := s.openRepo(ctx, fullPath, repoPath, service)
	if err != nil {
		log.Printf("git protocol: repository not found: %s\n", repoPath)
		return
	}

	if service == repository.GitReceivePack {
		isMirror, _, err := repo.IsMirror()
		if err != nil {
			log.Printf("git protocol: failed to check repository type: %v\n", err)
			return
		}
		if isMirror {
			log.Printf("git protocol: push to mirror repository %q is not allowed\n", repoPath)
			return
		}
	}

	cmd := utils.Command(ctx, service, fullPath)
	cmd.Stdin = conn
	cmd.Stdout = conn
	if err := cmd.Run(); err != nil {
		log.Printf("git protocol: command %s failed: %v\n", service, err)
		return
	}
}

// openRepo opens a repository, optionally creating a mirror from the proxy source.
func (s *Server) openRepo(ctx context.Context, repoPath, repoName, service string) (*repository.Repository, error) {
	repo, err := repository.Open(repoPath)
	if err == nil {
		if mirror, _, err := repo.IsMirror(); err == nil && mirror {
			if err := repo.SyncMirror(ctx); err != nil {
				return nil, err
			}
		}
		return repo, nil
	}
	// Only proxy for read operations
	if service != repository.GitUploadPack {
		return nil, err
	}
	if err == repository.ErrRepositoryNotExists && s.proxyManager != nil {
		if s.permissionHook != nil {
			if err := s.permissionHook(ctx, permission.OperationCreateProxyRepo, repoName, permission.Context{}); err != nil {
				return repository.Open(repoPath)
			}
		}
		return s.proxyManager.OpenOrProxy(ctx, repoPath, repoName)
	}
	return nil, err
}

// lfsAuthResponse is the JSON response returned by git-lfs-authenticate.
type lfsAuthResponse struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header,omitempty"`
	ExpiresIn int               `json:"expires_in,omitempty"`
}

// handleLFSAuthenticate handles a git-lfs-authenticate request over the git protocol.
// The repoPath may contain the operation appended after a space (e.g. "repo.git download").
func (s *Server) handleLFSAuthenticate(conn net.Conn, repoPath string) {
	if s.lfsURL == "" {
		log.Printf("git protocol: LFS authentication is not configured on this server\n")
		return
	}

	// Parse operation from repoPath: "path operation"
	actualPath, operation, ok := strings.Cut(repoPath, " ")
	if !ok {
		log.Printf("git protocol: git-lfs-authenticate: missing operation in %q\n", repoPath)
		return
	}
	actualPath = strings.Trim(actualPath, "'")
	operation = strings.TrimSpace(operation)

	if operation != "download" && operation != "upload" {
		log.Printf("git protocol: git-lfs-authenticate: invalid operation %q\n", operation)
		return
	}

	fullPath := repository.ResolvePath(s.repositoriesDir, actualPath)
	if fullPath == "" {
		log.Printf("git protocol: repository not found: %s\n", actualPath)
		return
	}

	ctx := context.Background()

	if s.permissionHook != nil {
		op := permission.OperationReadRepo
		if operation == "upload" {
			op = permission.OperationUpdateRepo
		}
		if err := s.permissionHook(ctx, op, actualPath, permission.Context{}); err != nil {
			log.Printf("git protocol: auth hook denied lfs-%s on %s: %v\n", operation, actualPath, err)
			return
		}
	}

	// Build the LFS API href
	href := repository.LFSHref(s.lfsURL, actualPath)

	resp := lfsAuthResponse{
		Href:      href,
		Header:    make(map[string]string),
		ExpiresIn: 3600,
	}

	// Include authentication headers when a token signer is configured,
	// so LFS clients can authenticate with the HTTP server.
	if s.tokenSignValidator != nil {
		batchURL := href + "/objects/batch"
		if token := s.tokenSignValidator.Sign(ctx, http.MethodPost, batchURL, authenticate.Anonymous, time.Duration(resp.ExpiresIn)*time.Second); token != "" {
			resp.Header["Authorization"] = "Bearer " + token
		}
	}

	data, err := json.Marshal(resp)
	if err != nil {
		log.Printf("git protocol: failed to marshal LFS auth response: %v\n", err)
		return
	}

	if _, err := conn.Write(data); err != nil {
		log.Printf("git protocol: failed to write LFS auth response: %v\n", err)
		return
	}
}

// readRequest reads the initial git protocol request from the connection.
// Returns the service name and repository path.
func readRequest(r io.Reader) (service string, repoPath string, err error) {
	// Read the 4-byte hex-encoded packet length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return "", "", fmt.Errorf("reading packet length: %w", err)
	}

	decoded, err := hex.DecodeString(string(lenBuf))
	if err != nil || len(decoded) != 2 {
		return "", "", fmt.Errorf("invalid packet length: %q", lenBuf)
	}
	pktLen := int(decoded[0])<<8 | int(decoded[1])

	if pktLen < 4 {
		return "", "", fmt.Errorf("invalid packet length: %d", pktLen)
	}

	// Read the rest of the packet
	payload := make([]byte, pktLen-4)
	if _, err := io.ReadFull(r, payload); err != nil {
		return "", "", fmt.Errorf("reading packet payload: %w", err)
	}

	// Parse: "service path\x00host=hostname\x00"
	// Split on the first NUL byte to separate the command from extra parameters
	parts := strings.SplitN(string(payload), "\x00", 2)
	if len(parts) < 1 {
		return "", "", fmt.Errorf("invalid request format")
	}

	// Split "service path"
	cmd := parts[0]
	before, after, ok := strings.Cut(cmd, " ")
	if !ok {
		return "", "", fmt.Errorf("invalid request: no space separator in %q", cmd)
	}

	service = before
	repoPath = after

	return service, repoPath, nil
}
