package git

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/wzshiming/hfd/internal/utils"
	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/repository"
)

// Server implements the git protocol (git://) server.
type Server struct {
	repositoriesDir string
	proxyManager    *repository.ProxyManager
	permissionHook  permission.PermissionHook
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

	if service != repository.GitUploadPack && service != repository.GitReceivePack {
		log.Printf("git protocol: unsupported service: %s\n", service)
		return
	}

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
