package git

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/wzshiming/gitd/internal/utils"
	"github.com/wzshiming/gitd/pkg/repository"
)

// Server implements the git protocol (git://) server.
type Server struct {
	repositoriesDir string
}

// NewServer creates a new git protocol server.
func NewServer(repositoriesDir string) *Server {
	return &Server{
		repositoriesDir: repositoriesDir,
	}
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

	if service != "git-upload-pack" && service != "git-receive-pack" {
		log.Printf("git protocol: unsupported service: %s\n", service)
		return
	}

	fullPath := repository.ResolvePath(s.repositoriesDir, repoPath)
	if fullPath == "" || !repository.IsRepository(fullPath) {
		log.Printf("git protocol: repository not found: %s\n", repoPath)
		return
	}

	if service == "git-receive-pack" {
		repo, err := repository.Open(fullPath)
		if err != nil {
			log.Printf("git protocol: failed to open repository: %v\n", err)
			return
		}
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

	ctx := context.Background()
	cmd := utils.Command(ctx, service, fullPath)
	cmd.Stdin = conn
	cmd.Stdout = conn
	if err := cmd.Run(); err != nil {
		log.Printf("git protocol: command %s failed: %v\n", service, err)
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
	spaceIdx := strings.Index(cmd, " ")
	if spaceIdx < 0 {
		return "", "", fmt.Errorf("invalid request: no space separator in %q", cmd)
	}

	service = cmd[:spaceIdx]
	repoPath = cmd[spaceIdx+1:]

	return service, repoPath, nil
}
