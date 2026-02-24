package ssh

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/wzshiming/gitd/internal/utils"
	"github.com/wzshiming/gitd/pkg/repository"
)

// Signer is an alias for ssh.Signer to avoid requiring callers to import golang.org/x/crypto/ssh.
type Signer = ssh.Signer

// PublicKey is an alias for ssh.PublicKey to avoid requiring callers to import golang.org/x/crypto/ssh.
type PublicKey = ssh.PublicKey

// Server implements the SSH protocol (ssh://) server for git operations.
type Server struct {
	repositoriesDir string
	config          *ssh.ServerConfig
}

// Option configures the SSH server.
type Option func(*ssh.ServerConfig)

// WithPublicKeyCallback sets the public key authentication callback for the SSH server.
// When set, clients must authenticate with a public key accepted by the callback.
func WithPublicKeyCallback(callback func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error)) Option {
	return func(config *ssh.ServerConfig) {
		config.NoClientAuth = false
		config.PublicKeyCallback = callback
	}
}

// NewServer creates a new SSH protocol server.
func NewServer(repositoriesDir string, hostKey ssh.Signer, opts ...Option) *Server {
	config := &ssh.ServerConfig{
		NoClientAuth: true,
	}
	for _, opt := range opts {
		opt(config)
	}
	config.AddHostKey(hostKey)

	return &Server{
		repositoriesDir: repositoriesDir,
		config:          config,
	}
}

// ParseAuthorizedKeys parses an OpenSSH authorized_keys file and returns
// the parsed public keys. Lines that are empty or start with '#' are skipped.
func ParseAuthorizedKeys(data []byte) ([]ssh.PublicKey, error) {
	var keys []ssh.PublicKey
	rest := data
	for len(rest) > 0 {
		var key ssh.PublicKey
		var err error
		key, _, _, rest, err = ssh.ParseAuthorizedKey(rest)
		if err != nil {
			return nil, fmt.Errorf("parsing authorized key: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// AuthorizedKeysCallback returns a PublicKeyCallback that checks incoming keys
// against the provided list of authorized public keys.
func AuthorizedKeysCallback(authorizedKeys []ssh.PublicKey) func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
	keyMap := make(map[string]bool, len(authorizedKeys))
	for _, k := range authorizedKeys {
		keyMap[string(k.Marshal())] = true
	}
	return func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if keyMap[string(key.Marshal())] {
			return &ssh.Permissions{}, nil
		}
		return nil, fmt.Errorf("public key not found in authorized keys")
	}
}

// GenerateAndSaveHostKey generates an ED25519 host key and saves it to the given file path
// with 0600 permissions.
func GenerateAndSaveHostKey(path string) (ssh.Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating host key: %w", err)
	}

	derBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("marshaling host key: %w", err)
	}

	pemBlock := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: derBytes,
	}

	if err := os.WriteFile(path, pem.EncodeToMemory(pemBlock), 0600); err != nil {
		return nil, fmt.Errorf("writing host key to %s: %w", path, err)
	}

	return ssh.NewSignerFromKey(priv)
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

// ListenAndServe listens on the given address and serves SSH protocol requests.
func (s *Server) ListenAndServe(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	return s.Serve(listener)
}

// handleConnection handles a single SSH connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	serverConn, chans, reqs, err := ssh.NewServerConn(conn, s.config)
	if err != nil {
		log.Printf("ssh protocol: handshake failed: %v\n", err)
		return
	}
	defer serverConn.Close()

	// Discard global requests
	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			continue
		}

		channel, requests, err := newChannel.Accept()
		if err != nil {
			log.Printf("ssh protocol: could not accept channel: %v\n", err)
			return
		}

		go s.handleSession(channel, requests)
	}
}

// handleSession handles an SSH session channel.
func (s *Server) handleSession(channel ssh.Channel, requests <-chan *ssh.Request) {
	defer channel.Close()

	for req := range requests {
		switch req.Type {
		case "exec":
			if len(req.Payload) < 4 {
				_ = req.Reply(false, nil)
				continue
			}

			// Payload format: uint32 length + string
			cmdLen := int(req.Payload[0])<<24 | int(req.Payload[1])<<16 | int(req.Payload[2])<<8 | int(req.Payload[3])
			if cmdLen+4 > len(req.Payload) {
				_ = req.Reply(false, nil)
				continue
			}
			cmdLine := string(req.Payload[4 : 4+cmdLen])

			service, repoPath, err := parseCommand(cmdLine)
			if err != nil {
				log.Printf("ssh protocol: invalid command: %v\n", err)
				_ = req.Reply(false, nil)
				continue
			}

			_ = req.Reply(true, nil)
			s.executeCommand(channel, service, repoPath)
			return

		default:
			// Reject unknown request types
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
		}
	}
}

// executeCommand runs a git service command and pipes I/O through the SSH channel.
func (s *Server) executeCommand(channel ssh.Channel, service string, repoPath string) {
	defer channel.Close()

	fullPath := repository.ResolvePath(s.repositoriesDir, repoPath)
	if fullPath == "" || !repository.IsRepository(fullPath) {
		log.Printf("ssh protocol: repository not found: %s\n", repoPath)
		sendExitStatus(channel, 1)
		return
	}

	if service == "git-receive-pack" {
		repo, err := repository.Open(fullPath)
		if err != nil {
			log.Printf("ssh protocol: failed to open repository: %v\n", err)
			sendExitStatus(channel, 1)
			return
		}
		isMirror, _, err := repo.IsMirror()
		if err != nil {
			log.Printf("ssh protocol: failed to check repository type: %v\n", err)
			sendExitStatus(channel, 1)
			return
		}
		if isMirror {
			log.Printf("ssh protocol: push to mirror repository %q is not allowed\n", repoPath)
			sendExitStatus(channel, 1)
			return
		}
	}

	ctx := context.Background()
	cmd := utils.Command(ctx, service, fullPath)
	cmd.Stdin = channel
	cmd.Stdout = channel
	cmd.Stderr = channel.Stderr()

	if err := cmd.Run(); err != nil {
		log.Printf("ssh protocol: command %s failed: %v\n", service, err)
		sendExitStatus(channel, 1)
		return
	}

	sendExitStatus(channel, 0)
}

// parseCommand parses an SSH exec command like "git-upload-pack '/repo.git'" or
// "git-upload-pack /repo.git".
func parseCommand(cmdLine string) (service string, repoPath string, err error) {
	parts := strings.SplitN(strings.TrimSpace(cmdLine), " ", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid command format: %q", cmdLine)
	}

	service = parts[0]
	if service != "git-upload-pack" && service != "git-receive-pack" {
		return "", "", fmt.Errorf("unsupported service: %s", service)
	}

	repoPath = parts[1]
	// Remove surrounding single quotes if present (git client sends 'path')
	repoPath = strings.Trim(repoPath, "'")

	return service, repoPath, nil
}

// sendExitStatus sends the exit status to the SSH client.
func sendExitStatus(channel ssh.Channel, status uint32) {
	payload := []byte{
		byte(status >> 24),
		byte(status >> 16),
		byte(status >> 8),
		byte(status),
	}
	_, _ = channel.SendRequest("exit-status", false, payload)
}

// ParseHostKeyFile reads a PEM-encoded private key file and returns an SSH signer.
func ParseHostKeyFile(data []byte) (ssh.Signer, error) {
	signer, err := ssh.ParsePrivateKey(data)
	if err != nil {
		return nil, fmt.Errorf("parsing host key: %w", err)
	}
	return signer, nil
}
