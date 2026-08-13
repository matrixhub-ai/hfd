package ssh

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"golang.org/x/crypto/ssh"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// Signer is an alias for ssh.Signer to avoid requiring callers to import golang.org/x/crypto/ssh.
type Signer = ssh.Signer

// PublicKey is an alias for ssh.PublicKey to avoid requiring callers to import golang.org/x/crypto/ssh.
type PublicKey = ssh.PublicKey

// Server implements the SSH protocol (ssh://) server for git operations.
type Server struct {
	storage             *storage.Storage
	config              *ssh.ServerConfig
	permissionHookFunc  permission.PermissionHookFunc
	preOpenHookFunc     PreOpenHookFunc
	preReceiveHookFunc  receive.PreReceiveHookFunc
	postReceiveHookFunc receive.PostReceiveHookFunc
	tokenSignValidator  authenticate.TokenSignValidator
	lfsURL              string
	mirror              *mirror.Mirror
}

// PreOpenHookFunc is called before opening a repository for a git service request.
type PreOpenHookFunc func(ctx context.Context, repoName string, write bool) error

// Option configures the SSH server.
type Option func(*Server)

// WithPublicKeyCallback sets the public key authentication callback for the SSH server.
func WithPublicKeyCallback(fn func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error)) Option {
	return func(s *Server) {
		s.config.NoClientAuth = false
		s.config.PublicKeyCallback = fn
	}
}

// WithPermissionHookFunc sets the permission hook for verifying operations.
func WithPermissionHookFunc(fn permission.PermissionHookFunc) Option {
	return func(s *Server) {
		s.permissionHookFunc = fn
	}
}

// WithPreOpenHookFunc sets a hook called before repository open.
func WithPreOpenHookFunc(fn PreOpenHookFunc) Option {
	return func(s *Server) {
		s.preOpenHookFunc = fn
	}
}

// WithPreReceiveHookFunc sets the pre-receive hook called before a git push is processed.
// If the hook returns an error, the push is rejected.
func WithPreReceiveHookFunc(fn receive.PreReceiveHookFunc) Option {
	return func(s *Server) {
		s.preReceiveHookFunc = fn
	}
}

// WithPostReceiveHookFunc sets the post-receive hook called after a git push is processed.
// Errors from this hook are logged but do not affect the push result.
func WithPostReceiveHookFunc(fn receive.PostReceiveHookFunc) Option {
	return func(s *Server) {
		s.postReceiveHookFunc = fn
	}
}

// WithLFSURL sets the base HTTP URL for the server, used by git-lfs-authenticate
// to tell LFS clients the LFS API endpoint. For example: "http://localhost:8080".
func WithLFSURL(lfsURL string) Option {
	return func(s *Server) {
		s.lfsURL = lfsURL
	}
}

// WithMirror sets the mirror to use for repository synchronization. If not provided,
// a mirror will be created when mirrorSourceFunc is set.
func WithMirror(m *mirror.Mirror) Option {
	return func(s *Server) {
		s.mirror = m
	}
}

func permissionsExtensions(user string) *ssh.Permissions {
	return &ssh.Permissions{
		Extensions: map[string]string{
			"x-user": user,
		},
	}
}

func getUserFromPermissions(perms *ssh.Permissions) string {
	if perms == nil {
		return authenticate.Anonymous
	}
	if user, ok := perms.Extensions["x-user"]; ok {
		return user
	}
	return authenticate.Anonymous
}

// WithBasicAuthValidator configures the SSH server to use the given validator
// for SSH password authentication.
func WithBasicAuthValidator(auth authenticate.BasicAuthValidator) Option {
	if auth == nil {
		return func(s *Server) {}
	}
	return func(s *Server) {
		s.config.NoClientAuth = false
		s.config.PasswordCallback = func(conn ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
			if user, _, ok, err := auth.Validate(context.Background(), conn.User(), string(password)); err != nil {
				slog.WarnContext(context.Background(), "password validation error", "error", err)
				return nil, fmt.Errorf("password validation error")
			} else if ok {
				return permissionsExtensions(user), nil
			}
			return nil, fmt.Errorf("invalid username or password")
		}
	}
}

// WithPublicKeyValidator configures the SSH server to use the given validator
// for SSH public key authentication.
func WithPublicKeyValidator(auth authenticate.PublicKeyValidator) Option {
	if auth == nil {
		return func(s *Server) {}
	}
	return func(s *Server) {
		s.config.NoClientAuth = false
		s.config.PublicKeyCallback = func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if user, _, ok, err := auth.Validate(context.Background(), conn.User(), key.Type(), key.Marshal()); err != nil {
				slog.WarnContext(context.Background(), "public key validation error", "error", err)
				return nil, fmt.Errorf("public key validation error")
			} else if ok {
				return permissionsExtensions(user), nil
			}
			return nil, fmt.Errorf("invalid public key")
		}
	}
}

// WithTokenSignValidator configures the SSH server to include authentication
// headers in git-lfs-authenticate responses so that LFS clients can authenticate
// with the HTTP server.
func WithTokenSignValidator(auth authenticate.TokenSignValidator) Option {
	return func(s *Server) {
		s.tokenSignValidator = auth
	}
}

// WithStorage sets the storage backend for the server, which is used to resolve
func WithStorage(storage *storage.Storage) Option {
	return func(s *Server) {
		s.storage = storage
	}
}

// WithHostKey adds the given SSH host key to the server configuration.
func WithHostKey(hostKey ssh.Signer) Option {
	return func(s *Server) {
		s.config.AddHostKey(hostKey)
	}
}

// NewServer creates a new SSH protocol server.
func NewServer(opts ...Option) *Server {
	config := &ssh.ServerConfig{
		NoClientAuth: true,
	}

	s := &Server{
		config: config,
	}
	for _, opt := range opts {
		opt(s)
	}

	return s
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

// Serve accepts connections on the listener and handles them.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go s.handleConnection(ctx, conn)
	}
}

// ListenAndServe listens on the given address and serves SSH protocol requests.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	return s.Serve(ctx, listener)
}

// handleConnection handles a single SSH connection.
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	serverConn, chans, reqs, err := ssh.NewServerConn(conn, s.config)
	if err != nil {
		slog.WarnContext(ctx, "ssh protocol: handshake failed", "error", err)
		return
	}
	defer serverConn.Close()

	// Discard global requests
	go ssh.DiscardRequests(reqs)

	user := getUserFromPermissions(serverConn.Permissions)
	ctx = authenticate.WithContext(ctx, authenticate.UserInfo{User: user})

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			continue
		}

		channel, requests, err := newChannel.Accept()
		if err != nil {
			slog.ErrorContext(ctx, "ssh protocol: could not accept channel", "error", err)
			return
		}

		go s.handleSession(ctx, channel, requests)
	}
}

// handleSession handles an SSH session channel.
func (s *Server) handleSession(ctx context.Context, channel ssh.Channel, requests <-chan *ssh.Request) {
	defer channel.Close()

	var gitProtocol string

	for req := range requests {
		switch req.Type {
		case "env":
			var env setenvRequest
			err := ssh.Unmarshal(req.Payload, &env)
			if err != nil {
				_ = req.Reply(false, nil)
				slog.WarnContext(ctx, "ssh protocol: failed to parse env request", "error", err)
				sendExitStatus(channel, 1, "")
				return
			}
			switch env.Name {
			case "GIT_PROTOCOL":
				if repository.IsValidGitProtocol(env.Value) {
					gitProtocol = env.Value
				}
				_ = req.Reply(true, nil)
			default:
				_ = req.Reply(false, nil)
			}
		case "exec":
			var exec execMsg
			err := ssh.Unmarshal(req.Payload, &exec)
			if err != nil {
				_ = req.Reply(false, nil)
				slog.WarnContext(ctx, "ssh protocol: failed to parse env request", "error", err)
				sendExitStatus(channel, 1, "")
				return
			}

			cmd, err := parseCommand(exec.Command)
			if err != nil {
				_ = req.Reply(false, nil)
				slog.WarnContext(ctx, "ssh protocol: failed to parse exec command", "error", err)
				sendExitStatus(channel, 1, "")
				return
			}

			_ = req.Reply(true, nil)

			switch cmd.service {
			case repository.GitLFSAuthenticate:
				s.executeLFSAuthenticate(ctx, channel, cmd.repoName, cmd.operation)
			case repository.GitLFSTransfer:
				sendExitStatus(channel, 1, "git-lfs-transfer is not supported\n")
			default:
				s.executeCommand(ctx, channel, cmd.service, cmd.repoName, gitProtocol)
			}
			return
		case "auth-agent-req@openssh.com":
			_ = req.Reply(false, nil)
		default:
			_ = req.Reply(false, nil)
			sendExitStatus(channel, 1, "unsupported request type\n")
			return
		}
	}
}

// sendExitStatus sends the exit status to the SSH client.
func sendExitStatus(channel ssh.Channel, status uint32, msg string) {
	if msg != "" {
		_, _ = fmt.Fprint(channel.Stderr(), msg)
	}
	_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(&exitStatusMsg{Status: status}))
}

// exitStatusMsg copy from golang.org/x/crypto/ssh.exitStatusMsg
type exitStatusMsg struct {
	Status uint32
}

// setenvRequest copy from golang.org/x/crypto/ssh.setenvRequest
type setenvRequest struct {
	Name  string
	Value string
}

// execMsg copy from golang.org/x/crypto/ssh.execMsg
type execMsg struct {
	Command string
}
