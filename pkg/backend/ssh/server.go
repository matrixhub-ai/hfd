package ssh

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/wzshiming/hfd/internal/utils"
	"github.com/wzshiming/hfd/pkg/authenticate"
	"github.com/wzshiming/hfd/pkg/permission"
	"github.com/wzshiming/hfd/pkg/receive"
	"github.com/wzshiming/hfd/pkg/repository"
)

// Signer is an alias for ssh.Signer to avoid requiring callers to import golang.org/x/crypto/ssh.
type Signer = ssh.Signer

// PublicKey is an alias for ssh.PublicKey to avoid requiring callers to import golang.org/x/crypto/ssh.
type PublicKey = ssh.PublicKey

// Server implements the SSH protocol (ssh://) server for git operations.
type Server struct {
	repositoriesDir    string
	config             *ssh.ServerConfig
	proxyManager       *repository.ProxyManager
	permissionHook     permission.PermissionHook
	preReceiveHook     receive.PreReceiveHook
	postReceiveHook    receive.PostReceiveHook
	tokenSignValidator authenticate.TokenSignValidator
	lfsURL             string
	logger             *slog.Logger
}

// Option configures the SSH server.
type Option func(*Server)

// WithLogger sets the logger for the SSH server.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Server) {
		s.logger = logger
	}
}

// WithPublicKeyCallback sets the public key authentication callback for the SSH server.
func WithPublicKeyCallback(callback func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error)) Option {
	return func(s *Server) {
		s.config.NoClientAuth = false
		s.config.PublicKeyCallback = callback
	}
}

// WithProxyManager sets the proxy manager for the SSH server.
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

// WithPreReceiveHookFunc sets the pre-receive hook called before a git push is processed.
// If the hook returns an error, the push is rejected.
func WithPreReceiveHookFunc(hook receive.PreReceiveHook) Option {
	return func(s *Server) {
		s.preReceiveHook = hook
	}
}

// WithPostReceiveHookFunc sets the post-receive hook called after a git push is processed.
// Errors from this hook are logged but do not affect the push result.
func WithPostReceiveHookFunc(hook receive.PostReceiveHook) Option {
	return func(s *Server) {
		s.postReceiveHook = hook
	}
}

// WithLFSURL sets the base HTTP URL for the server, used by git-lfs-authenticate
// to tell LFS clients the LFS API endpoint. For example: "http://localhost:8080".
func WithLFSURL(lfsURL string) Option {
	return func(s *Server) {
		s.lfsURL = lfsURL
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
			if user, _, ok := auth.Validate(context.Background(), conn.User(), string(password)); ok {
				return permissionsExtensions(user), nil
			}
			return nil, fmt.Errorf("invalid credentials")
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
			if user, _, ok := auth.Validate(context.Background(), conn.User(), key.Type(), key.Marshal()); ok {
				return permissionsExtensions(user), nil
			}
			return nil, fmt.Errorf("public key not authorized")
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

// NewServer creates a new SSH protocol server.
func NewServer(repositoriesDir string, hostKey ssh.Signer, opts ...Option) *Server {
	config := &ssh.ServerConfig{
		NoClientAuth: true,
	}

	s := &Server{
		repositoriesDir: repositoriesDir,
		config:          config,
		logger:          slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	config.AddHostKey(hostKey)

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
		s.logger.Warn("ssh protocol: handshake failed", "error", err)
		return
	}
	defer serverConn.Close()

	// Discard global requests
	go ssh.DiscardRequests(reqs)

	user := getUserFromPermissions(serverConn.Permissions)
	ctx := authenticate.WithContext(context.Background(), authenticate.UserInfo{User: user})

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			continue
		}

		channel, requests, err := newChannel.Accept()
		if err != nil {
			s.logger.Error("ssh protocol: could not accept channel", "error", err)
			return
		}

		go s.handleSession(ctx, channel, requests)
	}
}

// handleSession handles an SSH session channel.
func (s *Server) handleSession(ctx context.Context, channel ssh.Channel, requests <-chan *ssh.Request) {
	defer channel.Close()

	var env []string

	for req := range requests {
		switch req.Type {
		case "env":
			name, value, ok := parseSSHEnvRequest(req.Payload)
			if ok && name == "GIT_PROTOCOL" && repository.IsValidGitProtocol(value) {
				env = []string{"GIT_PROTOCOL=" + value}
			}
			// Reply true regardless of whether the variable was applied.
			// Replying false can cause some SSH clients to abort the session.
			// We silently ignore variables other than GIT_PROTOCOL.
			if req.WantReply {
				_ = req.Reply(true, nil)
			}

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

			cmd, err := parseCommand(cmdLine)
			if err != nil {
				s.logger.Error("ssh protocol: invalid command", "error", err)
				_ = req.Reply(false, nil)
				continue
			}

			_ = req.Reply(true, nil)

			switch cmd.service {
			case repository.GitLFSAuthenticate:
				s.executeLFSAuthenticate(ctx, channel, cmd.repoPath, cmd.operation)
			case repository.GitLFSTransfer:
				s.logger.Warn("ssh protocol: git-lfs-transfer is not supported, clients should fall back to git-lfs-authenticate")
				_, _ = fmt.Fprintf(channel.Stderr(), "git-lfs-transfer is not supported\n")
				sendExitStatus(channel, 1)
			default:
				s.executeCommand(ctx, channel, cmd.service, cmd.repoPath, env...)
			}
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
func (s *Server) executeCommand(ctx context.Context, channel ssh.Channel, service string, repoPath string, env ...string) {
	defer channel.Close()

	fullPath := repository.ResolvePath(s.repositoriesDir, repoPath)
	if fullPath == "" {
		s.logger.Error("ssh protocol: repository not found", "repo", repoPath)
		sendExitStatus(channel, 1)
		return
	}

	if s.permissionHook != nil {
		op := permission.OperationReadRepo
		if service == repository.GitReceivePack {
			op = permission.OperationUpdateRepo
		}
		if err := s.permissionHook(ctx, op, repoPath, permission.Context{}); err != nil {
			s.logger.Warn("ssh protocol: auth hook denied", "service", service, "repo", repoPath, "error", err)
			sendExitStatus(channel, 1)
			return
		}
	}

	repo, err := s.openRepo(ctx, fullPath, repoPath, service)
	if err != nil {
		s.logger.Error("ssh protocol: repository not found", "repo", repoPath)
		sendExitStatus(channel, 1)
		return
	}

	if service == repository.GitReceivePack {
		isMirror, _, err := repo.IsMirror()
		if err != nil {
			s.logger.Error("ssh protocol: failed to check repository type", "error", err)
			sendExitStatus(channel, 1)
			return
		}
		if isMirror {
			s.logger.Warn("ssh protocol: push to mirror repository is not allowed", "repo", repoPath)
			sendExitStatus(channel, 1)
			return
		}
	}

	// For receive-pack with permission/receive hooks: use pipe-based approach
	// to intercept pkt-line commands for permission checking before the push completes.
	if service == repository.GitReceivePack && (s.preReceiveHook != nil || s.postReceiveHook != nil) {
		s.executeReceivePackWithHooks(ctx, channel, service, repoPath, fullPath, env...)
		return
	}

	cmd := utils.Command(ctx, service, fullPath)
	cmd.Stdin = channel
	cmd.Stdout = channel
	cmd.Stderr = channel.Stderr()
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}

	if err := cmd.Run(); err != nil {
		s.logger.Error("ssh protocol: command failed", "service", service, "error", err)
		sendExitStatus(channel, 1)
		return
	}

	sendExitStatus(channel, 0)
}

// executeReceivePackWithHooks handles git-receive-pack using a pipe to intercept
// pkt-line ref update commands. This allows the permission hook to inspect and
// reject pushes before git-receive-pack processes the pack data.
//
// Flow:
//  1. Start git-receive-pack with io.Pipe as stdin; it sends ref advertisement
//     through stdout to the SSH channel.
//  2. Client receives the advertisement and sends pkt-line commands through the channel.
//  3. ParseRefUpdates reads the commands from the channel.
//  4. Permission hook runs — if denied, kill the git process.
//  5. If allowed, forward the remaining data through the pipe to git-receive-pack.
//  6. After completion, fire the receive hook.
func (s *Server) executeReceivePackWithHooks(ctx context.Context, channel ssh.Channel, service string, repoPath, fullPath string, env ...string) {
	pr, pw := io.Pipe()
	defer pr.Close()

	cmd := utils.Command(ctx, service, fullPath)
	cmd.Stdin = pr
	cmd.Stdout = channel
	cmd.Stderr = channel.Stderr()
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}

	if err := cmd.Start(); err != nil {
		pw.Close()
		s.logger.Error("ssh protocol: command start failed", "service", service, "error", err)
		sendExitStatus(channel, 1)
		return
	}

	// After git-receive-pack sends the ref advertisement through stdout→channel,
	// the client sends pkt-line commands back through the channel. ParseRefUpdates
	// reads these commands and returns a replay reader for forwarding.
	updates, replay := receive.ParseRefUpdates(channel)

	// Pre-receive hook — can reject the push before pack data is processed.
	if s.preReceiveHook != nil && len(updates) > 0 {
		if err := s.preReceiveHook(ctx, repoPath, updates); err != nil {
			s.logger.Warn("ssh protocol: pre-receive hook denied push", "repo", repoPath, "error", err)
			cmd.Process.Kill()
			pw.Close()
			_ = cmd.Wait() // expected error: process was killed
			sendExitStatus(channel, 1)
			return
		}
	}

	// Permission granted — forward the buffered pkt-line data and remaining
	// channel input to git-receive-pack through the pipe.
	go func() {
		defer pw.Close()
		if _, err := io.Copy(pw, replay); err != nil {
			s.logger.Warn("ssh protocol: error forwarding data to receive-pack", "repo", repoPath, "error", err)
		}
	}()

	if err := cmd.Wait(); err != nil {
		s.logger.Error("ssh protocol: command failed", "service", service, "error", err)
		sendExitStatus(channel, 1)
		return
	}

	// Fire post-receive hook with the ref updates.
	if s.postReceiveHook != nil && len(updates) > 0 {
		if hookErr := s.postReceiveHook(ctx, repoPath, updates); hookErr != nil {
			s.logger.Warn("ssh protocol: post-receive hook error", "repo", repoPath, "error", hookErr)
		}
	}

	sendExitStatus(channel, 0)
}

// openRepo opens a repository, optionally creating a mirror from the proxy source.
func (s *Server) openRepo(ctx context.Context, repoPath, repoName, service string) (*repository.Repository, error) {
	repo, err := repository.Open(repoPath)
	if err == nil {
		if mirror, _, err := repo.IsMirror(); err == nil && mirror {
			s.syncMirrorWithHook(ctx, repo, repoPath, repoName)
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
				return nil, err
			}
		}
		repo, err := s.proxyManager.Init(ctx, repoPath, repoName)
		if err != nil {
			return nil, err
		}
		s.fireHookForNewMirror(ctx, repo, repoPath, repoName)
		return repo, nil
	}
	return nil, err
}

// syncMirrorWithHook syncs a mirror and fires post-receive hooks for any ref changes.
func (s *Server) syncMirrorWithHook(ctx context.Context, repo *repository.Repository, repoPath, repoName string) {
	var before map[string]string
	if s.postReceiveHook != nil {
		before, _ = repo.Refs()
	}

	if err := repo.SyncMirror(ctx); err != nil {
		s.logger.Warn("failed to sync mirror", "repo", repoName, "error", err)
		return
	}

	if s.postReceiveHook != nil {
		after, _ := repo.Refs()
		updates := receive.DiffRefs(before, after)
		if len(updates) > 0 {
			if err := s.postReceiveHook(ctx, repoName, updates); err != nil {
				s.logger.Warn("post-receive hook error", "repo", repoName, "error", err)
			}
		}
	}
}

// fireHookForNewMirror fires post-receive hooks for all refs in a newly created mirror repo.
func (s *Server) fireHookForNewMirror(ctx context.Context, repo *repository.Repository, repoPath, repoName string) {
	if s.postReceiveHook == nil {
		return
	}
	after, _ := repo.Refs()
	updates := receive.DiffRefs(nil, after)
	if len(updates) > 0 {
		if err := s.postReceiveHook(ctx, repoName, updates); err != nil {
			s.logger.Warn("post-receive hook error", "repo", repoName, "error", err)
		}
	}
}

// lfsAuthResponse is the JSON response returned by git-lfs-authenticate.
type lfsAuthResponse struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header,omitempty"`
	ExpiresIn int               `json:"expires_in,omitempty"`
}

func lfsHref(httpURL, repoPath string) string {
	href := strings.TrimRight(httpURL, "/") + "/" + strings.TrimPrefix(repoPath, "/")
	if !strings.HasSuffix(href, ".git") {
		href += ".git"
	}
	href += "/info/lfs"
	return href
}

// executeLFSAuthenticate handles the git-lfs-authenticate command by returning
// a JSON response with the LFS API endpoint URL.
func (s *Server) executeLFSAuthenticate(ctx context.Context, channel ssh.Channel, repoPath string, operation string) {
	defer channel.Close()

	if s.lfsURL == "" {
		_, _ = fmt.Fprintf(channel.Stderr(), "LFS authentication is not configured on this server\n")
		sendExitStatus(channel, 1)
		return
	}

	if operation != "download" && operation != "upload" {
		s.logger.Error("ssh protocol: git-lfs-authenticate: invalid operation", "operation", operation)
		_, _ = fmt.Fprintf(channel.Stderr(), "invalid LFS operation: %s\n", operation)
		sendExitStatus(channel, 1)
		return
	}

	fullPath := repository.ResolvePath(s.repositoriesDir, repoPath)
	if fullPath == "" {
		s.logger.Error("ssh protocol: repository not found", "repo", repoPath)
		sendExitStatus(channel, 1)
		return
	}

	if s.permissionHook != nil {
		op := permission.OperationReadRepo
		if operation == "upload" {
			op = permission.OperationUpdateRepo
		}
		if err := s.permissionHook(ctx, op, repoPath, permission.Context{}); err != nil {
			s.logger.Warn("ssh protocol: auth hook denied lfs operation", "operation", operation, "repo", repoPath, "error", err)
			sendExitStatus(channel, 1)
			return
		}
	}

	// Build the LFS API href
	href := lfsHref(s.lfsURL, repoPath)

	resp := lfsAuthResponse{
		Href:      href,
		Header:    make(map[string]string),
		ExpiresIn: 3600,
	}

	// Include authentication headers when a token signer is configured,
	// so LFS clients can authenticate with the HTTP server.
	if s.tokenSignValidator != nil {
		userInfo, _ := authenticate.GetUserInfo(ctx)
		batchURL := href + "/objects/batch"
		if token := s.tokenSignValidator.Sign(ctx, http.MethodPost, batchURL, userInfo.User, time.Hour); token != "" {
			resp.Header["Authorization"] = "Bearer " + token
		}
	}

	data, err := json.Marshal(resp)
	if err != nil {
		s.logger.Error("ssh protocol: failed to marshal LFS auth response", "error", err)
		sendExitStatus(channel, 1)
		return
	}

	if _, err := channel.Write(data); err != nil {
		s.logger.Error("ssh protocol: failed to write LFS auth response", "error", err)
		sendExitStatus(channel, 1)
		return
	}

	sendExitStatus(channel, 0)
}

// parsedCommand holds the result of parsing an SSH exec command.
type parsedCommand struct {
	service   string
	repoPath  string
	operation string // only for git-lfs-authenticate and git-lfs-transfer
}

// parseCommand parses an SSH exec command like "git-upload-pack '/repo.git'",
// "git-upload-pack /repo.git", or "git-lfs-authenticate '/repo.git' download".
func parseCommand(cmdLine string) (*parsedCommand, error) {
	parts := strings.SplitN(strings.TrimSpace(cmdLine), " ", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid command format: %q", cmdLine)
	}

	service := parts[0]
	rest := parts[1]

	switch service {
	case repository.GitUploadPack, repository.GitReceivePack:
		repoPath := strings.Trim(rest, "'")
		return &parsedCommand{service: service, repoPath: repoPath}, nil

	case repository.GitLFSAuthenticate, repository.GitLFSTransfer:
		// Format: git-lfs-authenticate <path> <operation>
		// or: git-lfs-transfer <path> <operation>
		subParts := strings.SplitN(rest, " ", 2)
		if len(subParts) != 2 {
			return nil, fmt.Errorf("invalid %s format: %q", service, cmdLine)
		}
		repoPath := strings.Trim(subParts[0], "'")
		operation := strings.TrimSpace(subParts[1])
		return &parsedCommand{service: service, repoPath: repoPath, operation: operation}, nil

	default:
		return nil, fmt.Errorf("unsupported service: %s", service)
	}
}

// parseSSHEnvRequest parses an SSH "env" request payload and returns the variable
// name and value. The SSH wire format encodes each string as a uint32 length
// followed by the string bytes.
func parseSSHEnvRequest(payload []byte) (name, value string, ok bool) {
	if len(payload) < 4 {
		return "", "", false
	}
	nameLen := int(payload[0])<<24 | int(payload[1])<<16 | int(payload[2])<<8 | int(payload[3])
	if nameLen+4 > len(payload) {
		return "", "", false
	}
	name = string(payload[4 : 4+nameLen])
	rest := payload[4+nameLen:]
	if len(rest) < 4 {
		return "", "", false
	}
	valueLen := int(rest[0])<<24 | int(rest[1])<<16 | int(rest[2])<<8 | int(rest[3])
	if valueLen+4 > len(rest) {
		return "", "", false
	}
	value = string(rest[4 : 4+valueLen])
	return name, value, true
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
