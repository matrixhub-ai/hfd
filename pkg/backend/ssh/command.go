package ssh

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// parsedCommand holds the result of parsing an SSH exec command.
type parsedCommand struct {
	service   string
	repoName  string
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
		repoName := strings.Trim(rest, "'")
		return &parsedCommand{service: service, repoName: repoName}, nil

	case repository.GitLFSAuthenticate, repository.GitLFSTransfer:
		// Format: git-lfs-authenticate <path> <operation>
		// or: git-lfs-transfer <path> <operation>
		subParts := strings.SplitN(rest, " ", 2)
		if len(subParts) != 2 {
			return nil, fmt.Errorf("invalid %s format: %q", service, cmdLine)
		}
		repoName := strings.Trim(subParts[0], "'")
		operation := strings.TrimSpace(subParts[1])
		return &parsedCommand{service: service, repoName: repoName, operation: operation}, nil

	default:
		return nil, fmt.Errorf("unsupported service: %s", service)
	}
}

// executeCommand serves a git service in-process, reading and writing the SSH channel.
func (s *Server) executeCommand(ctx context.Context, channel ssh.Channel, service string, repoName string, gitProtocol string) {
	repoPath := s.storage.ResolvePath(repoName)
	if repoPath == "" {
		sendExitStatus(channel, 1, "repository not found\n")
		return
	}

	if !s.checkMirrorAccess(ctx, channel, repoName, service) {
		return
	}

	if s.permissionHookFunc != nil {
		op := permission.OperationReadRepo
		if service == repository.GitReceivePack {
			op = permission.OperationUpdateRepo
		}
		if ok, err := s.permissionHookFunc(ctx, op, repoName, permission.Context{}); err != nil {
			slog.WarnContext(ctx, "ssh protocol: permission hook error", "service", service, "repo", repoName, "error", err)
			sendExitStatus(channel, 1, "")
			return
		} else if !ok {
			sendExitStatus(channel, 1, "permission denied")
			return
		}
	}

	repo, err := s.openRepo(ctx, repoPath, repoName, service)
	if err != nil {
		if err == repository.ErrRepositoryNotExists {
			sendExitStatus(channel, 1, "repository not found\n")
			return
		}
		slog.WarnContext(ctx, "ssh protocol: failed to open repository", "repo", repoName, "error", err)
		sendExitStatus(channel, 1, "")
		return
	}

	err = repo.Serve(ctx, channel, service, gitProtocol, s.receivePackHooks(repoName))
	if err != nil {
		// A pre-receive rejection has already been reported to the client
		// in-protocol via report-status.
		if errors.Is(err, errPreReceiveDenied) {
			slog.WarnContext(ctx, "ssh protocol: pre-receive hook denied push", "repo", repoName, "error", err)
			sendExitStatus(channel, 1, "")
			return
		}
		slog.ErrorContext(ctx, "ssh protocol: command failed", "service", service, "error", err)
		sendExitStatus(channel, 1, "")
		return
	}

	sendExitStatus(channel, 0, "")
}

// checkMirrorAccess enforces mirror-only access rules, reporting the failure
// on the SSH channel. It returns true when the request may proceed.
func (s *Server) checkMirrorAccess(ctx context.Context, channel ssh.Channel, repoName, service string) bool {
	if s.mirror == nil {
		return true
	}
	switch service {
	case repository.GitUploadPack:
		isMirrorSrc, err := s.mirror.IsMirrorSource(ctx, repoName)
		if err != nil {
			slog.ErrorContext(ctx, "ssh protocol: failed to check mirror status", "repo", repoName, "error", err)
			sendExitStatus(channel, 1, "")
			return false
		}
		if !isMirrorSrc {
			slog.WarnContext(ctx, "ssh protocol: pull from mirror repository denied", "repo", repoName)
			sendExitStatus(channel, 1, "pull from mirror repository denied")
			return false
		}
	case repository.GitReceivePack:
		isMirrorDest, err := s.mirror.IsMirrorDestination(ctx, repoName)
		if err != nil {
			slog.ErrorContext(ctx, "ssh protocol: failed to check mirror destination status", "repo", repoName, "error", err)
			sendExitStatus(channel, 1, "")
			return false
		}
		if !isMirrorDest {
			slog.WarnContext(ctx, "ssh protocol: push to mirror destination repository denied", "repo", repoName)
			sendExitStatus(channel, 1, "push to mirror destination repository denied")
			return false
		}
	}
	return true
}

// errPreReceiveDenied marks a push rejected by the pre-receive hook; the
// rejection is reported to the client in-protocol via report-status.
var errPreReceiveDenied = errors.New("pre-receive hook denied the push")

// receivePackHooks wires the server's pre/post-receive hook functions into
// the go-git receive-pack serving path.
func (s *Server) receivePackHooks(repoName string) repository.ReceivePackHooks {
	var hooks repository.ReceivePackHooks
	if s.preReceiveHookFunc != nil {
		hooks.PreReceive = func(ctx context.Context, updates []receive.RefUpdate) error {
			if len(updates) == 0 {
				return nil
			}
			if ok, err := s.preReceiveHookFunc(ctx, repoName, updates); err != nil {
				return fmt.Errorf("%w: %v", errPreReceiveDenied, err)
			} else if !ok {
				return errPreReceiveDenied
			}
			return nil
		}
	}
	hooks.PostReceive = func(ctx context.Context, updates []receive.RefUpdate) {
		s.afterReceivePack(ctx, repoName, updates)
	}
	return hooks
}

func (s *Server) afterReceivePack(ctx context.Context, repoName string, updates []receive.RefUpdate) {
	if len(updates) == 0 {
		return
	}

	if s.postReceiveHookFunc != nil {
		if hookErr := s.postReceiveHookFunc(ctx, repoName, updates); hookErr != nil {
			slog.WarnContext(ctx, "ssh protocol: post-receive hook error", "repo", repoName, "error", hookErr)
		}
	}
}

func (s *Server) openRepo(ctx context.Context, repoPath, repoName, service string) (*repository.Repository, error) {
	if err := s.preOpenHook(ctx, repoName, service == repository.GitReceivePack); err != nil {
		return nil, err
	}
	return repository.Open(repoPath)
}

func (s *Server) preOpenHook(ctx context.Context, repoName string, write bool) error {
	if s.preOpenHookFunc == nil {
		return nil
	}
	return s.preOpenHookFunc(ctx, repoName, write)
}
