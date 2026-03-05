package ssh

import (
	"testing"

	"github.com/wzshiming/hfd/pkg/repository"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name      string
		cmdLine   string
		service   string
		repoPath  string
		operation string
		wantErr   bool
	}{
		{
			name:     "git-upload-pack with quotes",
			cmdLine:  "git-upload-pack '/repo.git'",
			service:  repository.GitUploadPack,
			repoPath: "/repo.git",
		},
		{
			name:     "git-upload-pack without quotes",
			cmdLine:  "git-upload-pack /repo.git",
			service:  repository.GitUploadPack,
			repoPath: "/repo.git",
		},
		{
			name:     "git-receive-pack with quotes",
			cmdLine:  "git-receive-pack '/repo.git'",
			service:  repository.GitReceivePack,
			repoPath: "/repo.git",
		},
		{
			name:      "git-lfs-authenticate download",
			cmdLine:   "git-lfs-authenticate '/repo.git' download",
			service:   repository.GitLFSAuthenticate,
			repoPath:  "/repo.git",
			operation: "download",
		},
		{
			name:      "git-lfs-authenticate upload",
			cmdLine:   "git-lfs-authenticate '/repo.git' upload",
			service:   repository.GitLFSAuthenticate,
			repoPath:  "/repo.git",
			operation: "upload",
		},
		{
			name:      "git-lfs-authenticate without quotes",
			cmdLine:   "git-lfs-authenticate /user/model upload",
			service:   repository.GitLFSAuthenticate,
			repoPath:  "/user/model",
			operation: "upload",
		},
		{
			name:      "git-lfs-transfer download",
			cmdLine:   "git-lfs-transfer '/repo.git' download",
			service:   repository.GitLFSTransfer,
			repoPath:  "/repo.git",
			operation: "download",
		},
		{
			name:      "git-lfs-transfer upload",
			cmdLine:   "git-lfs-transfer '/repo.git' upload",
			service:   repository.GitLFSTransfer,
			repoPath:  "/repo.git",
			operation: "upload",
		},
		{
			name:    "unsupported service",
			cmdLine: "git-foo '/repo.git'",
			wantErr: true,
		},
		{
			name:    "no arguments",
			cmdLine: "git-upload-pack",
			wantErr: true,
		},
		{
			name:    "empty string",
			cmdLine: "",
			wantErr: true,
		},
		{
			name:    "git-lfs-authenticate missing operation",
			cmdLine: "git-lfs-authenticate '/repo.git'",
			wantErr: true,
		},
		{
			name:    "git-lfs-transfer missing operation",
			cmdLine: "git-lfs-transfer '/repo.git'",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := parseCommand(tt.cmdLine)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cmd.service != tt.service {
				t.Errorf("service = %q, want %q", cmd.service, tt.service)
			}
			if cmd.repoPath != tt.repoPath {
				t.Errorf("repoPath = %q, want %q", cmd.repoPath, tt.repoPath)
			}
			if cmd.operation != tt.operation {
				t.Errorf("operation = %q, want %q", cmd.operation, tt.operation)
			}
		})
	}
}
