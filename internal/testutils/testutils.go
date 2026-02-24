package testutils

import (
	"os"
	"strings"
	"testing"

	"github.com/wzshiming/gitd/internal/utils"
)

// RunGitCmd runs a git command in the specified directory.
func RunGitCmd(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := utils.Command(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Git command failed: git %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

// RunGitLFSCmd runs a git-lfs command in the specified directory.
func RunGitLFSCmd(t *testing.T, dir string, args ...string) string {
	t.Helper()
	fullArgs := append([]string{"lfs"}, args...)
	cmd := utils.Command(t.Context(), "git", fullArgs...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Git LFS command failed: git lfs %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

// RunHFCmd runs an hf (HuggingFace CLI) command and returns its output.
func RunHFCmd(t *testing.T, endpoint string, args ...string) string {
	t.Helper()
	cmd := utils.Command(t.Context(), "hf", args...)
	cmd.Env = append(os.Environ(), "HF_ENDPOINT="+endpoint, "HF_HUB_DISABLE_TELEMETRY=1")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("HF command failed: hf %s\nError: %v\nOutput: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}
