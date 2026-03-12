package repository

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/matrixhub-ai/hfd/internal/utils"
)

// Stateless performs a stateless Git operation (like upload-pack or receive-pack) for the repository.
// It executes the specified Git service command with the appropriate arguments and streams data through the provided input and output.
// If advertise is true, it sends the initial packet-line advertisement for the service.
// extraEnv contains optional additional environment variables (e.g. "GIT_PROTOCOL=version=2") to set on the git process.
func (r *Repository) Stateless(ctx context.Context, output io.Writer, input io.Reader, service string, advertise bool, extraEnv ...string) error {
	args := []string{"--stateless-rpc"}

	if advertise {
		// Write packet-line formatted header
		if _, err := output.Write(packetLine(fmt.Sprintf("# service=%s\n", service))); err != nil {
			return err
		}
		if _, err := output.Write([]byte("0000")); err != nil {
			return err
		}

		args = append(args, "--advertise-refs")
	}

	args = append(args, ".")

	cmd := utils.Command(ctx, service, args...)
	cmd.Dir = r.repoPath
	cmd.Stdin = input
	cmd.Stdout = output
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

// packetLine formats a string as a git packet-line.
func packetLine(s string) []byte {
	return fmt.Appendf(nil, "%04x%s", len(s)+4, s)
}
