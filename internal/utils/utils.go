package utils

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"strings"
)

func Command(ctx context.Context, name string, args ...string) *exec.Cmd {
	slog.Default().Info("Executing command", "name", name, "args", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stderr = os.Stderr
	return cmd
}
