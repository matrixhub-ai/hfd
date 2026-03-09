package receive

import (
	"fmt"
	"os"
	"path/filepath"
)

const preReceiveScript = `#!/bin/sh
# pre-receive hook - invoked by git-receive-pack before updating refs.
# Reads ref updates from stdin, one per line:
#   <old-value> <new-value> <ref-name>
# Exit non-zero to reject the push.
exit 0
`

const postReceiveScript = `#!/bin/sh
# post-receive hook - invoked by git-receive-pack after updating refs.
# Reads ref updates from stdin, one per line:
#   <old-value> <new-value> <ref-name>
exit 0
`

// InstallHooks creates the pre-receive and post-receive hook scripts in the
// repository's hooks/ directory if they do not already exist.
// Existing hook scripts are not overwritten.
func InstallHooks(repoPath string) error {
	hooksDir := filepath.Join(repoPath, "hooks")
	if err := os.MkdirAll(hooksDir, 0755); err != nil {
		return fmt.Errorf("failed to create hooks directory: %w", err)
	}

	hooks := map[string]string{
		"pre-receive":  preReceiveScript,
		"post-receive": postReceiveScript,
	}

	for name, content := range hooks {
		path := filepath.Join(hooksDir, name)
		if _, err := os.Stat(path); err == nil {
			continue // Don't overwrite existing hooks
		}
		if err := os.WriteFile(path, []byte(content), 0755); err != nil {
			return fmt.Errorf("failed to write %s hook: %w", name, err)
		}
	}
	return nil
}
