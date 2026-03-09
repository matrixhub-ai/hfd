package receive

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	// EnvRepoName is set by the server so the hook script knows which
	// logical repository it is running in.
	EnvRepoName = "HFD_REPO_NAME"

	// EnvHookOutput is set by the server to tell the hook script where
	// to write the ref-update lines captured from stdin.
	EnvHookOutput = "HFD_HOOK_OUTPUT"
)

// postReceiveScript is the shell script installed as the post-receive hook.
// It reads ref-update lines from stdin (format: <old> <new> <refname>) and
// appends them to the file indicated by HFD_HOOK_OUTPUT.
const postReceiveScript = `#!/bin/sh
# hfd post-receive hook – captures ref updates for the server process.
if [ -n "$HFD_HOOK_OUTPUT" ]; then
    while read oldrev newrev refname; do
        echo "$oldrev $newrev $refname" >> "$HFD_HOOK_OUTPUT"
    done
fi
`

// InstallHooks writes the post-receive hook script into the hooks directory
// of a bare git repository.  Existing hooks are overwritten.
func InstallHooks(repoPath string) error {
	hooksDir := filepath.Join(repoPath, "hooks")
	if err := os.MkdirAll(hooksDir, 0755); err != nil {
		return fmt.Errorf("receive: creating hooks dir: %w", err)
	}
	hookPath := filepath.Join(hooksDir, "post-receive")
	if err := os.WriteFile(hookPath, []byte(postReceiveScript), 0755); err != nil {
		return fmt.Errorf("receive: writing post-receive hook: %w", err)
	}
	return nil
}
