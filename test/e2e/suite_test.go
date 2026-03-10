package e2e_test

import (
	"os"
	"strings"
	"testing"
)

const (
	suiteAll      = "all"
	suiteGitHTTP  = "git-http"
	suiteGitSSH   = "git-ssh"
	suiteGitLFS   = "git-lfs"
	suiteHFCLI    = "hf-cli"
	suiteHFPython = "hf-python"
)

// requireSuite skips the test when E2E_SUITE is set and does not include any of the provided suites.
// When E2E_SUITE is unset or "all", all tests run.
func requireSuite(t *testing.T, suites ...string) {
	t.Helper()

	selected := os.Getenv("E2E_SUITE")
	if selected == "" || selected == suiteAll {
		return
	}

	for _, raw := range strings.Split(selected, ",") {
		name := strings.TrimSpace(raw)
		if name == "" || name == suiteAll {
			return
		}
		for _, suite := range suites {
			if name == suite {
				return
			}
		}
	}

	t.Skipf("skipping suites %v (E2E_SUITE=%q)", suites, selected)
}
