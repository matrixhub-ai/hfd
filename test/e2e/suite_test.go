package e2e_test

import (
	"os"
	"testing"
)

const (
	suiteHTTP     = "http"
	suiteSSH      = "ssh"
	suiteProxy    = "proxy"
	suiteLFS      = "lfs"
	suiteHookHTTP = "hook-http"
	suiteHookSSH  = "hook-ssh"
	suiteHFCLI    = "hf-cli"
	suiteHFPython = "hf-python"
	suiteAll      = "all"
)

// requireSuite skips the test when E2E_SUITE is set and does not match any
// of the provided suite names. An unset E2E_SUITE or a value of "all" runs
// every suite.
func requireSuite(t *testing.T, suites ...string) {
	t.Helper()

	env := os.Getenv("E2E_SUITE")
	if env == "" || env == suiteAll {
		return
	}
	for _, suite := range suites {
		if env == suite {
			return
		}
	}
	t.Skipf("skipping suite %q (needs one of %v)", env, suites)
}
