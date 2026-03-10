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
	suiteHFCli    = "hf-cli"
	suiteHFPython = "hf-python"
)

// requireSuite skips the test when E2E_SUITE is set and none of the provided
// suites match. Multiple suites can be provided to share coverage across
// matrix entries. When E2E_SUITE is unset or "all", all suites are enabled.
func requireSuite(t *testing.T, suites ...string) {
	t.Helper()

	env := strings.TrimSpace(strings.ToLower(os.Getenv("E2E_SUITE")))
	if env == "" || env == suiteAll {
		return
	}

	active := map[string]struct{}{}
	for _, name := range strings.Split(env, ",") {
		name = strings.TrimSpace(strings.ToLower(name))
		if name != "" {
			active[name] = struct{}{}
		}
	}

	for _, suite := range suites {
		if _, ok := active[strings.ToLower(suite)]; ok {
			return
		}
	}

	t.Skipf("skipping e2e suite %v because E2E_SUITE=%q", suites, env)
}
