package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// TestAPIHookMatrix checks API hooks through the authenticated HTTP chain.
func TestAPIHookMatrix(t *testing.T) {
	preRecorder, postRecorder := &matrixHookRecorder{}, &matrixHookRecorder{}
	// admissionCall is one PreOpen call (op "preopen") or one catalog gate call.
	type admissionCall struct {
		op, repoName, dest, user string
		write                    bool
	}
	var mu sync.Mutex
	var calls []admissionCall
	preAllow := true
	var preOpenErr, catalogErr, preErr error
	preOpen := func(ctx context.Context, repoName string, write bool) error {
		user := authenticate.IdentityFrom(ctx).Name()
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, admissionCall{op: "preopen", repoName: repoName, user: user, write: write})
		return preOpenErr
	}
	gate := func(ctx context.Context, op, repoName, dest string) error {
		user := authenticate.IdentityFrom(ctx).Name()
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, admissionCall{op: op, repoName: repoName, dest: dest, user: user})
		return catalogErr
	}
	pre := func(ctx context.Context, repoName string, updates []receive.RefUpdate) (bool, error) {
		_ = preRecorder.hook(ctx, repoName, updates)
		mu.Lock()
		defer mu.Unlock()
		return preAllow, preErr
	}
	s := newE2EServer(t, withAuth(authMatrixUser, authMatrixPass), withPreOpenHook(preOpen), withCatalogGate(gate), withHooks(pre, postRecorder.hook), withAPIHooks())
	reset := func(openErr, gateErr error, allowPre bool, receiveErr error) {
		mu.Lock()
		preOpenErr, catalogErr, preAllow, preErr = openErr, gateErr, allowPre, receiveErr
		calls = nil
		mu.Unlock()
		preRecorder.reset()
		postRecorder.reset()
	}
	admissionCalls := func() []admissionCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]admissionCall(nil), calls...)
	}
	request := func(t *testing.T, method, route, contentType, body string, basic bool, wantStatus int) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, s.httpURL+route, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Accept", contentType)
		if basic {
			req.SetBasicAuth(authMatrixUser, authMatrixPass)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		responseBody, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != wantStatus {
			t.Fatalf("%s %s status=%d want=%d body=%s read=%v calls=%+v pre=%+v post=%+v", method, route, resp.StatusCode, wantStatus, responseBody, err, admissionCalls(), preRecorder.getCalls(), postRecorder.getCalls())
		}
		return responseBody
	}
	assertAdmission := func(t *testing.T, want []admissionCall, body []byte) {
		t.Helper()
		if recorded := admissionCalls(); !slices.Equal(recorded, want) {
			t.Fatalf("admission calls = %+v, want %+v; body=%s", recorded, want, body)
		}
	}

	t.Run("HFCommit/PreReceiveAllow", func(t *testing.T) {
		const repoID = "api-hook-org/commit-pre-allow"
		s.createRepo(t, "api-hook-org", "commit-pre-allow")
		initial := postRecorder.getCalls()
		if len(initial) != 1 || initial[0].repoName != repoID || len(initial[0].updates) != 1 {
			t.Fatalf("create repo post-receive calls = %+v", initial)
		}
		previousHead := initial[0].updates[0].NewRev()
		if initial[0].updates[0].OldRev() != receive.ZeroHash || len(previousHead) != 40 || previousHead == receive.ZeroHash {
			t.Fatalf("create repo update: old=%q new=%q", initial[0].updates[0].OldRev(), previousHead)
		}
		preRecorder.reset()
		postRecorder.reset()
		mu.Lock()
		calls = nil
		mu.Unlock()

		body := "{\"key\":\"header\",\"value\":{\"summary\":\"API hook commit\"}}\n" +
			"{\"key\":\"file\",\"value\":{\"path\":\"README.md\",\"content\":\"API hooks\\n\",\"encoding\":\"utf-8\"}}\n"
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.httpURL+"/api/models/"+repoID+"/commit/main", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.SetBasicAuth(authMatrixUser, authMatrixPass)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		responseBody, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			CommitOid string `json:"commitOid"`
		}
		if err := json.Unmarshal(responseBody, &result); err != nil || resp.StatusCode != http.StatusOK || len(result.CommitOid) != 40 || result.CommitOid == previousHead || result.CommitOid == receive.ZeroHash {
			t.Fatalf("commit status=%d body=%s decode=%v previousHead=%q", resp.StatusCode, responseBody, err, previousHead)
		}
		assertAdmission(t, []admissionCall{{op: "preopen", repoName: repoID, user: authMatrixUser, write: true}}, responseBody)
		for _, phase := range []struct {
			name     string
			recorder *matrixHookRecorder
			newRev   string
		}{
			{name: "pre-receive", recorder: preRecorder},
			{name: "post-receive", recorder: postRecorder, newRev: result.CommitOid},
		} {
			received := phase.recorder.getCalls()
			if len(received) != 1 || received[0].repoName != repoID || len(received[0].updates) != 1 {
				t.Fatalf("%s calls=%+v; commit status=%d body=%s", phase.name, received, resp.StatusCode, responseBody)
			}
			update := received[0].updates[0]
			if update.RefName() != "refs/heads/main" || update.OldRev() != previousHead || (phase.newRev != "" && update.NewRev() != phase.newRev) {
				t.Fatalf("%s update: ref=%q old=%q new=%q; want ref=%q old=%q new=%q; commit status=%d body=%s", phase.name, update.RefName(), update.OldRev(), update.NewRev(), "refs/heads/main", previousHead, phase.newRev, resp.StatusCode, responseBody)
			}
		}
	})

	t.Run("HFCreateRepo", func(t *testing.T) {
		const repoID = "api-hook-org/create-deny"
		reset(nil, fs.ErrPermission, true, nil)
		body := request(t, http.MethodPost, "/api/repos/create", "application/json", `{"type":"model","name":"create-deny","organization":"api-hook-org"}`, true, http.StatusForbidden)
		assertAdmission(t, []admissionCall{{op: "create", repoName: repoID, user: authMatrixUser}}, body)
		recorded := admissionCalls()
		if !strings.Contains(string(body), "permission denied") || len(preRecorder.getCalls()) != 0 || len(postRecorder.getCalls()) != 0 {
			t.Fatalf("body=%s calls=%+v pre=%+v post=%+v", body, recorded, preRecorder.getCalls(), postRecorder.getCalls())
		}
		reset(nil, nil, true, nil)
		request(t, http.MethodGet, "/api/models/"+repoID, "application/json", "", true, http.StatusNotFound)
	})

	t.Run("HFDuplicateRepo/Allow", func(t *testing.T) {
		const repoID, destRepo = "api-hook-org/duplicate-allow", "api-hook-org/duplicate-allow-copy"
		reset(nil, nil, true, nil)
		s.createRepo(t, "api-hook-org", "duplicate-allow")
		// Create's post-receive proves the recorder is live, so an empty one after the copy is meaningful.
		initial := postRecorder.getCalls()
		if len(initial) != 1 || len(initial[0].updates) != 1 {
			t.Fatalf("create post-receive calls=%+v", initial)
		}
		reset(nil, nil, true, nil)
		body := request(t, http.MethodPost, "/api/models/"+repoID+"/duplicate", "application/json", fmt.Sprintf(`{"repository":%q}`, destRepo), true, http.StatusOK)
		assertAdmission(t, []admissionCall{
			{op: "preopen", repoName: repoID, user: authMatrixUser},
			{op: "duplicate", repoName: repoID, dest: destRepo, user: authMatrixUser},
		}, body)
		if preCalls, postCalls := preRecorder.getCalls(), postRecorder.getCalls(); len(preCalls) != 0 || len(postCalls) != 0 {
			t.Fatalf("pre-receive calls=%+v post-receive calls=%+v, want none; body=%s", preCalls, postCalls, body)
		}
	})

	t.Run("HFMoveRepo/Allow", func(t *testing.T) {
		const repoID, destRepo = "api-hook-org/move-allow", "api-hook-org/move-allow-moved"
		reset(nil, nil, true, nil)
		s.createRepo(t, "api-hook-org", "move-allow")
		// Create's post-receive proves the recorder is live, so an empty one after the move is meaningful.
		initial := postRecorder.getCalls()
		if len(initial) != 1 || len(initial[0].updates) != 1 {
			t.Fatalf("create post-receive calls=%+v", initial)
		}
		reset(nil, nil, true, nil)
		body := request(t, http.MethodPost, "/api/repos/move", "application/json", fmt.Sprintf(`{"fromRepo":%q,"toRepo":%q,"type":"model"}`, repoID, destRepo), true, http.StatusOK)
		assertAdmission(t, []admissionCall{
			{op: "preopen", repoName: repoID, user: authMatrixUser, write: true},
			{op: "move", repoName: repoID, dest: destRepo, user: authMatrixUser},
		}, body)
		if preCalls, postCalls := preRecorder.getCalls(), postRecorder.getCalls(); len(preCalls) != 0 || len(postCalls) != 0 {
			t.Fatalf("pre-receive calls=%+v post-receive calls=%+v, want none; body=%s", preCalls, postCalls, body)
		}
		request(t, http.MethodGet, "/api/models/"+destRepo, "application/json", "", true, http.StatusOK)
	})

	for _, row := range []struct {
		name       string
		preOpenErr error
		allowPre   bool
		preErr     error
		status     int
		message    string
		wantPre    int
	}{
		{name: "PreOpenDeny", preOpenErr: fs.ErrPermission, status: http.StatusForbidden, message: "permission denied"},
		{name: "PreReceiveDeny", status: http.StatusForbidden, message: "pre-receive hook denied the commit", wantPre: 1},
		{name: "HookError/PreOpen", preOpenErr: errors.New("matrix pre-open error"), status: http.StatusInternalServerError, message: "matrix pre-open error"},
		{name: "HookError/PreReceive", preErr: errors.New("matrix pre-receive error"), status: http.StatusInternalServerError, message: "matrix pre-receive error", wantPre: 1},
	} {
		t.Run("HFCommit/"+row.name, func(t *testing.T) {
			reset(nil, nil, true, nil)
			repoName := "commit-" + strings.ToLower(strings.ReplaceAll(row.name, "/", "-"))
			repoID := "api-hook-org/" + repoName
			s.createRepo(t, "api-hook-org", repoName)
			initial := postRecorder.getCalls()
			if len(initial) != 1 || len(initial[0].updates) != 1 {
				t.Fatalf("create post-receive calls=%+v", initial)
			}
			previousHead := initial[0].updates[0].NewRev()
			refsRoute := "/api/models/" + repoID + "/refs"
			before := request(t, http.MethodGet, refsRoute, "application/json", "", true, http.StatusOK)
			reset(row.preOpenErr, nil, row.allowPre, row.preErr)
			body := request(t, http.MethodPost, "/api/models/"+repoID+"/commit/main", "application/x-ndjson",
				"{\"key\":\"header\",\"value\":{\"summary\":\"Denied commit\"}}\n"+
					"{\"key\":\"file\",\"value\":{\"path\":\"README.md\",\"content\":\"must not land\\n\",\"encoding\":\"utf-8\"}}\n", true, row.status)
			assertAdmission(t, []admissionCall{{op: "preopen", repoName: repoID, user: authMatrixUser, write: true}}, body)
			recorded := admissionCalls()
			preCalls, postCalls := preRecorder.getCalls(), postRecorder.getCalls()
			if !strings.Contains(string(body), row.message) || len(preCalls) != row.wantPre || len(postCalls) != 0 {
				t.Fatalf("body=%s want=%q calls=%+v pre=%+v post=%+v", body, row.message, recorded, preCalls, postCalls)
			}
			if row.wantPre != 0 {
				if preCalls[0].repoName != repoID || len(preCalls[0].updates) != 1 {
					t.Fatalf("body=%s calls=%+v pre=%+v", body, recorded, preCalls)
				}
				update := preCalls[0].updates[0]
				if update.RefName() != "refs/heads/main" || update.OldRev() != previousHead {
					t.Fatalf("pre ref=%q old=%q new=%q want old=%q; body=%s calls=%+v", update.RefName(), update.OldRev(), update.NewRev(), previousHead, body, recorded)
				}
			}
			reset(nil, nil, true, nil)
			after := request(t, http.MethodGet, refsRoute, "application/json", "", true, http.StatusOK)
			if string(after) != string(before) {
				t.Fatalf("refs changed: before=%s after=%s body=%s calls=%+v", before, after, body, recorded)
			}
		})
	}

	for _, row := range []struct {
		name    string
		method  string
		route   string
		body    string
		write   bool
		catalog string
		dest    string
	}{
		{name: "HFCreateBranch", method: http.MethodPost, route: "/api/models/%s/branch/feature", body: `{}`, write: true},
		{name: "HFDeleteRepo", method: http.MethodDelete, route: "/api/repos/delete", catalog: "delete"},
		{name: "HFDuplicateRepo", method: http.MethodPost, route: "/api/models/%s/duplicate", body: `{"repository":"api-hook-org/hfduplicaterepo-copy"}`, catalog: "duplicate", dest: "api-hook-org/hfduplicaterepo-copy"},
		{name: "HFMoveRepo", method: http.MethodPost, route: "/api/repos/move", body: `{"fromRepo":"api-hook-org/hfmoverepo","toRepo":"api-hook-org/hfmoverepo-moved","type":"model"}`, write: true, catalog: "move", dest: "api-hook-org/hfmoverepo-moved"},
		{name: "HFResolve", method: http.MethodGet, route: "/%s/resolve/main/README.md"},
		{name: "LFSBatchDownload", method: http.MethodPost, route: "/%s.git/info/lfs/objects/batch"},
		{name: "LFSBatchUpload", method: http.MethodPost, route: "/%s.git/info/lfs/objects/batch", write: true},
		{name: "LFSLockCreate", method: http.MethodPost, route: "/%s.git/info/lfs/locks", body: `{"path":"model.bin"}`, write: true},
		{name: "LFSLocksList", method: http.MethodGet, route: "/%s.git/info/lfs/locks"},
		{name: "CASReadToken", method: http.MethodGet, route: "/api/models/%s/xet-read-token/main"},
		{name: "CASWriteToken", method: http.MethodGet, route: "/api/models/%s/xet-write-token/main", write: true},
	} {
		t.Run(row.name, func(t *testing.T) {
			reset(nil, nil, true, nil)
			repoName := strings.ToLower(row.name)
			repoID := "api-hook-org/" + repoName
			if row.name == "HFResolve" {
				seedAuthRepo(t, s, repoID)
			} else {
				s.createRepo(t, "api-hook-org", repoName)
			}
			route, payload := row.route, row.body
			if strings.Contains(route, "%s") {
				route = fmt.Sprintf(route, repoID)
			}
			mediaType := "application/json"
			if strings.HasPrefix(row.name, "LFS") {
				mediaType = "application/vnd.git-lfs+json"
			}
			if row.name == "HFDeleteRepo" {
				payload = fmt.Sprintf(`{"type":"model","name":%q,"organization":"api-hook-org"}`, repoName)
			}
			if after, ok := strings.CutPrefix(row.name, "LFSBatch"); ok {
				operation := strings.ToLower(after)
				payload = fmt.Sprintf(`{"operation":%q,"transfers":["basic"],"objects":[{"oid":%q,"size":64}]}`, operation, strings.Repeat("a", 64))
			}
			refsRoute := "/api/models/" + repoID + "/refs"
			before := request(t, http.MethodGet, refsRoute, "application/json", "", true, http.StatusOK)
			// Catalog rows are refused by the gate once PreOpen admitted the source; the others by PreOpen.
			if row.catalog != "" {
				reset(nil, fs.ErrPermission, true, nil)
			} else {
				reset(fs.ErrPermission, nil, true, nil)
			}
			basic, user := true, authMatrixUser
			if row.name == "HFResolve" {
				basic, user = false, "<anonymous>"
			}
			// Delete is judged by its callback alone.
			var want []admissionCall
			if row.catalog != "delete" {
				want = append(want, admissionCall{op: "preopen", repoName: repoID, user: user, write: row.write})
			}
			if row.catalog != "" {
				want = append(want, admissionCall{op: row.catalog, repoName: repoID, dest: row.dest, user: user})
			}
			body := request(t, row.method, route, mediaType, payload, basic, http.StatusForbidden)
			assertAdmission(t, want, body)
			recorded := admissionCalls()
			if !strings.Contains(string(body), "permission denied") || len(preRecorder.getCalls()) != 0 || len(postRecorder.getCalls()) != 0 {
				t.Fatalf("body=%s calls=%+v pre=%+v post=%+v", body, recorded, preRecorder.getCalls(), postRecorder.getCalls())
			}
			if strings.HasPrefix(row.name, "CAS") && strings.Contains(string(body), "accessToken") {
				t.Fatalf("denied token leaked: body=%s calls=%+v", body, recorded)
			}
			reset(nil, nil, true, nil)
			after := request(t, http.MethodGet, refsRoute, "application/json", "", true, http.StatusOK)
			if string(after) != string(before) {
				t.Fatalf("refs changed: before=%s after=%s body=%s calls=%+v", before, after, body, recorded)
			}
			switch row.name {
			case "HFCreateBranch":
				if strings.Contains(string(after), `"feature"`) {
					t.Fatalf("denied branch exists: refs=%s body=%s calls=%+v", after, body, recorded)
				}
				reset(nil, nil, true, nil)
				allowed := request(t, row.method, route, mediaType, payload, true, http.StatusOK)
				assertAdmission(t, want, allowed)
				refs := request(t, http.MethodGet, refsRoute, "application/json", "", true, http.StatusOK)
				if !strings.Contains(string(refs), `"feature"`) {
					t.Fatalf("allowed branch absent: refs=%s body=%s calls=%+v", refs, allowed, admissionCalls())
				}
			case "HFDeleteRepo":
				request(t, http.MethodGet, "/api/models/"+repoID, "application/json", "", true, http.StatusOK)
				reset(nil, nil, true, nil)
				allowed := request(t, row.method, route, mediaType, payload, true, http.StatusOK)
				assertAdmission(t, want, allowed)
				request(t, http.MethodGet, "/api/models/"+repoID, "application/json", "", true, http.StatusNotFound)
			case "HFDuplicateRepo":
				// The allowed copy is HFDuplicateRepo/Allow's.
				request(t, http.MethodGet, "/api/models/"+row.dest, "application/json", "", true, http.StatusNotFound)
			case "HFMoveRepo":
				// The allowed move is HFMoveRepo/Allow's.
				request(t, http.MethodGet, "/api/models/"+row.dest, "application/json", "", true, http.StatusNotFound)
			case "HFResolve":
				allowed := request(t, row.method, route, mediaType, payload, false, http.StatusOK)
				if string(allowed) != authMatrixReadme {
					t.Fatalf("resolve changed: allowed=%s denied=%s calls=%+v", allowed, body, recorded)
				}
			case "LFSLockCreate", "LFSLocksList":
				reset(nil, nil, true, nil)
				allowed := request(t, http.MethodGet, "/"+repoID+".git/info/lfs/locks", mediaType, "", true, http.StatusOK)
				assertAdmission(t, []admissionCall{{op: "preopen", repoName: repoID, user: authMatrixUser}}, allowed)
				var result struct {
					Locks []lfsLockEntry `json:"locks"`
				}
				if err := json.Unmarshal(allowed, &result); err != nil || len(result.Locks) != 0 {
					t.Fatalf("locks changed: body=%s decode=%v denied=%s calls=%+v", allowed, err, body, recorded)
				}
				verified := postLFSLocksVerify(t, s.httpURL, repoID)
				if len(verified.Ours) != 0 || len(verified.Theirs) != 0 {
					t.Fatalf("locks verify=%+v denied=%s calls=%+v", verified, body, recorded)
				}
			case "CASReadToken", "CASWriteToken":
				reset(nil, nil, true, nil)
				allowed := request(t, row.method, route, mediaType, payload, true, http.StatusOK)
				assertAdmission(t, want, allowed)
				var token struct {
					AccessToken string `json:"accessToken"`
				}
				if err := json.Unmarshal(allowed, &token); err != nil || token.AccessToken == "" {
					t.Fatalf("token body=%s decode=%v calls=%+v", allowed, err, admissionCalls())
				}
			}
		})
	}
}

// setupHookServer starts a harness server with the given option, creates the
// hook test repo, and returns the remote for the requested protocol.
func setupHookServer(t *testing.T, sshProto bool, opt e2eOption) (repoURL string, env []string) {
	t.Helper()
	opts := []e2eOption{opt}
	if sshProto {
		opts = append(opts, withSSH())
	}
	s := newE2EServer(t, opts...)
	s.createRepo(t, "hook-org", "hook-repo")
	if sshProto {
		return s.sshRemote("hook-org/hook-repo")
	}
	return s.httpRemote("hook-org/hook-repo")
}

// TestReceiveHooksMatrix tests receive hooks across HTTP and SSH protocols
func TestReceiveHooksMatrix(t *testing.T) {
	protocols := []struct {
		name string
		ssh  bool
	}{
		{name: "HTTP"},
		{name: "SSH", ssh: true},
	}

	type hookTest struct {
		name string
		test func(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder)
	}

	tests := []hookTest{
		{name: "BranchPush", test: testHookBranchPush},
		{name: "TagCreate", test: testHookTagCreate},
		{name: "TagDelete", test: testHookTagDelete},
		{name: "BranchCreateAndDelete", test: testHookBranchCreateDelete},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					recorder := &matrixHookRecorder{}
					repoURL, env := setupHookServer(t, protocol.ssh, withHooks(nil, recorder.hook))
					test.test(t, repoURL, env, recorder)
				})
			}
		})
	}
}

// TestPreReceiveHookDenyMatrix tests pre-receive hook denial across protocols
func TestPreReceiveHookDenyMatrix(t *testing.T) {
	protocols := []struct {
		name string
		ssh  bool
	}{
		{name: "HTTP"},
		{name: "SSH", ssh: true},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			preHook := func(ctx context.Context, repoName string, updates []receive.RefUpdate) (bool, error) {
				for _, e := range updates {
					if e.IsTag() {
						return false, nil
					}
				}
				return true, nil
			}

			postRecorder := &matrixHookRecorder{}
			repoURL, env := setupHookServer(t, protocol.ssh, withHooks(preHook, postRecorder.hook))

			clientDir, err := os.MkdirTemp("", "hook-deny-client")
			if err != nil {
				t.Fatalf("Failed to create temp client dir: %v", err)
			}
			defer os.RemoveAll(clientDir)

			// Clone and push commit (should succeed)
			cloneDir := filepath.Join(clientDir, "clone")
			runGit(t, "", env, "clone", repoURL, cloneDir)
			runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
			runGit(t, cloneDir, env, "config", "user.name", "Test User")

			if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			runGit(t, cloneDir, env, "add", "README.md")
			runGit(t, cloneDir, env, "commit", "-m", "Initial commit")
			runGit(t, cloneDir, env, "push", "origin", "main")

			// Tag push should be denied
			runGit(t, cloneDir, env, "tag", "v1.0")
			cmd := exec.CommandContext(t.Context(), "git", "push", "origin", "v1.0")
			cmd.Dir = cloneDir
			cmd.Env = append(testEnv(), env...)
			output, err := cmd.Output()
			if err == nil {
				t.Fatalf("Expected tag push to fail, but it succeeded: %s", output)
			}
		})
	}
}

func testHookBranchPush(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder) {
	clientDir, err := os.MkdirTemp("", "hook-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGit(t, "", env, "clone", repoURL, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")

	if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Test\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	runGit(t, cloneDir, env, "add", "README.md")
	runGit(t, cloneDir, env, "commit", "-m", "Initial commit")
	runGit(t, cloneDir, env, "push", "origin", "main")

	calls := recorder.getCalls()
	if len(calls) == 0 {
		t.Fatal("Expected receive hook to be called")
	}
	call := calls[len(calls)-1]
	if len(call.updates) == 0 {
		t.Fatal("Expected at least one ref update")
	}
	update := call.updates[0]
	if !update.IsBranch() {
		t.Errorf("Expected branch update, got ref %q", update.RefName())
	}
}

func testHookTagCreate(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder) {
	testHookBranchPush(t, repoURL, env, recorder)

	recorder.reset()

	clientDir, err := os.MkdirTemp("", "hook-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGit(t, "", env, "clone", repoURL, cloneDir)

	runGit(t, cloneDir, env, "tag", "v1.0")
	runGit(t, cloneDir, env, "push", "origin", "v1.0")

	calls := recorder.getCalls()
	if len(calls) == 0 {
		t.Fatal("Expected receive hook to be called for tag push")
	}
	call := calls[len(calls)-1]
	if len(call.updates) == 0 {
		t.Fatal("Expected at least one ref update for tag")
	}
	update := call.updates[0]
	if !update.IsTag() {
		t.Errorf("Expected tag update, got ref %q", update.RefName())
	}
	if !update.IsCreate() {
		t.Errorf("Expected tag create")
	}
}

func testHookTagDelete(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder) {
	testHookTagCreate(t, repoURL, env, recorder)

	recorder.reset()

	clientDir, err := os.MkdirTemp("", "hook-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGit(t, "", env, "clone", repoURL, cloneDir)

	runGit(t, cloneDir, env, "push", "origin", "--delete", "v1.0")

	calls := recorder.getCalls()
	if len(calls) == 0 {
		t.Fatal("Expected receive hook to be called for tag delete")
	}
	call := calls[len(calls)-1]
	if len(call.updates) == 0 {
		t.Fatal("Expected at least one ref update for tag delete")
	}
	update := call.updates[0]
	if !update.IsTag() {
		t.Errorf("Expected tag update, got ref %q", update.RefName())
	}
	if !update.IsDelete() {
		t.Errorf("Expected tag delete")
	}
}

func testHookBranchCreateDelete(t *testing.T, repoURL string, env []string, recorder *matrixHookRecorder) {
	testHookBranchPush(t, repoURL, env, recorder)

	recorder.reset()

	clientDir, err := os.MkdirTemp("", "hook-test-client")
	if err != nil {
		t.Fatalf("Failed to create temp client dir: %v", err)
	}
	defer os.RemoveAll(clientDir)

	cloneDir := filepath.Join(clientDir, "clone")
	runGit(t, "", env, "clone", repoURL, cloneDir)
	runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
	runGit(t, cloneDir, env, "config", "user.name", "Test User")

	runGit(t, cloneDir, env, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(cloneDir, "feature.txt"), []byte("feature\n"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	runGit(t, cloneDir, env, "add", "feature.txt")
	runGit(t, cloneDir, env, "commit", "-m", "Feature commit")
	runGit(t, cloneDir, env, "push", "origin", "feature")

	calls := recorder.getCalls()
	if len(calls) == 0 {
		t.Fatal("Expected receive hook for branch create")
	}
	call := calls[len(calls)-1]
	found := false
	for _, u := range call.updates {
		if u.IsBranch() && u.IsCreate() && u.Name() == "feature" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected branch create for 'feature' in updates: %+v", call.updates)
	}

	// Delete the branch
	recorder.reset()
	runGit(t, cloneDir, env, "checkout", "main")
	runGit(t, cloneDir, env, "push", "origin", "--delete", "feature")

	calls = recorder.getCalls()
	if len(calls) == 0 {
		t.Fatal("Expected receive hook for branch delete")
	}
	call = calls[len(calls)-1]
	found = false
	for _, u := range call.updates {
		if u.IsBranch() && u.IsDelete() && u.Name() == "feature" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected branch delete for 'feature' in updates: %+v", call.updates)
	}
}

// matrixHookRecorder records receive hook calls in a thread-safe manner (for matrix tests)
type matrixHookRecorder struct {
	mu    sync.Mutex
	calls []matrixHookCall
}

type matrixHookCall struct {
	repoName string
	updates  []receive.RefUpdate
}

func (r *matrixHookRecorder) hook(ctx context.Context, repoName string, updates []receive.RefUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, matrixHookCall{repoName: repoName, updates: updates})
	return nil
}

func (r *matrixHookRecorder) getCalls() []matrixHookCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]matrixHookCall, len(r.calls))
	copy(result, r.calls)
	return result
}

func (r *matrixHookRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

// TestPreOpenHookMatrix tests a PreOpen hook refusing writes across protocols
func TestPreOpenHookMatrix(t *testing.T) {
	protocols := []struct {
		name string
		ssh  bool
	}{
		{name: "HTTP"},
		{name: "SSH", ssh: true},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			readOnly := func(_ context.Context, _ string, write bool) error {
				if write {
					return fs.ErrPermission
				}
				return nil
			}

			repoURL, env := setupHookServer(t, protocol.ssh, withPreOpenHook(readOnly))

			clientDir, err := os.MkdirTemp("", "perm-test-client")
			if err != nil {
				t.Fatalf("Failed to create temp client dir: %v", err)
			}
			defer os.RemoveAll(clientDir)

			// Clone should succeed (read admitted)
			cloneDir := filepath.Join(clientDir, "clone")
			runGit(t, "", env, "clone", repoURL, cloneDir)

			// Push should fail (write refused)
			runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
			runGit(t, cloneDir, env, "config", "user.name", "Test User")

			if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			runGit(t, cloneDir, env, "add", "README.md")
			runGit(t, cloneDir, env, "commit", "-m", "Initial commit")

			// HTTP refuses the receive-pack discovery with 403; SSH reports the refusal on stderr.
			want := "403"
			if protocol.ssh {
				want = "permission denied"
			}
			if _, stderr, err := gitCmd(t, cloneDir, env, "push", "origin", "main"); err == nil || !strings.Contains(stderr, want) {
				t.Fatalf("push: err=%v, want a failure naming %q; stderr:\n%s", err, want, stderr)
			}
		})
	}
}
