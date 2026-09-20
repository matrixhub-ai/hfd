package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// TestAPIHookMatrix checks API hooks through the authenticated HTTP chain.
func TestAPIHookMatrix(t *testing.T) {
	preRecorder, postRecorder := &matrixHookRecorder{}, &matrixHookRecorder{}
	type permissionCall struct {
		op       permission.Operation
		repoName string
		ctx      permission.Context
		user     string
	}
	var mu sync.Mutex
	var calls []permissionCall
	permissionAllow, preAllow := true, true
	var permissionErr, preErr error
	hook := func(ctx context.Context, op permission.Operation, repoName string, opCtx permission.Context) (bool, error) {
		user := authenticate.IdentityFrom(ctx).Name()
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, permissionCall{op: op, repoName: repoName, ctx: opCtx, user: user})
		return permissionAllow, permissionErr
	}
	pre := func(ctx context.Context, repoName string, updates []receive.RefUpdate) (bool, error) {
		_ = preRecorder.hook(ctx, repoName, updates)
		mu.Lock()
		defer mu.Unlock()
		return preAllow, preErr
	}
	s := newE2EServer(t, withAuth(authMatrixUser, authMatrixPass), withPermissionHook(hook), withHooks(pre, postRecorder.hook), withAPIHooks())
	reset := func(allow bool, hookErr error, allowPre bool, receiveErr error) {
		mu.Lock()
		permissionAllow, permissionErr, preAllow, preErr = allow, hookErr, allowPre, receiveErr
		calls = nil
		mu.Unlock()
		preRecorder.reset()
		postRecorder.reset()
	}
	permissionCalls := func() []permissionCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]permissionCall(nil), calls...)
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
			t.Fatalf("%s %s status=%d want=%d body=%s read=%v calls=%+v pre=%+v post=%+v", method, route, resp.StatusCode, wantStatus, responseBody, err, permissionCalls(), preRecorder.getCalls(), postRecorder.getCalls())
		}
		return responseBody
	}
	assertPermission := func(t *testing.T, op permission.Operation, repoID, ref, user string, body []byte) {
		t.Helper()
		recorded := permissionCalls()
		if len(recorded) != 1 || recorded[0].op != op || recorded[0].repoName != repoID || recorded[0].ctx.Ref != ref || recorded[0].ctx.DestRepo != "" || recorded[0].user != user {
			t.Fatalf("want op=%s repo=%q ref=%q user=%q; body=%s calls=%+v", op, repoID, ref, user, body, recorded)
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
		mu.Lock()
		permissionCalls := append([]permissionCall(nil), calls...)
		mu.Unlock()
		if len(permissionCalls) != 1 || permissionCalls[0].op != permission.OperationUpdateRepo || permissionCalls[0].repoName != repoID || permissionCalls[0].ctx.Ref != "main" || permissionCalls[0].user != authMatrixUser {
			t.Fatalf("commit permission calls = %+v", permissionCalls)
		}
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
		reset(false, nil, true, nil)
		body := request(t, http.MethodPost, "/api/repos/create", "application/json", `{"type":"model","name":"create-deny","organization":"api-hook-org"}`, true, http.StatusForbidden)
		assertPermission(t, permission.OperationCreateRepo, repoID, "", authMatrixUser, body)
		recorded := permissionCalls()
		if !strings.Contains(string(body), "permission denied") || len(preRecorder.getCalls()) != 0 || len(postRecorder.getCalls()) != 0 {
			t.Fatalf("body=%s calls=%+v pre=%+v post=%+v", body, recorded, preRecorder.getCalls(), postRecorder.getCalls())
		}
		reset(true, nil, true, nil)
		request(t, http.MethodGet, "/api/models/"+repoID, "application/json", "", true, http.StatusNotFound)
	})

	for _, row := range []struct {
		name          string
		allow         bool
		permissionErr error
		allowPre      bool
		preErr        error
		status        int
		message       string
		wantPre       int
	}{
		{name: "PermissionDeny", status: http.StatusForbidden, message: "permission denied"},
		{name: "PreReceiveDeny", allow: true, status: http.StatusForbidden, message: "pre-receive hook denied the commit", wantPre: 1},
		{name: "HookError/Permission", permissionErr: errors.New("matrix permission error"), status: http.StatusInternalServerError, message: "matrix permission error"},
		{name: "HookError/PreReceive", allow: true, preErr: errors.New("matrix pre-receive error"), status: http.StatusInternalServerError, message: "matrix pre-receive error", wantPre: 1},
	} {
		t.Run("HFCommit/"+row.name, func(t *testing.T) {
			reset(true, nil, true, nil)
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
			reset(row.allow, row.permissionErr, row.allowPre, row.preErr)
			body := request(t, http.MethodPost, "/api/models/"+repoID+"/commit/main", "application/x-ndjson",
				"{\"key\":\"header\",\"value\":{\"summary\":\"Denied commit\"}}\n"+
					"{\"key\":\"file\",\"value\":{\"path\":\"README.md\",\"content\":\"must not land\\n\",\"encoding\":\"utf-8\"}}\n", true, row.status)
			assertPermission(t, permission.OperationUpdateRepo, repoID, "main", authMatrixUser, body)
			recorded := permissionCalls()
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
			reset(true, nil, true, nil)
			after := request(t, http.MethodGet, refsRoute, "application/json", "", true, http.StatusOK)
			if string(after) != string(before) {
				t.Fatalf("refs changed: before=%s after=%s body=%s calls=%+v", before, after, body, recorded)
			}
		})
	}

	for _, row := range []struct {
		name   string
		method string
		route  string
		body   string
		op     permission.Operation
		ref    string
	}{
		{name: "HFCreateBranch", method: http.MethodPost, route: "/api/models/%s/branch/feature", body: `{}`, op: permission.OperationUpdateRepo, ref: "feature"},
		{name: "HFDeleteRepo", method: http.MethodDelete, route: "/api/repos/delete", op: permission.OperationDeleteRepo},
		{name: "HFResolve", method: http.MethodGet, route: "/%s/resolve/main/README.md", op: permission.OperationReadRepo},
		{name: "LFSBatchDownload", method: http.MethodPost, route: "/%s.git/info/lfs/objects/batch", op: permission.OperationReadRepo},
		{name: "LFSBatchUpload", method: http.MethodPost, route: "/%s.git/info/lfs/objects/batch", op: permission.OperationUpdateRepo},
		{name: "LFSLockCreate", method: http.MethodPost, route: "/%s.git/info/lfs/locks", body: `{"path":"model.bin"}`, op: permission.OperationUpdateRepo},
		{name: "LFSLocksList", method: http.MethodGet, route: "/%s.git/info/lfs/locks", op: permission.OperationReadRepo},
		{name: "CASReadToken", method: http.MethodGet, route: "/api/models/%s/xet-read-token/main", op: permission.OperationReadRepo, ref: "main"},
		{name: "CASWriteToken", method: http.MethodGet, route: "/api/models/%s/xet-write-token/main", op: permission.OperationUpdateRepo, ref: "main"},
	} {
		t.Run(row.name, func(t *testing.T) {
			reset(true, nil, true, nil)
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
			reset(false, nil, true, nil)
			basic, user := true, authMatrixUser
			if row.name == "HFResolve" {
				basic, user = false, "<anonymous>"
			}
			body := request(t, row.method, route, mediaType, payload, basic, http.StatusForbidden)
			assertPermission(t, row.op, repoID, row.ref, user, body)
			recorded := permissionCalls()
			if !strings.Contains(string(body), "permission denied") || len(preRecorder.getCalls()) != 0 || len(postRecorder.getCalls()) != 0 {
				t.Fatalf("body=%s calls=%+v pre=%+v post=%+v", body, recorded, preRecorder.getCalls(), postRecorder.getCalls())
			}
			if strings.HasPrefix(row.name, "CAS") && strings.Contains(string(body), "accessToken") {
				t.Fatalf("denied token leaked: body=%s calls=%+v", body, recorded)
			}
			reset(true, nil, true, nil)
			after := request(t, http.MethodGet, refsRoute, "application/json", "", true, http.StatusOK)
			if string(after) != string(before) {
				t.Fatalf("refs changed: before=%s after=%s body=%s calls=%+v", before, after, body, recorded)
			}
			switch row.name {
			case "HFCreateBranch":
				if strings.Contains(string(after), `"feature"`) {
					t.Fatalf("denied branch exists: refs=%s body=%s calls=%+v", after, body, recorded)
				}
				reset(true, nil, true, nil)
				allowed := request(t, row.method, route, mediaType, payload, true, http.StatusOK)
				assertPermission(t, row.op, repoID, row.ref, authMatrixUser, allowed)
				refs := request(t, http.MethodGet, refsRoute, "application/json", "", true, http.StatusOK)
				if !strings.Contains(string(refs), `"feature"`) {
					t.Fatalf("allowed branch absent: refs=%s body=%s calls=%+v", refs, allowed, permissionCalls())
				}
			case "HFDeleteRepo":
				request(t, http.MethodGet, "/api/models/"+repoID, "application/json", "", true, http.StatusOK)
				reset(true, nil, true, nil)
				allowed := request(t, row.method, route, mediaType, payload, true, http.StatusOK)
				assertPermission(t, row.op, repoID, "", authMatrixUser, allowed)
				request(t, http.MethodGet, "/api/models/"+repoID, "application/json", "", true, http.StatusNotFound)
			case "HFResolve":
				allowed := request(t, row.method, route, mediaType, payload, false, http.StatusOK)
				if string(allowed) != authMatrixReadme {
					t.Fatalf("resolve changed: allowed=%s denied=%s calls=%+v", allowed, body, recorded)
				}
			case "LFSLockCreate", "LFSLocksList":
				reset(true, nil, true, nil)
				allowed := request(t, http.MethodGet, "/"+repoID+".git/info/lfs/locks", mediaType, "", true, http.StatusOK)
				assertPermission(t, permission.OperationReadRepo, repoID, "", authMatrixUser, allowed)
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
				reset(true, nil, true, nil)
				allowed := request(t, row.method, route, mediaType, payload, true, http.StatusOK)
				assertPermission(t, row.op, repoID, row.ref, authMatrixUser, allowed)
				var token struct {
					AccessToken string `json:"accessToken"`
				}
				if err := json.Unmarshal(allowed, &token); err != nil || token.AccessToken == "" {
					t.Fatalf("token body=%s decode=%v calls=%+v", allowed, err, permissionCalls())
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

// TestPermissionHookMatrix tests permission hooks across protocols
func TestPermissionHookMatrix(t *testing.T) {
	protocols := []struct {
		name string
		ssh  bool
	}{
		{name: "HTTP"},
		{name: "SSH", ssh: true},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			permHook := func(ctx context.Context, op permission.Operation, repoName string, opCtx permission.Context) (bool, error) {
				// Deny write operations (only allow read)
				if !op.IsRead() {
					return false, nil
				}
				return true, nil
			}

			repoURL, env := setupHookServer(t, protocol.ssh, withPermissionHook(permHook))

			clientDir, err := os.MkdirTemp("", "perm-test-client")
			if err != nil {
				t.Fatalf("Failed to create temp client dir: %v", err)
			}
			defer os.RemoveAll(clientDir)

			// Clone should succeed (read permission)
			cloneDir := filepath.Join(clientDir, "clone")
			runGit(t, "", env, "clone", repoURL, cloneDir)

			// Push should fail (write denied)
			runGit(t, cloneDir, env, "config", "user.email", "test@test.com")
			runGit(t, cloneDir, env, "config", "user.name", "Test User")

			if err := os.WriteFile(filepath.Join(cloneDir, "README.md"), []byte("# Test\n"), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			runGit(t, cloneDir, env, "add", "README.md")
			runGit(t, cloneDir, env, "commit", "-m", "Initial commit")

			cmd := exec.CommandContext(t.Context(), "git", "push", "origin", "main")
			cmd.Dir = cloneDir
			cmd.Env = append(testEnv(), env...)
			output, err := cmd.Output()
			if err == nil {
				t.Fatalf("Expected push to fail due to permission hook, but it succeeded: %s", output)
			}
		})
	}
}
