package e2e_test

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func hfUploadFile(t *testing.T, s *e2eServer, repoID string, xet bool, data []byte) {
	t.Helper()
	src := filepath.Join(t.TempDir(), transferMatrixFile)
	if err := os.WriteFile(src, data, 0644); err != nil {
		t.Fatalf("write upload file: %v", err)
	}
	runHFCmdXet(t, s.httpURL, xet, "upload", repoID, src, transferMatrixFile, "--commit-message", "upload via hf cli")
}

func hfDownloadFile(t *testing.T, s *e2eServer, repoID string, xet bool) []byte {
	t.Helper()
	dir := t.TempDir()
	runHFCmdXet(t, s.httpURL, xet, "download", repoID, transferMatrixFile, "--local-dir", dir)
	got, err := os.ReadFile(filepath.Join(dir, transferMatrixFile))
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	return got
}

// TestUploadDownloadClientMatrix crosses every upload channel with every
// download channel: hf CLI over plain http and over xet-core (Rust hf_xet),
// git-lfs over HTTP and SSH, and the Go xet client. The request recorder
// proves the hf CLI channels used the protocol they claim instead of
// silently falling back: xet-core traffic must hit the CAS xorb/shard and
// reconstruction routes and plain http traffic must hit the basic transfer
// and bridge routes.
// TestMain repeats the matrix for local and S3 storage.
func TestUploadDownloadClientMatrix(t *testing.T) {
	requireUpDownMatrixTools(t)

	uploads := []struct {
		name   string
		repo   string
		upload func(t *testing.T, s *e2eServer, rec *requestRecorder, repoID string, data []byte)
	}{
		{
			name: "UpHFCliHTTP",
			repo: "matrix-org/updown-hf-http",
			upload: func(t *testing.T, s *e2eServer, rec *requestRecorder, repoID string, data []byte) {
				hfUploadFile(t, s, repoID, false, data)
				if rec.saw("", "/xorbs/") || rec.saw("", "/shards") {
					t.Fatal("plain hf upload wrote to the CAS; expected basic transfer only")
				}
				if !rec.saw(http.MethodPut, "/objects/") {
					t.Fatal("plain hf upload never PUT the basic transfer endpoint")
				}
			},
		},
		{
			name: "UpHFCliXetCore",
			repo: "matrix-org/updown-hf-xet",
			upload: func(t *testing.T, s *e2eServer, rec *requestRecorder, repoID string, data []byte) {
				hfUploadFile(t, s, repoID, true, data)
				// Chunk dedup queries are reads; only xorb/shard uploads prove a write.
				if !rec.saw("", "/xorbs/") && !rec.saw("", "/shards") {
					t.Fatalf("xet-core upload never wrote to the CAS; it fell back to another transfer\nrequests:\n%s", rec.dump())
				}
				if rec.saw(http.MethodPut, "/objects/") {
					t.Fatal("xet-core upload used the basic transfer PUT; expected CAS xorbs")
				}
			},
		},
		{
			name: "UpGitHTTPLFS",
			repo: "matrix-org/updown-git-http",
			upload: func(t *testing.T, s *e2eServer, _ *requestRecorder, repoID string, data []byte) {
				remote, env := s.httpRemote(repoID)
				pushViaGitLFS(t, s, remote, append(env, noProxyEnv()...), repoID, data)
			},
		},
		{
			name: "UpGitSSHLFS",
			repo: "matrix-org/updown-git-ssh",
			upload: func(t *testing.T, s *e2eServer, _ *requestRecorder, repoID string, data []byte) {
				remote, env := s.sshRemote(repoID)
				pushViaGitLFS(t, s, remote, append(env, noProxyEnv()...), repoID, data)
			},
		},
		{
			name: "UpXetGoBatch",
			repo: "matrix-org/updown-xet-go",
			upload: func(t *testing.T, s *e2eServer, _ *requestRecorder, repoID string, data []byte) {
				pushViaXetBatch(t, s, repoID, data)
			},
		},
	}

	for i, up := range uploads {
		t.Run(up.name, func(t *testing.T) {
			rec := &requestRecorder{}
			s := newE2EServer(t, withWrap(rec.wrap), withSSH())
			org, name, _ := strings.Cut(up.repo, "/")
			s.createRepo(t, org, name)

			data := makeBinaryData(128*1024, byte(40+i))

			rec.reset()
			up.upload(t, s, rec, up.repo, data)

			t.Run("DownHFCliHTTP", func(t *testing.T) {
				rec.reset()
				got := hfDownloadFile(t, s, up.repo, false)
				if !bytes.Equal(got, data) {
					t.Fatal("hf http download bytes mismatch")
				}
				if rec.saw(http.MethodGet, "reconstructions") {
					t.Fatal("plain hf download used the xet reconstruction API")
				}
				if !rec.saw(http.MethodGet, "/xet-bridge/") {
					t.Fatal("plain hf download never followed the resolve redirect to the bridge")
				}
			})
			t.Run("DownHFCliXetCore", func(t *testing.T) {
				rec.reset()
				got := hfDownloadFile(t, s, up.repo, true)
				if !bytes.Equal(got, data) {
					t.Fatal("hf xet-core download bytes mismatch")
				}
				// Fragment matches the v1, v2, and batch reconstruction routes.
				if !rec.saw(http.MethodGet, "reconstructions") {
					t.Fatalf("xet-core download never queried the reconstruction API; it fell back to http\nrequests:\n%s", rec.dump())
				}
				if rec.saw(http.MethodGet, "/xet-bridge/") {
					t.Fatal("xet-core download fetched bytes over the plain bridge")
				}
			})
			t.Run("DownGitHTTPLFSPull", func(t *testing.T) {
				remote, env := s.httpRemote(up.repo)
				verifyGitLFSPull(t, s, remote, append(env, noProxyEnv()...), up.repo, data)
			})
			t.Run("DownGitSSHLFSPull", func(t *testing.T) {
				remote, env := s.sshRemote(up.repo)
				verifyGitLFSPull(t, s, remote, append(env, noProxyEnv()...), up.repo, data)
			})
			t.Run("DownXetGoResolve", func(t *testing.T) {
				verifyHFResolveXet(t, s, up.repo, data)
			})
		})
	}
}
