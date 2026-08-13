package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/matrixhub-ai/hfd/internal/netutil"
	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/repository"
	xetclient "github.com/wzshiming/xet/client"
)

// PushToRemote pushes the given ref updates to the configured remote destination.
// It is typically called after a successful push to the local repository (post-receive hook)
// to keep the remote destination in sync with local changes.
// If mirrorDestinationFunc is not set, the function returns nil.
// A nil opts uses the Mirror's configured callbacks.
func (m *Mirror) PushToRemote(ctx context.Context, repoPath, repoName string, opts *PushOptions) error {
	if m.mirrorDestinationFunc == nil {
		return nil
	}

	var opt PushOptions
	if opts != nil {
		opt = *opts
	}

	if m.gitOutputFunc != nil {
		logctx := context.Background()
		ui, _ := authenticate.GetUserInfo(ctx)
		logctx = authenticate.WithContext(logctx, ui)
		opt.Output = m.gitOutputFunc(logctx, repoName)
	}

	isPushMirror, err := m.resolvePushDestination(ctx, repoName, &opt)
	if err != nil {
		return err
	}
	if !isPushMirror {
		return nil
	}

	repo, err := repository.Open(repoPath)
	if err != nil {
		return fmt.Errorf("failed to open repository: %w", err)
	}

	_, err, _ = m.pushGroup.Do(repoPath, func() (any, error) {
		if err := m.pushMirrorLFS(repo, opt.DestinationURL); err != nil {
			return nil, fmt.Errorf("failed to push LFS objects to remote: %w", err)
		}

		refspecs, prune := buildPushRefspecs(opt.Refs)
		if err := repo.PushMirrorRefs(ctx, opt.DestinationURL, refspecs, prune, opt.Output); err != nil {
			return nil, err
		}

		return nil, nil
	})
	return err
}

// resolvePushDestination fills in the destination URL and embedded credentials
// from the configured callbacks when not overridden by the caller. It returns
// false when the repository is not configured as a push mirror.
func (m *Mirror) resolvePushDestination(ctx context.Context, repoName string, opt *PushOptions) (bool, error) {
	if opt.DestinationURL == "" {
		destURL, isPushMirror, err := m.mirrorDestinationFunc(ctx, repoName)
		if err != nil {
			return false, err
		}
		if !isPushMirror {
			return false, nil
		}
		opt.DestinationURL = destURL
	}

	if opt.UserInfo == nil && m.syncUserInfoFunc != nil {
		userInfo, err := m.syncUserInfoFunc(ctx, repoName)
		if err != nil {
			return false, fmt.Errorf("failed to get sync user info: %w", err)
		}
		opt.UserInfo = userInfo
	}

	if opt.UserInfo != nil {
		u, err := url.Parse(opt.DestinationURL)
		if err != nil {
			return false, fmt.Errorf("failed to parse dest URL: %w", err)
		}
		u.User = opt.UserInfo
		opt.DestinationURL = u.String()
	}
	return true, nil
}

// buildPushRefspecs converts an optional ref list into force-push refspecs.
// An empty list means mirroring all branches and tags with pruning.
func buildPushRefspecs(refs []string) (refspecs []string, prune bool) {
	if len(refs) > 0 {
		for _, ref := range refs {
			refspecs = append(refspecs, "+"+ref+":"+ref)
		}
		return refspecs, false
	}
	return []string{
		"+refs/heads/*:refs/heads/*",
		"+refs/tags/*:refs/tags/*",
	}, true
}

// pushMirrorLFS uploads LFS objects referenced by the repository to the remote LFS endpoint.
func (m *Mirror) pushMirrorLFS(repo *repository.Repository, destURL string) error {
	if m.lfsStorage == nil {
		return nil
	}

	ctx := context.Background()

	getter, ok := m.lfsStorage.(lfs.Getter)
	if !ok {
		return nil
	}

	lfsPointers, err := repo.ScanLFSPointers()
	if err != nil {
		return fmt.Errorf("failed to scan LFS pointers: %w", err)
	}

	if len(lfsPointers) == 0 {
		return nil
	}

	objects := make([]lfs.LFSObject, 0, len(lfsPointers))
	for _, ptr := range lfsPointers {
		if m.lfsStorage.Exists(ptr.OID()) {
			objects = append(objects, lfs.LFSObject{Oid: ptr.OID(), Size: ptr.Size()})
		}
	}

	if len(objects) == 0 {
		return nil
	}

	lfsClient := lfs.NewClient(netutil.HTTPClient)

	// When XET is enabled and the xet client is initialized, advertise the xet transfer
	// protocol so the remote can select it. Fall back to a basic-only request on error.
	var batchResp *lfs.BatchResponse
	var xetC *xetclient.Client
	if m.enablePushXET && m.lfsTeeCache != nil {
		xetC = m.lfsTeeCache.xetClient
	}
	xetUpload := xetC != nil
	if xetUpload {
		batchResp, err = lfsClient.UploadBatch(ctx, destURL, lfs.TransferWithXETCapabilities, objects)
		if err != nil {
			return fmt.Errorf("failed to get LFS upload batch from remote with XET capabilities: %w", err)
		}

		if !strings.EqualFold(batchResp.Transfer, "xet") {
			xetUpload = false
		}
	} else {
		batchResp, err = lfsClient.UploadBatch(ctx, destURL, lfs.TransferCapabilities, objects)
		if err != nil {
			return fmt.Errorf("failed to get LFS upload batch from remote: %w", err)
		}
	}

	for _, obj := range batchResp.Objects {
		if obj.Error != nil {
			slog.WarnContext(ctx, "LFS push mirror: remote returned error for object", "oid", obj.Oid, "error", obj.Error)
			continue
		}

		uploadAction, ok := obj.Actions["upload"]
		if !ok {
			// Remote already has this object; skip.
			continue
		}

		if xetUpload {
			if err := m.doXETUpload(ctx, obj.Oid, uploadAction, obj.Actions["verify"], getter, xetC); err != nil {
				slog.WarnContext(ctx, "LFS push mirror: XET upload failed", "oid", obj.Oid, "error", err)
				continue
			}
			slog.InfoContext(ctx, "LFS push mirror: uploaded object via XET", "oid", obj.Oid)
			continue
		}

		if err := m.doBasicUpload(ctx, obj.Oid, uploadAction, obj.Actions, getter); err != nil {
			slog.WarnContext(ctx, "LFS push mirror: upload failed", "oid", obj.Oid, "error", err)
			continue
		}

		slog.InfoContext(ctx, "LFS push mirror: uploaded object", "oid", obj.Oid)
	}

	return nil
}

// doBasicUpload uploads an LFS object via the basic transfer protocol, then
// fires the optional verify action.
func (m *Mirror) doBasicUpload(ctx context.Context, oid string, uploadAction lfs.Action, actions map[string]lfs.Action, getter lfs.Getter) error {
	content, info, err := getter.Get(oid)
	if err != nil {
		return fmt.Errorf("read local object: %w", err)
	}

	req, err := uploadAction.UploadRequest(ctx, content, info.Size())
	if err != nil {
		_ = content.Close()
		return fmt.Errorf("build upload request: %w", err)
	}

	resp, err := netutil.HTTPClient.Do(req)
	_ = content.Close()
	if err != nil {
		return fmt.Errorf("upload object: %w", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("upload returned unexpected status %d", resp.StatusCode)
	}

	// If a verify action is present, call it.
	if verifyAction, ok := actions["verify"]; ok {
		verifyReq, err := verifyAction.Request(ctx)
		if err != nil {
			return fmt.Errorf("build verify request: %w", err)
		}
		verifyReq.Method = http.MethodPost
		verifyResp, err := netutil.HTTPClient.Do(verifyReq)
		if err != nil {
			return fmt.Errorf("verify object: %w", err)
		}
		_ = verifyResp.Body.Close()
	}

	return nil
}

// doXETUpload uploads an LFS object to the remote XET CAS using the credentials
// embedded in uploadAction.Header, then fires the optional verify action.
func (m *Mirror) doXETUpload(ctx context.Context, oid string, uploadAction, verifyAction lfs.Action, getter lfs.Getter, xetC *xetclient.Client) error {
	casURL := uploadAction.Header["X-Xet-Cas-Url"]
	casToken := uploadAction.Header["X-Xet-Access-Token"]

	provider := xetclient.StaticAuthProvider(casURL, casToken)

	content, _, err := getter.Get(oid)
	if err != nil {
		return fmt.Errorf("read local object: %w", err)
	}
	defer content.Close()

	if _, err := xetC.UploadFileWithAuthProvider(ctx, provider, content); err != nil {
		return fmt.Errorf("XET upload: %w", err)
	}

	if verifyAction.Href != "" {
		verifyReq, err := verifyAction.Request(ctx)
		if err != nil {
			slog.WarnContext(ctx, "LFS push mirror: failed to build XET verify request", "oid", oid, "error", err)
			return nil
		}
		verifyReq.Method = http.MethodPost
		verifyResp, err := netutil.HTTPClient.Do(verifyReq)
		if err != nil {
			slog.WarnContext(ctx, "LFS push mirror: failed to verify XET upload", "oid", oid, "error", err)
			return nil
		}
		_ = verifyResp.Body.Close()
	}

	return nil
}
