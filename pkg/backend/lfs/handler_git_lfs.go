package lfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/lfs"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

const (
	contentMediaType = "application/vnd.git-lfs"
	metaMediaType    = contentMediaType + "+json"
)

// handleBatch provides the batch api
func (h *Handler) handleBatch(w http.ResponseWriter, r *http.Request) {
	bv := unpackBatch(r)

	if h.permissionHookFunc != nil {
		op := permission.OperationReadRepo
		if bv.Operation == "upload" {
			op = permission.OperationUpdateRepo
		}
		repoName := bv.repoName()
		if ok, err := h.permissionHookFunc(r.Context(), op, repoName, permission.Context{}); err != nil {
			responseJSON(w, err.Error(), http.StatusInternalServerError)
			return
		} else if !ok {
			responseJSON(w, "permission denied", http.StatusForbidden)
			return
		}
	}

	var responseObjects []*lfsRepresentation

	// Create a response object
	for _, object := range bv.Objects {
		if h.lfsStorage.Exists(object.Oid) {
			responseObjects = append(responseObjects, h.lfsRepresent(r.Context(), bv.Operation, object, true, false, true))
			continue
		}

		if h.mirror != nil {
			if pf := h.mirror.Get(object.Oid); pf != nil {
				responseObjects = append(responseObjects, h.lfsRepresent(r.Context(), bv.Operation, object, true, false, false))
				continue
			}
		}

		// Object is not found
		if bv.Operation == "upload" {
			responseObjects = append(responseObjects, h.lfsRepresent(r.Context(), bv.Operation, object, false, true, false))
		} else {
			rep := &lfsRepresentation{
				Oid:  object.Oid,
				Size: object.Size,
				Error: &lfsObjectError{
					Code:    404,
					Message: "Not found",
				},
			}
			responseObjects = append(responseObjects, rep)
		}
	}

	w.Header().Set("Content-Type", metaMediaType)

	respobj := &lfsBatchResponse{
		Transfer: "basic",
		Objects:  responseObjects,
	}

	responseJSON(w, respobj, http.StatusOK)
}

// handlePutContent receives data from the client and puts it into the content store.
// Presign-capable storages are handled at batch time; a client reaching this
// endpoint sends the content here, so store it directly.
func (h *Handler) handlePutContent(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if err := h.lfsStorage.Put(rv.Oid, r.Body, r.ContentLength); err != nil {
		responseJSON(w, fmt.Sprintf("failed to put LFS object %s: %v", rv.Oid, err), http.StatusInternalServerError)
		return
	}
}

// handleGetContent gets the content from the content store
func (h *Handler) handleGetContent(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if !h.lfsStorage.Exists(rv.Oid) {
		if h.mirror != nil {
			pf := h.mirror.Get(rv.Oid)
			if pf != nil {
				rs := pf.NewReadSeeker()
				defer rs.Close()
				http.ServeContent(w, r, rv.Oid, pf.ModTime(), rs)
				return
			}
		}
		responseJSON(w, fmt.Sprintf("LFS object %s not found", rv.Oid), http.StatusNotFound)
		return
	}
	if signer, ok := h.lfsStorage.(lfs.SignGetter); ok {
		url, _, err := signer.SignGet(rv.Oid)
		if err != nil {
			responseJSON(w, fmt.Sprintf("failed to sign URL for LFS object %q: %v", rv.Oid, err), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
		return
	}
	if getter, ok := h.lfsStorage.(lfs.Getter); ok {
		content, stat, err := getter.Get(rv.Oid)
		if err != nil {
			if os.IsNotExist(err) {
				responseJSON(w, fmt.Sprintf("LFS object %s not found", rv.Oid), http.StatusNotFound)
				return
			}
			responseJSON(w, fmt.Sprintf("failed to get LFS object %s: %v", rv.Oid, err), http.StatusInternalServerError)
			return
		}
		defer func() {
			_ = content.Close()
		}()

		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", rv.Oid))
		http.ServeContent(w, r, rv.Oid, stat.ModTime(), content)
		return
	}
	responseJSON(w, fmt.Sprintf("LFS storage does not support direct content retrieval for object %s", rv.Oid), http.StatusNotImplemented)
}

// handleVerifyObject confirms an upload completed. Staged presigned uploads
// are size-checked and moved into place (their hash rides in the signed
// checksum); Put uploads were hash-checked on write, so only presence and
// size are checked.
func (h *Handler) handleVerifyObject(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if verifier, ok := h.lfsStorage.(lfs.PutVerifier); ok {
		if err := verifier.VerifyPut(rv.Oid, rv.Size); err != nil {
			switch {
			case os.IsNotExist(err):
				responseJSON(w, fmt.Sprintf("LFS object %s not found", rv.Oid), http.StatusNotFound)
			case errors.Is(err, lfs.ErrSizeMismatch):
				responseJSON(w, "Size mismatch", http.StatusBadRequest)
			default:
				responseJSON(w, fmt.Sprintf("failed to verify LFS object %s: %v", rv.Oid, err), http.StatusInternalServerError)
			}
		}
		return
	}
	info, err := h.lfsStorage.Info(rv.Oid)
	if err != nil {
		if os.IsNotExist(err) {
			responseJSON(w, fmt.Sprintf("LFS object %s not found", rv.Oid), http.StatusNotFound)
			return
		}
		responseJSON(w, fmt.Sprintf("failed to get LFS object %s info: %v", rv.Oid, err), http.StatusInternalServerError)
		return
	}

	if info.Size() != rv.Size {
		responseJSON(w, "Size mismatch", http.StatusBadRequest)
		return
	}
}

const tokenExpiration = time.Hour

// lfsRepresent takes a RequestVars and Meta and turns it into a Representation suitable
// for json encoding. inStorage reports whether the object is already in the
// LFS storage (as opposed to only in the mirror cache).
func (h *Handler) lfsRepresent(ctx context.Context, op string, rv *lfsRequestVars, download, upload, inStorage bool) *lfsRepresentation {
	rep := &lfsRepresentation{
		Oid:     rv.Oid,
		Size:    rv.Size,
		Actions: make(map[string]*lfsLink),
	}

	user, _ := authenticate.GetUserInfo(ctx)

	if download && op == "download" {
		// hand out a presigned URL when the storage holds the object; objects
		// only in the mirror cache are served by this server's endpoint
		if signer, ok := h.lfsStorage.(lfs.SignGetter); ok && inStorage {
			if url, header, err := signer.SignGet(rv.Oid); err == nil {
				rep.Actions["download"] = &lfsLink{Href: url, Header: header, ExpiresAt: time.Now().Add(lfs.SignExpiry)}
				return rep
			} else {
				slog.WarnContext(ctx, "failed to presign LFS download link", "oid", rv.Oid, "error", err)
			}
		}
		link := rv.objectsLink()
		header := map[string]string{"Accept": contentMediaType}
		if h.tokenSignValidator != nil {
			if token, err := h.tokenSignValidator.Sign(ctx, http.MethodGet, link, user.User, tokenExpiration); err != nil {
				slog.WarnContext(ctx, "failed to sign token for LFS download link", "oid", rv.Oid, "error", err)
			} else if token != "" {
				header["Authorization"] = "Bearer " + token
			}
		} else if len(rv.Authorization) > 0 {
			header["Authorization"] = rv.Authorization
		}
		rep.Actions["download"] = &lfsLink{Href: link, Header: header}
	}

	if upload && op == "upload" {
		// the checksum-enforced grant requires its signed headers on the PUT;
		// clients that upload bare (hf-style) send the content here instead
		if signer, ok := h.lfsStorage.(lfs.SignPutter); ok && !rv.PlainPut {
			if url, header, err := signer.SignPut(rv.Oid); err == nil {
				rep.Actions["upload"] = &lfsLink{Href: url, Header: header, ExpiresAt: time.Now().Add(lfs.SignExpiry)}
				rep.Actions["verify"] = h.verifyAction(ctx, rv, user.User)
				return rep
			} else {
				slog.WarnContext(ctx, "failed to presign LFS upload link", "oid", rv.Oid, "error", err)
			}
		}
		link := rv.objectsLink()
		header := map[string]string{"Accept": contentMediaType}
		if h.tokenSignValidator != nil {
			if token, err := h.tokenSignValidator.Sign(ctx, http.MethodPut, link, user.User, tokenExpiration); err != nil {
				slog.WarnContext(ctx, "failed to sign token for LFS upload link", "oid", rv.Oid, "error", err)
			} else if token != "" {
				header["Authorization"] = "Bearer " + token
			}
		} else if len(rv.Authorization) > 0 {
			header["Authorization"] = rv.Authorization
		}
		rep.Actions["upload"] = &lfsLink{Href: link, Header: header}
		rep.Actions["verify"] = h.verifyAction(ctx, rv, user.User)
	}

	if len(rep.Actions) == 0 {
		rep.Actions = nil
	}

	return rep
}

// verifyAction builds the post-upload verify action, always served by this
// server so uploads are checked even when the content goes directly to S3.
func (h *Handler) verifyAction(ctx context.Context, rv *lfsRequestVars, user string) *lfsLink {
	verifyHeader := make(map[string]string)
	verifyLink := rv.verifyLink()
	if h.tokenSignValidator != nil {
		if token, err := h.tokenSignValidator.Sign(ctx, http.MethodPost, verifyLink, user, tokenExpiration); err != nil {
			slog.WarnContext(ctx, "failed to sign token for LFS verify link", "oid", rv.Oid, "error", err)
		} else if token != "" {
			verifyHeader["Authorization"] = "Bearer " + token
		}
	} else if len(rv.Authorization) > 0 {
		verifyHeader["Authorization"] = rv.Authorization
	}
	return &lfsLink{Href: verifyLink, Header: verifyHeader}
}

func unpack(r *http.Request) *lfsRequestVars {
	vars := mux.Vars(r)
	rv := &lfsRequestVars{
		Repo:          vars["repo"],
		Oid:           vars["oid"],
		Authorization: r.Header.Get("Authorization"),
	}

	if r.Method == http.MethodPost {
		var p lfsRequestVars
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&p)
		if err != nil {
			return rv
		}

		rv.Oid = p.Oid
		rv.Size = p.Size
	}

	return rv
}

func unpackBatch(r *http.Request) *lfsBatchVars {
	vars := mux.Vars(r)

	var bv lfsBatchVars

	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&bv)
	if err != nil {
		return &bv
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwdProto := r.Header.Get("X-Forwarded-Proto"); fwdProto != "" {
		scheme = fwdProto
	}
	origin := fmt.Sprintf("%s://%s", scheme, r.Host)

	// hf-style clients declare the "multipart" transfer and PUT without the
	// action headers, so checksum-enforced presigned uploads are unusable
	// for them
	plainPut := slices.Contains(bv.Transfers, "multipart")
	for i := range len(bv.Objects) {
		bv.Objects[i].Repo = vars["repo"]
		bv.Objects[i].Authorization = r.Header.Get("Authorization")
		bv.Objects[i].Origin = origin
		bv.Objects[i].PlainPut = plainPut
	}

	return &bv
}

// lfsRequestVars contain variables from the HTTP request. Variables from routing, json body decoding, and
// some headers are stored.
type lfsRequestVars struct {
	Origin string
	Oid    string
	Size   int64

	Repo          string
	Authorization string
	// PlainPut marks clients that upload without the action header (hf-style);
	// they upload to this server instead of a presigned URL.
	PlainPut bool `json:"-"`
}

func (v *lfsRequestVars) objectsLink() string {
	return fmt.Sprintf("%s/objects/%s", v.Origin, v.Oid)
}

func (v *lfsRequestVars) verifyLink() string {
	return fmt.Sprintf("%s/objects/%s/verify", v.Origin, v.Oid)
}

type lfsBatchVars struct {
	Transfers []string          `json:"transfers,omitempty"`
	Operation string            `json:"operation"`
	Objects   []*lfsRequestVars `json:"objects"`
}

func (bv *lfsBatchVars) repoName() string {
	if len(bv.Objects) == 0 {
		return ""
	}
	return bv.Objects[0].Repo
}

type lfsBatchResponse struct {
	Transfer string               `json:"transfer,omitempty"`
	Objects  []*lfsRepresentation `json:"objects"`
}

// lfsRepresentation is object medata as seen by clients of the lfs server.
type lfsRepresentation struct {
	Oid     string              `json:"oid"`
	Size    int64               `json:"size"`
	Actions map[string]*lfsLink `json:"actions,omitempty"`
	Error   *lfsObjectError     `json:"error,omitempty"`
}

type lfsObjectError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// lfsLink provides a structure used to build a hypermedia representation of an HTTP lfsLink.
type lfsLink struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header,omitempty"`
	ExpiresAt time.Time         `json:"expires_at,omitzero"`
}

// metaMatcher provides a mux.MatcherFunc that only allows requests that contain
// an Accept header with the metaMediaType
func metaMatcher(r *http.Request, m *mux.RouteMatch) bool {
	mediaParts := strings.Split(r.Header.Get("Accept"), ";")
	mt := mediaParts[0]
	return mt == metaMediaType
}
