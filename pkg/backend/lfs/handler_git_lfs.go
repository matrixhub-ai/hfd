package lfs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
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

	// Negotiate transfer protocol
	transfer := negotiateTransfer(bv.Transfers, bv.Operation, h.lfsStorage)

	var responseObjects []*lfsRepresentation

	// Create a response object
	for _, object := range bv.Objects {
		if h.lfsStorage.Exists(object.Oid) {
			responseObjects = append(responseObjects, h.lfsRepresent(r.Context(), bv.Operation, object, true, false, transfer))
			continue
		}

		if h.mirror != nil {
			if pf := h.mirror.Get(object.Oid); pf != nil {
				responseObjects = append(responseObjects, h.lfsRepresent(r.Context(), bv.Operation, object, true, false, transfer))
				continue
			}
		}

		// Object is not found
		if bv.Operation == "upload" {
			responseObjects = append(responseObjects, h.lfsRepresent(r.Context(), bv.Operation, object, false, true, transfer))
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
		Transfer: transfer,
		Objects:  responseObjects,
	}

	responseJSON(w, respobj, http.StatusOK)
}

// negotiateTransfer determines which transfer protocol to use based on client request and server capabilities.
func negotiateTransfer(requestedTransfers []string, operation string, storage lfs.Storage) string {
	// Default to basic if no transfers requested
	if len(requestedTransfers) == 0 {
		return "basic"
	}

	// Check if multipart is supported by storage
	_, supportsMultipart := storage.(lfs.SignMultipartPutter)

	// Only use multipart for uploads
	if operation == "upload" && supportsMultipart {
		// Check if client requested multipart
		for _, t := range requestedTransfers {
			if t == "multipart" {
				return "multipart"
			}
		}
	}

	// Fallback to basic if supported
	for _, t := range requestedTransfers {
		if t == "basic" {
			return "basic"
		}
	}

	// Default to basic
	return "basic"
}

// handlePutContent receives data from the client and puts it into the content store
func (h *Handler) handlePutContent(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if signer, ok := h.lfsStorage.(lfs.SignPutter); ok {
		url, err := signer.SignPut(rv.Oid)
		if err != nil {
			responseJSON(w, fmt.Sprintf("failed to sign URL for LFS object %q: %v", rv.Oid, err), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
		return
	}
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
		url, err := signer.SignGet(rv.Oid)
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

func (h *Handler) handleVerifyObject(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
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

// handleMultipartVerify completes a multipart upload by verifying all parts were uploaded successfully.
func (h *Handler) handleMultipartVerify(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	oid := vars["oid"]

	// Parse the request body
	var req struct {
		Oid    string         `json:"oid"`
		Size   int64          `json:"size"`
		Params map[string]any `json:"params"`
	}

	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		responseJSON(w, fmt.Sprintf("failed to decode request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate OID matches URL
	if req.Oid != oid {
		responseJSON(w, "OID mismatch between URL and request body", http.StatusBadRequest)
		return
	}

	// Extract upload_id and part_etags from params
	uploadID, ok := req.Params["upload_id"].(string)
	if !ok {
		responseJSON(w, "missing or invalid upload_id in params", http.StatusBadRequest)
		return
	}

	// Get part ETags from params
	partETagsRaw, ok := req.Params["part_etags"]
	if !ok {
		responseJSON(w, "missing part_etags in params", http.StatusBadRequest)
		return
	}

	// Parse part ETags map
	partETags := make(map[int]string)
	switch etags := partETagsRaw.(type) {
	case map[string]any:
		for partNumStr, etagVal := range etags {
			var partNum int
			if _, err := fmt.Sscanf(partNumStr, "%d", &partNum); err != nil {
				responseJSON(w, fmt.Sprintf("invalid part number: %s", partNumStr), http.StatusBadRequest)
				return
			}
			etag, ok := etagVal.(string)
			if !ok {
				responseJSON(w, fmt.Sprintf("invalid etag for part %d", partNum), http.StatusBadRequest)
				return
			}
			partETags[partNum] = etag
		}
	default:
		responseJSON(w, "invalid part_etags format", http.StatusBadRequest)
		return
	}

	// Complete the multipart upload
	multipartSigner, ok := h.lfsStorage.(lfs.SignMultipartPutter)
	if !ok {
		responseJSON(w, "multipart upload not supported by storage", http.StatusInternalServerError)
		return
	}

	if err := multipartSigner.CompleteMultipartUpload(oid, uploadID, partETags); err != nil {
		slog.ErrorContext(r.Context(), "failed to complete multipart upload", "oid", oid, "error", err)
		responseJSON(w, fmt.Sprintf("failed to complete multipart upload: %v", err), http.StatusConflict)
		return
	}

	// Verify the object exists and has the correct size
	info, err := h.lfsStorage.Info(oid)
	if err != nil {
		if os.IsNotExist(err) {
			responseJSON(w, fmt.Sprintf("LFS object %s not found after upload", oid), http.StatusConflict)
			return
		}
		responseJSON(w, fmt.Sprintf("failed to verify LFS object %s: %v", oid, err), http.StatusInternalServerError)
		return
	}

	if info.Size() != req.Size {
		responseJSON(w, fmt.Sprintf("Size mismatch: expected %d, got %d", req.Size, info.Size()), http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleMultipartAbort aborts a multipart upload and cleans up any uploaded parts.
func (h *Handler) handleMultipartAbort(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	oid := vars["oid"]

	// Parse the request body
	var req struct {
		Params map[string]any `json:"params"`
	}

	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		responseJSON(w, fmt.Sprintf("failed to decode request: %v", err), http.StatusBadRequest)
		return
	}

	// Extract upload_id from params
	uploadID, ok := req.Params["upload_id"].(string)
	if !ok {
		responseJSON(w, "missing or invalid upload_id in params", http.StatusBadRequest)
		return
	}

	// Abort the multipart upload
	multipartSigner, ok := h.lfsStorage.(lfs.SignMultipartPutter)
	if !ok {
		responseJSON(w, "multipart upload not supported by storage", http.StatusInternalServerError)
		return
	}

	if err := multipartSigner.AbortMultipartUpload(oid, uploadID); err != nil {
		slog.ErrorContext(r.Context(), "failed to abort multipart upload", "oid", oid, "error", err)
		responseJSON(w, fmt.Sprintf("failed to abort multipart upload: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

const tokenExpiration = time.Hour

// lfsRepresent takes a RequestVars and Meta and turns it into a Representation suitable
// for json encoding
func (h *Handler) lfsRepresent(ctx context.Context, op string, rv *lfsRequestVars, download, upload bool, transfer string) *lfsRepresentation {
	rep := &lfsRepresentation{
		Oid:     rv.Oid,
		Size:    rv.Size,
		Actions: make(map[string]any),
	}

	user, _ := authenticate.GetUserInfo(ctx)

	if download && op == "download" {
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
		// Use multipart transfer for large files if supported
		const multipartThreshold = 100 * 1024 * 1024 // 100MB
		if transfer == "multipart" && rv.Size >= multipartThreshold {
			if multipartSigner, ok := h.lfsStorage.(lfs.SignMultipartPutter); ok {
				multipartUpload, err := multipartSigner.SignMultipartPut(rv.Oid, rv.Size)
				if err != nil {
					slog.ErrorContext(ctx, "failed to initiate multipart upload", "oid", rv.Oid, "error", err)
					// Fallback to basic transfer
					rep.Actions["upload"] = h.createBasicUploadAction(ctx, rv, user)
				} else {
					// Create multipart actions
					parts := make([]lfsPartLink, len(multipartUpload.Parts))
					for i, part := range multipartUpload.Parts {
						parts[i] = lfsPartLink{
							Href:      part.URL,
							Header:    map[string]string{},
							Pos:       part.Pos,
							Size:      part.Size,
							ExpiresIn: int(tokenExpiration.Seconds()),
						}
					}
					rep.Actions["parts"] = parts

					// Create verify action with params
					verifyLink := rv.multipartVerifyLink()
					verifyHeader := make(map[string]string)
					if h.tokenSignValidator != nil {
						if token, err := h.tokenSignValidator.Sign(ctx, http.MethodPost, verifyLink, user.User, tokenExpiration); err != nil {
							slog.WarnContext(ctx, "failed to sign token for multipart verify link", "oid", rv.Oid, "error", err)
						} else if token != "" {
							verifyHeader["Authorization"] = "Bearer " + token
						}
					} else if len(rv.Authorization) > 0 {
						verifyHeader["Authorization"] = rv.Authorization
					}

					partNumbers := make([]int, len(multipartUpload.Parts))
					for i, part := range multipartUpload.Parts {
						partNumbers[i] = part.PartNumber
					}

					rep.Actions["verify"] = &lfsVerifyLink{
						Href:      verifyLink,
						Header:    verifyHeader,
						ExpiresIn: int(tokenExpiration.Seconds()),
						Params: map[string]any{
							"oid":        rv.Oid,
							"size":       rv.Size,
							"upload_id":  multipartUpload.UploadID,
							"part_count": len(multipartUpload.Parts),
						},
					}

					// Create abort action
					abortLink := rv.multipartAbortLink()
					abortHeader := make(map[string]string)
					if h.tokenSignValidator != nil {
						if token, err := h.tokenSignValidator.Sign(ctx, http.MethodPost, abortLink, user.User, tokenExpiration); err != nil {
							slog.WarnContext(ctx, "failed to sign token for multipart abort link", "oid", rv.Oid, "error", err)
						} else if token != "" {
							abortHeader["Authorization"] = "Bearer " + token
						}
					} else if len(rv.Authorization) > 0 {
						abortHeader["Authorization"] = rv.Authorization
					}

					rep.Actions["abort"] = &lfsAbortLink{
						Href:      abortLink,
						Header:    abortHeader,
						Method:    "POST",
						ExpiresIn: int(tokenExpiration.Seconds()),
					}
				}
			} else {
				// Storage doesn't support multipart, fallback to basic
				rep.Actions["upload"] = h.createBasicUploadAction(ctx, rv, user)
			}
		} else {
			// Use basic transfer for small files or when basic is requested
			rep.Actions["upload"] = h.createBasicUploadAction(ctx, rv, user)

			verifyHeader := make(map[string]string)
			verifyLink := rv.verifyLink()
			if h.tokenSignValidator != nil {
				if token, err := h.tokenSignValidator.Sign(ctx, http.MethodPost, verifyLink, user.User, tokenExpiration); err != nil {
					slog.WarnContext(ctx, "failed to sign token for LFS verify link", "oid", rv.Oid, "error", err)
				} else if token != "" {
					verifyHeader["Authorization"] = "Bearer " + token
				}
			} else if len(rv.Authorization) > 0 {
				verifyHeader["Authorization"] = rv.Authorization
			}
			rep.Actions["verify"] = &lfsLink{Href: verifyLink, Header: verifyHeader}
		}
	}

	if len(rep.Actions) == 0 {
		rep.Actions = nil
	}

	return rep
}

func (h *Handler) createBasicUploadAction(ctx context.Context, rv *lfsRequestVars, user authenticate.UserInfo) *lfsLink {
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
	return &lfsLink{Href: link, Header: header}
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

	for i := range len(bv.Objects) {
		bv.Objects[i].Repo = vars["repo"]
		bv.Objects[i].Authorization = r.Header.Get("Authorization")
		bv.Objects[i].Origin = origin
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
}

func (v *lfsRequestVars) objectsLink() string {
	return fmt.Sprintf("%s/objects/%s", v.Origin, v.Oid)
}

func (v *lfsRequestVars) verifyLink() string {
	return fmt.Sprintf("%s/objects/%s/verify", v.Origin, v.Oid)
}

func (v *lfsRequestVars) multipartVerifyLink() string {
	return fmt.Sprintf("%s/objects/%s/multipart/verify", v.Origin, v.Oid)
}

func (v *lfsRequestVars) multipartAbortLink() string {
	return fmt.Sprintf("%s/objects/%s/multipart/abort", v.Origin, v.Oid)
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
	Oid     string         `json:"oid"`
	Size    int64          `json:"size"`
	Actions map[string]any `json:"actions,omitempty"`
	Error   *lfsObjectError `json:"error,omitempty"`
}

type lfsObjectError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// lfsLink provides a structure used to build a hypermedia representation of an HTTP lfsLink.
type lfsLink struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header,omitempty"`
	ExpiresAt time.Time         `json:"expires_at,omitempty"`
	ExpiresIn int               `json:"expires_in,omitempty"`
}

// lfsPartLink represents a single part in a multipart upload.
type lfsPartLink struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header,omitempty"`
	Pos       int64             `json:"pos,omitempty"`
	Size      int64             `json:"size,omitempty"`
	ExpiresIn int               `json:"expires_in,omitempty"`
}

// lfsVerifyLink represents the verify action with additional params support.
type lfsVerifyLink struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header,omitempty"`
	ExpiresIn int               `json:"expires_in,omitempty"`
	Params    map[string]any    `json:"params,omitempty"`
}

// lfsAbortLink represents the abort action for multipart uploads.
type lfsAbortLink struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header,omitempty"`
	Method    string            `json:"method,omitempty"`
	ExpiresIn int               `json:"expires_in,omitempty"`
}

// metaMatcher provides a mux.MatcherFunc that only allows requests that contain
// an Accept header with the metaMediaType
func metaMatcher(r *http.Request, m *mux.RouteMatch) bool {
	mediaParts := strings.Split(r.Header.Get("Accept"), ";")
	mt := mediaParts[0]
	return mt == metaMediaType
}
