package lfs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	"github.com/matrixhub-ai/hfd/pkg/permission"
)

const (
	contentMediaType = "application/vnd.git-lfs"
	metaMediaType    = contentMediaType + "+json"
)

// handleBatch provides the batch api
func (h *Handler) handleBatch(w http.ResponseWriter, r *http.Request) {
	bv := unpackBatch(r)

	op := permission.OperationReadRepo
	if bv.Operation == "upload" {
		op = permission.OperationUpdateRepo
	}
	repoName := bv.repoName()
	if !h.checkPermission(w, r, op, repoName) {
		return
	}

	var responseObjects []*lfsRepresentation

	// Select the xet transfer for uploads when the client advertises it and
	// the data plane can receive xorbs; uploaded objects land in xet storage.
	xetUpload := bv.Operation == "upload" && h.mirror != nil && h.mirror.CanMintToken() &&
		slices.ContainsFunc(bv.Transfers, func(tr string) bool { return strings.EqualFold(tr, "xet") })
	var casURL, casToken string
	var casExpiresAt time.Time
	if xetUpload {
		casURL, casToken, casExpiresAt = h.mirror.MintXETToken(r)
	}

	// Create a response object
	for _, object := range bv.Objects {
		if h.mirror != nil && h.mirror.KnowsObject(r.Context(), object.Oid) {
			responseObjects = append(responseObjects, h.lfsRepresent(r.Context(), bv.Operation, object, true, false))
			continue
		}

		// Object is not found
		if bv.Operation == "upload" {
			rep := h.lfsRepresent(r.Context(), bv.Operation, object, false, true)
			if xetUpload {
				addXETUploadHeaders(rep, casURL, casToken, casExpiresAt)
			}
			responseObjects = append(responseObjects, rep)
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

	transfer := "basic"
	if xetUpload {
		transfer = "xet"
	}
	respobj := &lfsBatchResponse{
		Transfer: transfer,
		Objects:  responseObjects,
	}

	responseJSON(w, respobj, http.StatusOK)
}

// addXETUploadHeaders attaches the CAS credentials the xet transfer reads
// from the upload action; the href stays as the basic fallback endpoint.
func addXETUploadHeaders(rep *lfsRepresentation, casURL, casToken string, expiresAt time.Time) {
	upload, ok := rep.Actions["upload"]
	if !ok {
		return
	}
	if upload.Header == nil {
		upload.Header = make(map[string]string)
	}
	upload.Header["X-Xet-Cas-Url"] = casURL
	upload.Header["X-Xet-Access-Token"] = casToken
	upload.Header["X-Xet-Token-Expiration"] = strconv.FormatInt(expiresAt.Unix(), 10)
	if upload.ExpiresAt.IsZero() {
		upload.ExpiresAt = expiresAt
	}
}

// handlePutContent receives data from the client and ingests it into the xet
// storage server-side; the OID and size are verified before anything lands.
func (h *Handler) handlePutContent(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if h.mirror == nil {
		responseJSON(w, "no object store configured", http.StatusNotImplemented)
		return
	}
	if err := h.mirror.PutObject(r.Context(), rv.Oid, r.Body, r.ContentLength); err != nil {
		responseJSON(w, fmt.Sprintf("failed to put LFS object %s: %v", rv.Oid, err), http.StatusInternalServerError)
		return
	}
}

// handleGetContent proxies the object bytes out of the xet storage; there is
// no presignable location for reconstructed files.
func (h *Handler) handleGetContent(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if h.mirror != nil {
		// Fully ingested objects serve straight from the xet storage, with
		// the hub metadata and xet Link headers.
		if rs, size, err := h.mirror.OpenObject(r.Context(), rv.Oid); err == nil {
			defer func() {
				_ = rs.Close()
			}()
			h.mirror.SetXETLinkHeaders(w, r, rv.Oid, size)
			w.Header().Set("Content-Type", "application/octet-stream")
			http.ServeContent(w, r, rv.Oid, time.Time{}, rs)
			return
		}
		// Objects known from a pull scan or a resolve delegate to the hub
		// front end, streaming while they ingest.
		if h.mirror.ServeOID(w, r, rv.Oid) {
			return
		}
	}
	responseJSON(w, fmt.Sprintf("LFS object %s not found", rv.Oid), http.StatusNotFound)
}

// handleVerifyObject confirms an upload completed. The content hash is the
// OID, so presence in the xet storage implies integrity and size.
func (h *Handler) handleVerifyObject(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if h.mirrorHasObject(r, rv.Oid) {
		return
	}
	responseJSON(w, fmt.Sprintf("LFS object %s not found", rv.Oid), http.StatusNotFound)
}

// mirrorHasObject reports whether the xet data plane holds the object; its
// content hash is the OID, so presence implies integrity.
func (h *Handler) mirrorHasObject(r *http.Request, oid string) bool {
	return h.mirror != nil && h.mirror.HasObject(r.Context(), oid)
}

const tokenExpiration = time.Hour

// lfsRepresent takes a RequestVars and Meta and turns it into a Representation suitable
// for json encoding. All content transfers are served by this server: the
// xet storage has no presignable location for whole objects.
func (h *Handler) lfsRepresent(ctx context.Context, op string, rv *lfsRequestVars, download, upload bool) *lfsRepresentation {
	rep := &lfsRepresentation{
		Oid:     rv.Oid,
		Size:    rv.Size,
		Actions: make(map[string]*lfsLink),
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
