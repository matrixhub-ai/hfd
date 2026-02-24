package lfs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

const (
	contentMediaType = "application/vnd.git-lfs"
	metaMediaType    = contentMediaType + "+json"
)

func (h *Handler) registryLFS(r *mux.Router) {
	r.HandleFunc("/{repo:.+}.git/info/lfs/objects/batch", h.handleBatch).Methods(http.MethodPost).MatcherFunc(metaMatcher)
	r.HandleFunc("/{repo:.+}/info/lfs/objects/batch", h.handleBatch).Methods(http.MethodPost).MatcherFunc(metaMatcher)
	r.HandleFunc("/objects/{oid}", h.handleGetContent).Methods(http.MethodGet, http.MethodHead)
	r.HandleFunc("/objects/{oid}", h.handlePutContent).Methods(http.MethodPut)
	r.HandleFunc("/objects/{oid}/verify", h.handleVerifyObject).Methods(http.MethodPost)
}

// handleBatch provides the batch api
func (h *Handler) handleBatch(w http.ResponseWriter, r *http.Request) {
	bv := unpackBatch(r)

	var responseObjects []*lfsRepresentation

	// Create a response object
	for _, object := range bv.Objects {
		var exists bool
		if h.storage.S3Store() != nil {
			fi, _ := h.storage.S3Store().Info(object.Oid)
			exists = fi != nil
		} else {
			exists = h.storage.ContentStore().Exists(object.Oid)
		}

		if exists { // Object is found and exists
			responseObjects = append(responseObjects, lfsRepresent(object, true, false))
			continue
		}

		// Object is not found
		if bv.Operation == "upload" {
			responseObjects = append(responseObjects, lfsRepresent(object, false, true))
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

// handlePutContent receives data from the client and puts it into the content store
func (h *Handler) handlePutContent(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if h.storage.S3Store() != nil {
		url, err := h.storage.S3Store().SignPut(rv.Oid)
		if err != nil {
			responseJSON(w, fmt.Sprintf("failed to sign S3 URL for LFS object %q: %v", rv.Oid, err), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
		return
	}
	if err := h.storage.ContentStore().Put(rv.Oid, r.Body, r.ContentLength); err != nil {
		responseJSON(w, fmt.Sprintf("failed to put LFS object %s: %v", rv.Oid, err), http.StatusInternalServerError)
		return
	}
}

// handleGetContent gets the content from the content store
func (h *Handler) handleGetContent(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if h.storage.S3Store() != nil {
		url, err := h.storage.S3Store().SignGet(rv.Oid)
		if err != nil {
			responseJSON(w, fmt.Sprintf("failed to sign S3 URL for LFS object %q: %v", rv.Oid, err), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
		return
	}
	content, stat, err := h.storage.ContentStore().Get(rv.Oid)
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
}

func (h *Handler) handleVerifyObject(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	if h.storage.S3Store() != nil {
		info, err := h.storage.S3Store().Info(rv.Oid)
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
		return
	}
	info, err := h.storage.ContentStore().Info(rv.Oid)
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

// lfsRepresent takes a RequestVars and Meta and turns it into a Representation suitable
// for json encoding
func lfsRepresent(rv *lfsRequestVars, download, upload bool) *lfsRepresentation {
	rep := &lfsRepresentation{
		Oid:     rv.Oid,
		Size:    rv.Size,
		Actions: make(map[string]*lfsLink),
	}

	header := make(map[string]string)
	verifyHeader := make(map[string]string)

	header["Accept"] = contentMediaType

	if len(rv.Authorization) > 0 {
		header["Authorization"] = rv.Authorization
		verifyHeader["Authorization"] = rv.Authorization
	}

	if download {
		rep.Actions["download"] = &lfsLink{Href: rv.objectsLink(), Header: header}
	}

	if upload {
		rep.Actions["upload"] = &lfsLink{Href: rv.objectsLink(), Header: header}
		rep.Actions["verify"] = &lfsLink{Href: rv.verifyLink(), Header: verifyHeader}
	}
	return rep
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

type lfsBatchResponse struct {
	Transfer string               `json:"transfer,omitempty"`
	Objects  []*lfsRepresentation `json:"objects"`
}

// lfsRepresentation is object medata as seen by clients of the lfs server.
type lfsRepresentation struct {
	Oid     string              `json:"oid"`
	Size    int64               `json:"size"`
	Actions map[string]*lfsLink `json:"actions"`
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
	ExpiresAt time.Time         `json:"expires_at"`
}

// metaMatcher provides a mux.MatcherFunc that only allows requests that contain
// an Accept header with the metaMediaType
func metaMatcher(r *http.Request, m *mux.RouteMatch) bool {
	mediaParts := strings.Split(r.Header.Get("Accept"), ";")
	mt := mediaParts[0]
	return mt == metaMediaType
}
