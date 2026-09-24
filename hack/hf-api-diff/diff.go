package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

const (
	kindMissing    = "missing"
	kindExtra      = "extra"
	kindType       = "type"
	kindValue      = "value"
	kindLength     = "length"
	kindStatus     = "status"
	kindError      = "error"
	kindIncomplete = "incomplete"

	resultMatch = "match"
	resultDiff  = "diff"
	resultError = "error"

	bodyNone   = "none"
	bodyDigest = "digest"
	bodyJSON   = "json"
	bodyText   = "text"
)

// Difference is one normalized mismatch between the Hugging Face and hfd captures of a case.
type Difference struct {
	Path, Kind, HF, HFD string
}

type diffList []Difference

func (d *diffList) add(path, kind, hf, hfd string) {
	*d = append(*d, Difference{path, kind, hf, hfd})
}

var (
	ignoredKey     = map[string]bool{}
	comparedHeader = map[string]bool{}
	arrayKeys      = []string{"path", "rfilename", "name", "ref", "id", "oid"}
)

func init() {
	for _, key := range []string{"_id", "downloads", "downloadsAllTime", "likes", "usedStorage", "lastModified", "createdAt",
		"trendingScore", "accessToken", "exp", "casUrl", "avatar", "spaces", "securityFileStatus"} {
		ignoredKey[key] = true
	}
	for _, name := range []string{"Content-Type", "Location", "Link", "X-Repo-Commit", "X-Linked-Etag", "X-Linked-Size", "X-Xet-Hash",
		"X-Total-Count", "X-Error-Code", "X-Error-Message", "WWW-Authenticate", "Accept-Ranges", "Content-Disposition", "Cache-Control"} {
		comparedHeader[http.CanonicalHeaderKey(name)] = true
	}
}

// classify turns a case's differences into the report result; status, transport and incomplete captures are errors.
func classify(diffs []Difference) string {
	result := resultMatch
	for _, diff := range diffs {
		switch diff.Kind {
		case kindStatus, kindError, kindIncomplete:
			return resultError
		}
		result = resultDiff
	}
	return result
}

func compareRecords(hf, hfd Record) []Difference {
	var d diffList
	hf.Response, hf.Final = canonicalHeaders(hf.Response), canonicalHeaders(hf.Final)
	hfd.Response, hfd.Final = canonicalHeaders(hfd.Response), canonicalHeaders(hfd.Final)
	if hf.Response == nil || hfd.Response == nil {
		d.add("response", kindError, errorOrChain(hf), errorOrChain(hfd))
		return d
	}
	if hf.Error != "" || hfd.Error != "" {
		d.add("error", kindError, hf.Error, hfd.Error)
	}
	if a, b := statusChain(hf), statusChain(hfd); a != b {
		d.add("response.status", kindStatus, a, b)
	}
	compareHeaders(&d, "response.headers", hf.Response, hfd.Response, hf.Request.File)
	for i := range min(len(hf.Hops), len(hfd.Hops)) {
		if hf.Hops[i].Location != hfd.Hops[i].Location {
			d.add(fmt.Sprintf("hops[%d].location", i), kindValue, hf.Hops[i].Location, hfd.Hops[i].Location)
		}
	}
	if len(hf.Hops)+len(hfd.Hops) == 0 {
		compareBody(&d, "response", hf.Request, hf.Response, hfd.Response)
	} else {
		compareResponse(&d, "final", hf.Request, hf.Final, hfd.Final)
	}
	return d
}

func canonicalHeaders(r *Response) *Response {
	if r == nil {
		return nil
	}
	out := *r
	out.Headers = map[string]string{}
	for name, value := range r.Headers {
		out.Headers[http.CanonicalHeaderKey(name)] = value
	}
	return &out
}

func errorOrChain(r Record) string {
	if r.Error != "" {
		return r.Error
	}
	return statusChain(r)
}

// statusChain renders the redirect chain of a record, e.g. "302 -> 200".
func statusChain(r Record) string {
	if r.Response == nil {
		return "-"
	}
	var parts []string
	for _, hop := range r.Hops {
		parts = append(parts, strconv.Itoa(hop.Status))
	}
	if len(r.Hops) > 0 && r.Final != nil {
		parts = append(parts, strconv.Itoa(r.Final.Status))
	}
	if len(parts) == 0 {
		parts = []string{strconv.Itoa(r.Response.Status)}
	}
	return strings.Join(parts, " -> ")
}

func statusText(r *Response) string {
	if r == nil {
		return "-"
	}
	return strconv.Itoa(r.Status)
}

func compareResponse(d *diffList, prefix string, req Request, a, b *Response) {
	if a == nil || b == nil {
		d.add(prefix, kindError, statusText(a), statusText(b))
		return
	}
	if a.Status != b.Status {
		d.add(prefix+".status", kindStatus, statusText(a), statusText(b))
	}
	compareHeaders(d, prefix+".headers", a, b, req.File)
	compareBody(d, prefix, req, a, b)
}

// compareHeaders checks the allowlisted headers; ETag and Content-Length only matter for files and non-JSON bodies.
func compareHeaders(d *diffList, prefix string, a, b *Response, file bool) {
	sizes := file || !(jsonContent(a.Headers["Content-Type"]) && jsonContent(b.Headers["Content-Type"]))
	for _, name := range unionKeys(a.Headers, b.Headers) {
		if !comparedHeader[name] && !(sizes && (name == "Etag" || name == "Content-Length")) {
			continue
		}
		av, aok := a.Headers[name]
		bv, bok := b.Headers[name]
		child := prefix + "." + name
		switch {
		case !bok:
			d.add(child, kindMissing, av, "")
		case !aok:
			d.add(child, kindExtra, "", bv)
		case name == "Content-Type" && !sameMediaType(av, bv), name != "Content-Type" && av != bv:
			d.add(child, kindValue, av, bv)
		}
	}
}

func sameMediaType(a, b string) bool {
	am, ap, aerr := mime.ParseMediaType(a)
	bm, bp, berr := mime.ParseMediaType(b)
	if aerr != nil || berr != nil {
		return a == b
	}
	delete(ap, "charset")
	delete(bp, "charset")
	return am == bm && maps.Equal(ap, bp)
}

// bodyForm names what the recorder keeps of a response body: nothing, a payload digest, structured JSON or text.
func bodyForm(req Request, r *Response) string {
	switch {
	case req.Method == http.MethodHead || r.Status == http.StatusNoContent || r.Status == http.StatusNotModified || redirectStatus[r.Status]:
		return bodyNone
	case req.File && r.Status/100 == 2:
		return bodyDigest
	case jsonContent(r.Headers["Content-Type"]):
		return bodyJSON
	}
	return bodyText
}

// incomplete reports a capture that lacks the evidence its form needs, so it can never count as a match.
func incomplete(form string, r *Response) bool {
	switch form {
	case bodyJSON:
		return r.Truncated || r.JSON == nil
	case bodyDigest, bodyText:
		return r.Truncated || r.SHA256 == ""
	}
	return false
}

func compareBody(d *diffList, prefix string, req Request, a, b *Response) {
	path := prefix + ".body"
	af, bf := bodyForm(req, a), bodyForm(req, b)
	switch {
	case incomplete(af, a) || incomplete(bf, b):
		d.add(path, kindIncomplete, bodyState(af, a), bodyState(bf, b))
	case af != bf:
		d.add(path, kindType, bodyState(af, a), bodyState(bf, b))
	case af == bodyJSON:
		compareJSON(d, prefix+".json", decodeJSON(a.JSON), decodeJSON(b.JSON))
	case af != bodyNone && (a.SHA256 != b.SHA256 || a.Size != b.Size):
		d.add(path, kindValue, bodyState(af, a), bodyState(bf, b))
	}
}

func bodyState(form string, r *Response) string {
	switch {
	case r.Truncated:
		return fmt.Sprintf("truncated after %d bytes", r.Size)
	case r.JSON != nil:
		return "json"
	case r.SHA256 == "":
		return "no body"
	case form == bodyJSON:
		return fmt.Sprintf("invalid json (%d bytes)", r.Size)
	case r.Text != "":
		return r.Text
	}
	return fmt.Sprintf("sha256:%s (%d bytes)", r.SHA256[:min(16, len(r.SHA256))], r.Size)
}

func decodeJSON(raw []byte) any {
	var value any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	dec.Decode(&value)
	return value
}

func jsonKind(value any) string {
	switch value.(type) {
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case json.Number:
		return "number"
	case bool:
		return "bool"
	}
	return "null"
}

func render(value any) string {
	out, _ := json.Marshal(value)
	return string(out)
}

func sameNumber(a, b json.Number) bool {
	if a == b {
		return true
	}
	ar, aok := new(big.Rat).SetString(string(a))
	br, bok := new(big.Rat).SetString(string(b))
	return aok && bok && ar.Cmp(br) == 0
}

func unionKeys[V any](a, b map[string]V) []string {
	keys := slices.Collect(maps.Keys(a))
	for key := range b {
		if _, ok := a[key]; !ok {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

// compareJSON walks both decoded values; ignored object keys keep only presence and type.
func compareJSON(d *diffList, path string, a, b any) {
	if jsonKind(a) != jsonKind(b) {
		d.add(path, kindType, render(a), render(b))
		return
	}
	switch av := a.(type) {
	case map[string]any:
		compareMaps(d, av, b.(map[string]any), func(key string) string { return path + "." + key }, true)
	case []any:
		compareArrays(d, path, av, b.([]any))
	case json.Number:
		if !sameNumber(av, b.(json.Number)) {
			d.add(path, kindValue, string(av), string(b.(json.Number)))
		}
	default:
		if a != b {
			d.add(path, kindValue, render(a), render(b))
		}
	}
}

func compareMaps(d *diffList, a, b map[string]any, child func(key string) string, ignore bool) {
	for _, key := range unionKeys(a, b) {
		av, aok := a[key]
		bv, bok := b[key]
		switch {
		case !bok:
			d.add(child(key), kindMissing, render(av), "")
		case !aok:
			d.add(child(key), kindExtra, "", render(bv))
		case !(ignore && ignoredKey[key]) || jsonKind(av) != jsonKind(bv):
			compareJSON(d, child(key), av, bv)
		}
	}
}

// arrayKey returns the first stable key that uniquely identifies every object item on both sides.
func arrayKey(a, b []any) string {
	for _, key := range arrayKeys {
		if keyedBy(key, a) && keyedBy(key, b) {
			return key
		}
	}
	return ""
}

func keyedBy(key string, items []any) bool {
	seen := map[string]bool{}
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			return false
		}
		id, ok := scalarKey(obj[key])
		if !ok || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

func scalarKey(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case json.Number:
		return string(v), true
	}
	return "", false
}

func compareArrays(d *diffList, path string, a, b []any) {
	if len(a)+len(b) == 0 {
		return
	}
	if key := arrayKey(a, b); key != "" {
		index := func(items []any) map[string]any {
			out := map[string]any{}
			for _, item := range items {
				id, _ := scalarKey(item.(map[string]any)[key])
				out[id] = item
			}
			return out
		}
		compareMaps(d, index(a), index(b), func(id string) string { return fmt.Sprintf("%s[%s=%s]", path, key, id) }, false)
		return
	}
	if len(a) != len(b) {
		d.add(path, kindLength, strconv.Itoa(len(a)), strconv.Itoa(len(b)))
	}
	for i := range max(len(a), len(b)) {
		child := fmt.Sprintf("%s[%d]", path, i)
		switch {
		case i >= len(b):
			d.add(child, kindMissing, render(a[i]), "")
		case i >= len(a):
			d.add(child, kindExtra, "", render(b[i]))
		default:
			compareJSON(d, child, a[i], b[i])
		}
	}
}
