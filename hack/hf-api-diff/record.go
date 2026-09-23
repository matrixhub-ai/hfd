package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	redacted = "[redacted]"
	textCap  = 64 << 10
	maxHops  = 10
)

// Response is one captured HTTP response with volatile and secret parts removed.
type Response struct {
	Status    int               `json:"status"`
	Headers   map[string]string `json:"headers"`
	JSON      json.RawMessage   `json:"json,omitempty"`
	Text      string            `json:"text,omitempty"`
	SHA256    string            `json:"sha256,omitempty"`
	Size      int64             `json:"size,omitempty"`
	Truncated bool              `json:"truncated,omitempty"`
}

// Hop is one redirect followed for a File case.
type Hop struct {
	Status   int    `json:"status"`
	Location string `json:"location"`
}

// Record is the persisted outcome of one case; Final is the response after redirects.
type Record struct {
	Request  Request   `json:"request"`
	Response *Response `json:"response,omitempty"`
	Hops     []Hop     `json:"hops,omitempty"`
	Final    *Response `json:"final,omitempty"`
	Error    string    `json:"error,omitempty"`
}

type Meta struct {
	Tool       string  `json:"tool"`
	Base       string  `json:"base"`
	RecordedAt string  `json:"recordedAt"`
	Fixture    Fixture `json:"fixture"`
}

var (
	caseFile       = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*$`)
	linkURL        = regexp.MustCompile(`<[^>]*>`)
	redirectStatus = map[int]bool{301: true, 302: true, 303: true, 307: true, 308: true}
	volatileHeader = map[string]bool{}
)

func init() {
	for _, name := range []string{"Date", "X-Request-Id", "X-Amz-Cf-Id", "X-Amz-Cf-Pop", "Via", "X-Cache", "RateLimit", "RateLimit-Policy",
		"Server-Timing", "Set-Cookie", "Alt-Svc", "Connection", "Age", "CF-Ray", "X-Hub-Cache", "Authorization", "Cookie"} {
		volatileHeader[http.CanonicalHeaderKey(name)] = true
	}
}

type recorder struct {
	base    *url.URL
	client  *http.Client
	timeout time.Duration
	maxBody int64
}

func newRecorder(base *url.URL, timeout time.Duration, maxBody int64) *recorder {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &recorder{base: base, client: client, timeout: timeout, maxBody: maxBody}
}

func recordAll(ctx context.Context, rec *recorder, cases []Request, dir string, fixture Fixture, stdout io.Writer) ([]Record, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	meta := Meta{Tool: "hf-api-diff", Base: rec.base.String(), RecordedAt: time.Now().UTC().Format(time.RFC3339), Fixture: fixture}
	var records []Record
	var failed []string
	for _, c := range cases {
		if !caseFile.MatchString(c.Name) {
			return records, fmt.Errorf("unsafe case name %q", c.Name)
		}
		out := rec.record(ctx, c)
		if err := writeJSON(filepath.Join(dir, c.Name+".json"), out); err != nil {
			return records, err
		}
		records = append(records, out)
		line := fmt.Sprintf("%-30s", c.Name)
		if out.Response != nil {
			line += fmt.Sprintf(" %d", out.Response.Status)
		}
		if len(out.Hops) > 0 && out.Final != nil {
			line += fmt.Sprintf(" -> %d", out.Final.Status)
		}
		if out.Error != "" {
			line += " error: " + out.Error
			failed = append(failed, c.Name+": "+out.Error)
		}
		fmt.Fprintln(stdout, line)
	}
	if err := writeJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
		return records, err
	}
	if len(failed) > 0 {
		return records, fmt.Errorf("%d of %d cases failed:\n  %s", len(failed), len(cases), strings.Join(failed, "\n  "))
	}
	return records, nil
}

func writeJSON(path string, value any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(value); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

func (rec *recorder) do(ctx context.Context, c Request, target string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, c.Method, target, strings.NewReader(c.Body))
	if err != nil {
		return nil, err
	}
	for name, value := range c.Headers {
		req.Header.Set(name, value)
	}
	return rec.client.Do(req)
}

// record performs one case; File cases follow redirects manually with the same method.
func (rec *recorder) record(ctx context.Context, c Request) Record {
	ctx, cancel := context.WithTimeout(ctx, rec.timeout)
	defer cancel()
	out := Record{Request: c}
	resp, err := rec.do(ctx, c, rec.base.String()+c.Path)
	if err != nil {
		out.Error = sanitizeError(err)
		return out
	}
	out.Response, out.Error = rec.capture(resp, c.File)
	if !c.File || out.Error != "" {
		return out
	}
	last := resp
	for redirectStatus[last.StatusCode] {
		if len(out.Hops) == maxHops {
			out.Error = fmt.Sprintf("stopped after %d redirects", maxHops)
			return out
		}
		next, err := last.Location()
		if err != nil {
			out.Error = "redirect: unusable Location header"
			return out
		}
		if next.Scheme != "http" && next.Scheme != "https" {
			out.Error = fmt.Sprintf("redirect to unsupported scheme %q", next.Scheme)
			return out
		}
		if next.User != nil {
			out.Error = "redirect: Location carries credentials"
			return out
		}
		out.Hops = append(out.Hops, Hop{Status: last.StatusCode, Location: normalizeURL(next.String(), rec.base)})
		if last, err = rec.do(ctx, c, next.String()); err != nil {
			out.Error = sanitizeError(err)
			return out
		}
		if redirectStatus[last.StatusCode] {
			last.Body.Close()
		} else {
			out.Final, out.Error = rec.capture(last, c.File)
		}
	}
	if out.Final == nil {
		out.Final = out.Response
	}
	return out
}

// capture reads and closes resp.Body; file payloads are hashed only, JSON is kept structured, other text is capped.
func (rec *recorder) capture(resp *http.Response, file bool) (*Response, string) {
	defer resp.Body.Close()
	out := &Response{Status: resp.StatusCode, Headers: normalizeHeaders(resp.Header, rec.base)}
	if resp.Request.Method == http.MethodHead || redirectStatus[resp.StatusCode] {
		return out, ""
	}
	isJSON := jsonContent(resp.Header.Get("Content-Type"))
	keep := int64(textCap)
	switch {
	case file && resp.StatusCode/100 == 2:
		keep = 0
	case isJSON:
		keep = rec.maxBody
	}
	body, err := readBody(resp.Body, rec.maxBody, keep, out)
	if err != nil {
		return out, "reading body: " + sanitizeError(err)
	}
	switch {
	case keep == 0:
	case isJSON && !out.Truncated && json.Valid(body):
		out.JSON = redactJSON(body)
		if !bytes.Equal(out.JSON, body) {
			out.SHA256 = ""
		}
	case isJSON: // truncated or malformed JSON cannot be redacted, so its text is dropped
	default:
		out.Text = string(body[:min(len(body), textCap)])
	}
	return out, ""
}

// readBody hashes up to maxBody bytes while retaining the first keep bytes.
func readBody(r io.Reader, maxBody, keep int64, out *Response) ([]byte, error) {
	hash := sha256.New()
	var buf bytes.Buffer
	size, err := io.Copy(io.MultiWriter(hash, &cappedWriter{buf: &buf, left: keep}), io.LimitReader(r, maxBody))
	out.SHA256, out.Size = hex.EncodeToString(hash.Sum(nil)), size
	if err != nil {
		out.Truncated = true
		return buf.Bytes(), err
	}
	if size == maxBody {
		extra, err := io.ReadFull(r, make([]byte, 1))
		if err != nil && err != io.EOF {
			out.Truncated = true
			return buf.Bytes(), err
		}
		out.Truncated = extra > 0
	}
	return buf.Bytes(), nil
}

type cappedWriter struct {
	buf  *bytes.Buffer
	left int64
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	keep := min(int64(len(p)), w.left)
	w.buf.Write(p[:keep])
	w.left -= keep
	return len(p), nil
}

func jsonContent(contentType string) bool {
	mediaType, _, _ := mime.ParseMediaType(contentType)
	return mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}

// redactJSON blanks a top-level accessToken value while keeping its presence and JSON type.
func redactJSON(body []byte) json.RawMessage {
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil || object["accessToken"] == nil {
		return body
	}
	var token any
	decoder := json.NewDecoder(bytes.NewReader(object["accessToken"]))
	decoder.UseNumber()
	if decoder.Decode(&token) != nil {
		return nil
	}
	object["accessToken"], _ = json.Marshal(redactValue(token))
	out, err := json.Marshal(object)
	if err != nil {
		return nil
	}
	return out
}

// redactValue replaces strings and numbers in a decoded JSON value, keeping its shape.
func redactValue(value any) any {
	switch v := value.(type) {
	case string:
		return redacted
	case json.Number:
		return 0
	case []any:
		for i := range v {
			v[i] = redactValue(v[i])
		}
	case map[string]any:
		for key := range v {
			v[key] = redactValue(v[key])
		}
	}
	return value
}

func normalizeHeaders(header http.Header, base *url.URL) map[string]string {
	out := map[string]string{}
	for name, values := range header {
		name = http.CanonicalHeaderKey(name)
		if volatileHeader[name] {
			continue
		}
		value := strings.Join(values, ", ")
		switch name {
		case "Location":
			value = normalizeURL(value, base)
		case "Link":
			value = linkURL.ReplaceAllStringFunc(value, func(m string) string { return "<" + normalizeURL(m[1:len(m)-1], base) + ">" })
		case "X-Xet-Access-Token":
			value = redacted
		}
		out[name] = value
	}
	return out
}

// normalizeURL makes base-origin URLs relative and strips every query value from foreign URLs.
func normalizeURL(raw string, base *url.URL) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid-url>"
	}
	u.User, u.Fragment, u.RawFragment = nil, "", ""
	if u.Host != "" && !(strings.EqualFold(u.Scheme, base.Scheme) && strings.EqualFold(u.Host, base.Host)) {
		u.RawQuery = strings.Join(slices.Sorted(maps.Keys(u.Query())), "&")
		return u.String()
	}
	query, sensitive := u.Query(), false
	for key := range query {
		if sensitiveKey(key) {
			query[key], sensitive = []string{""}, true
		}
	}
	if sensitive {
		u.RawQuery = query.Encode()
	}
	u.Scheme, u.Host = "", ""
	return u.String()
}

func sensitiveKey(key string) bool {
	key = strings.ToLower(key)
	return strings.HasPrefix(key, "x-amz-") || strings.HasPrefix(key, "x-goog-") ||
		slices.Contains([]string{"signature", "policy", "key-pair-id", "expires", "token"}, key)
}

// sanitizeError names a request failure without echoing nested net/http text, which may repeat signed URLs.
func sanitizeError(err error) string {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err.Error()
	}
	var netErr *net.OpError
	reason := "request failed"
	switch {
	case urlErr.Timeout() || errors.Is(err, context.DeadlineExceeded):
		reason = "timeout"
	case errors.Is(err, context.Canceled):
		reason = "canceled"
	case errors.As(err, &netErr):
		reason = netErr.Error()
	}
	return fmt.Sprintf("%s %q: %s", urlErr.Op, normalizeURL(urlErr.URL, &url.URL{}), reason)
}
