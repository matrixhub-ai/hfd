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
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var safeName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*$`)

func TestDefaultListFixture(t *testing.T) {
	requests, err := buildCases(defaultFixture)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range requests {
		if request.Name == "models.list" {
			parsed, err := url.Parse(request.Path)
			if err != nil || parsed.Query().Get("author") != "wzshiming" || parsed.Query().Get("search") != "gpt2" {
				t.Fatalf("default model list request = %q, %v", request.Path, err)
			}
			return
		}
	}
	t.Fatal("missing model list request")
}

func TestRunRejectsBadInput(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"bogus"},
		{"record", "extra"},
		{"record", "-unknown"},
		{"record", "-base", "ftp://example.com"},
		{"record", "-base", "http://user:pw@example.com"},
		{"record", "-base", "http://example.com/prefix"},
		{"record", "-base", "http://example.com?x=1"},
		{"record", "-base", "example.com"},
		{"record", "-timeout", "0"},
		{"record", "-max-body", "0"},
		{"record", "-max-body", "1MB"},
		{"record", "-model", "noslash"},
	} {
		if err := run(args, io.Discard); err == nil {
			t.Errorf("run(%q) accepted", args)
		}
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"64MiB": 64 << 20, "1KiB": 1024, "2GiB": 2 << 30, "123": 123, "8589934591GiB": 8589934591 << 30} {
		got, err := parseSize(in)
		if err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "-1", "0", "1.5MiB", "1MB", "MiB", "8589934592GiB", "17179869184GiB", "17179869185GiB"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("parseSize(%q) accepted", bad)
		}
	}
}

func TestParseOrigin(t *testing.T) {
	origin, err := parseOrigin("https://huggingface.co/")
	if err != nil || origin.String() != "https://huggingface.co" {
		t.Fatalf("parseOrigin = %v, %v", origin, err)
	}
}

func mustOrigin(t *testing.T, raw string) *url.URL {
	t.Helper()
	origin, err := parseOrigin(raw)
	if err != nil {
		t.Fatal(err)
	}
	return origin
}

func TestNormalizeURL(t *testing.T) {
	base := mustOrigin(t, "https://huggingface.co")
	for in, want := range map[string]string{
		"https://cdn-lfs.hf.co/repos/ab/cd?Signature=xyz&Expires=1&Key-Pair-Id=k": "https://cdn-lfs.hf.co/repos/ab/cd?Expires&Key-Pair-Id&Signature",
		"https://huggingface.co/api/models?author=a&cursor=abc":                   "/api/models?author=a&cursor=abc",
		"https://HUGGINGFACE.co/x?token=secret&limit=2":                           "/x?limit=2&token=",
		"/relative/path?limit=1":                                                  "/relative/path?limit=1",
		"https://huggingface.co.evil.com/x?Signature=1":                           "https://huggingface.co.evil.com/x?Signature",
		"//cdn.example/x?Signature=1":                                             "//cdn.example/x?Signature",
		"http://huggingface.co/x?Signature=1":                                     "http://huggingface.co/x?Signature",
		"https://user:pw@cdn.example/x#frag":                                      "https://cdn.example/x",
	} {
		if got := normalizeURL(in, base); got != want {
			t.Errorf("normalizeURL(%q) = %q, want %q", in, got, want)
		}
	}
	headers := http.Header{
		"Link":               {`<https://huggingface.co/api/models?cursor=a>; rel="next", <https://cdn.example/x?Signature=s>; rel="other"`},
		"Location":           {"https://cdn.example/y?Policy=p"},
		"Date":               {"Mon, 01 Jan 2024 00:00:00 GMT"},
		"RateLimit":          {"limit=10"},
		"CF-Ray":             {"abc"},
		"X-Hub-Cache":        {"HIT"},
		"Set-Cookie":         {"a=b"},
		"Authorization":      {"Bearer x"},
		"X-Xet-Access-Token": {"tok"},
		"Content-Type":       {"application/json"},
	}
	got := normalizeHeaders(headers, base)
	want := map[string]string{
		"Link":               `</api/models?cursor=a>; rel="next", <https://cdn.example/x?Signature>; rel="other"`,
		"Location":           "https://cdn.example/y?Policy",
		"X-Xet-Access-Token": redacted,
		"Content-Type":       "application/json",
	}
	if len(got) != len(want) {
		t.Errorf("normalizeHeaders = %v", got)
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("header %s = %q, want %q", name, got[name], value)
		}
	}
}

func TestSanitizeError(t *testing.T) {
	signed := "https://cdn.example/x?Signature=abc&Expires=1"
	nested := fmt.Errorf("failed to parse Location header %q: %w", signed, &url.Error{Op: "parse", URL: signed, Err: errors.New("invalid URL escape")})
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&url.Error{Op: "Get", URL: signed, Err: errors.New("boom")}, `Get "https://cdn.example/x?Expires&Signature": request failed`},
		{&url.Error{Op: "Get", URL: "http://hub.example/", Err: nested}, `Get "http://hub.example/": request failed`},
		{&url.Error{Op: "Get", URL: signed, Err: context.DeadlineExceeded}, `Get "https://cdn.example/x?Expires&Signature": timeout`},
		{&url.Error{Op: "Get", URL: signed, Err: context.Canceled}, `Get "https://cdn.example/x?Expires&Signature": canceled`},
		{&url.Error{Op: "Get", URL: signed, Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}}, `Get "https://cdn.example/x?Expires&Signature": dial tcp: connection refused`},
		{io.ErrUnexpectedEOF, "unexpected EOF"},
	} {
		if got := sanitizeError(tc.err); got != tc.want {
			t.Errorf("sanitizeError(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

func TestRedactJSON(t *testing.T) {
	for in, want := range map[string]string{
		`{"accessToken":"secret-token-abc","exp":1}`:             `{"accessToken":"[redacted]","exp":1}`,
		`{"accessToken":null}`:                                   `{"accessToken":null}`,
		`{"accessToken":12345678901234567890123}`:                `{"accessToken":0}`,
		`{"accessToken":1e400}`:                                  `{"accessToken":0}`,
		`{"accessToken":{"token":"secret-token-abc","n":1e400}}`: `{"accessToken":{"n":0,"token":"[redacted]"}}`,
		`{"accessToken":false}`:                                  `{"accessToken":false}`,
		`{"accessToken":[]}`:                                     `{"accessToken":[]}`,
		`{"accessToken":{}}`:                                     `{"accessToken":{}}`,
		`{"casUrl":"https://cas.example"}`:                       `{"casUrl":"https://cas.example"}`,
		`{"accessToken":["secret-token-abc",{"token":"secret-token-abc","n":7,"ok":true}]}`: `{"accessToken":["[redacted]",{"n":0,"ok":true,"token":"[redacted]"}]}`,
	} {
		if got := string(redactJSON([]byte(in))); got != want {
			t.Errorf("redactJSON(%s) = %s, want %s", in, got, want)
		}
	}
}

type seenRequest struct {
	Method, Path, RawQuery, Body string
	Header                       http.Header
}

type fixtureServers struct {
	hub, cdn *httptest.Server
	payload  []byte

	mu   sync.Mutex
	seen []seenRequest
}

func (f *fixtureServers) observe(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, seenRequest{r.Method, r.URL.Path, r.URL.RawQuery, string(body), r.Header.Clone()})
}

func (f *fixtureServers) requests(method, path string) []seenRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []seenRequest
	for _, s := range f.seen {
		if s.Method == method && s.Path == path {
			out = append(out, s)
		}
	}
	return out
}

var counter atomic.Int64

func volatile(w http.ResponseWriter) {
	n := counter.Add(1)
	h := w.Header()
	h.Set("X-Request-Id", "req-"+strconv.FormatInt(n, 10))
	h.Set("Set-Cookie", "session=secret-cookie")
	h.Set("RateLimit", "limit=100, remaining="+strconv.FormatInt(1000-n, 10))
	h.Set("Server-Timing", "app;dur=12")
	h.Set("Via", "1.1 varnish")
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Repo-Commit", "abc123")
}

func newFixtureServers(t *testing.T) *fixtureServers {
	f := &fixtureServers{payload: make([]byte, 200000)}
	for i := range f.payload {
		f.payload[i] = byte(i*7 + i>>8)
	}
	f.cdn = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.observe(r)
		if r.URL.Query().Get("Signature") != "sig-secret" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		h := w.Header()
		h.Set("X-Amz-Cf-Id", "cf-"+strconv.FormatInt(counter.Add(1), 10))
		h.Set("X-Amz-Cf-Pop", "SFO5")
		h.Set("X-Cache", "Hit from cloudfront")
		h.Set("Age", "12")
		h.Set("Content-Type", "application/octet-stream")
		h.Set("Content-Length", strconv.Itoa(len(f.payload)))
		h.Set("Etag", `"oid123"`)
		if r.Method != http.MethodHead {
			w.Write(f.payload)
		}
	}))
	t.Cleanup(f.cdn.Close)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/models/ns/name", func(w http.ResponseWriter, r *http.Request) {
		f.observe(r)
		volatile(w)
		io.WriteString(w, `{"id":"ns/name","likes":12345678901234567890123,"private":false}`)
	})
	mux.HandleFunc("/api/models/ns/name/xet-read-token/main", func(w http.ResponseWriter, r *http.Request) {
		f.observe(r)
		volatile(w)
		w.Header().Set("X-Xet-Access-Token", "secret-token-abc")
		io.WriteString(w, `{"accessToken":"secret-token-abc","casUrl":"https://cas.example","exp":1700000000}`)
	})
	mux.HandleFunc("/api/models", func(w http.ResponseWriter, r *http.Request) {
		f.observe(r)
		volatile(w)
		w.Header().Set("Link", "<"+f.hub.URL+`/api/models?author=ns&cursor=abc>; rel="next"`)
		io.WriteString(w, `[{"id":"ns/name"}]`)
	})
	mux.HandleFunc("/api/models/ns/name/paths-info/main", func(w http.ResponseWriter, r *http.Request) {
		f.observe(r)
		volatile(w)
		io.WriteString(w, `[{"path":"config.json","type":"file","size":7}]`)
	})
	mux.HandleFunc("/ns/name/resolve/main/config.json", func(w http.ResponseWriter, r *http.Request) {
		f.observe(r)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Etag", `"cfg"`)
		io.WriteString(w, `{"a":1}`)
	})
	mux.HandleFunc("/ns/name/resolve/main/model.bin", func(w http.ResponseWriter, r *http.Request) {
		f.observe(r)
		w.Header().Set("X-Linked-Size", strconv.Itoa(len(f.payload)))
		http.Redirect(w, r, f.cdn.URL+"/blob/oid123?Signature=sig-secret&Expires=999&Key-Pair-Id=kp", http.StatusFound)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		f.observe(r)
		volatile(w)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"error":"not found"}`)
	})
	f.hub = httptest.NewServer(mux)
	t.Cleanup(f.hub.Close)
	return f
}

func readDir(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]byte{}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = data
	}
	return out
}

func loadRecord(t *testing.T, data []byte) Record {
	t.Helper()
	var rec Record
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

func sha(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestRecordRoundTrip(t *testing.T) {
	f := newFixtureServers(t)
	dir := t.TempDir()
	args := []string{"record", "-base", f.hub.URL, "-out", filepath.Join(dir, "one"), "-model", "ns/name",
		"-dataset", "dns/dname", "-space", "sns/sname", "-lfs-file", "model.bin", "-timeout", "10s"}
	var out bytes.Buffer
	if err := run(args, &out); err != nil {
		t.Fatalf("record: %v\n%s", err, out.String())
	}
	first := readDir(t, filepath.Join(dir, "one"))
	if len(first) != 35 {
		t.Fatalf("got %d files, want 34 cases + meta", len(first))
	}

	pathsInfo := f.requests(http.MethodPost, "/api/models/ns/name/paths-info/main")
	if len(pathsInfo) != 1 || pathsInfo[0].Body != `{"paths":["config.json","model.bin","does-not-exist.txt"],"expand":true}` ||
		pathsInfo[0].Header.Get("Content-Type") != "application/json" {
		t.Errorf("paths-info requests %+v", pathsInfo)
	}
	info := f.requests(http.MethodGet, "/api/models/ns/name")
	if len(info) != 1 || info[0].Header.Get("Accept") != "application/json" || info[0].Header.Get("Authorization") != "" || info[0].Header.Get("Cookie") != "" ||
		info[0].Header.Get("Accept-Encoding") != "" {
		t.Errorf("info requests %+v", info)
	}
	if list := f.requests(http.MethodGet, "/api/models"); len(list) != 1 || list[0].RawQuery != "author=ns&limit=5&search=name" {
		t.Errorf("list requests %+v", list)
	}
	if got := f.requests(http.MethodHead, "/blob/oid123"); len(got) != 1 || got[0].RawQuery != "Signature=sig-secret&Expires=999&Key-Pair-Id=kp" {
		t.Errorf("cdn HEAD requests %+v", got)
	}
	if got := f.requests(http.MethodGet, "/blob/oid123"); len(got) != 1 {
		t.Errorf("cdn GET requests %+v", got)
	}

	lfs := loadRecord(t, first["models.resolve.lfs.json"])
	wantLocation := f.cdn.URL + "/blob/oid123?Expires&Key-Pair-Id&Signature"
	if lfs.Response.Status != http.StatusFound || lfs.Response.Headers["Location"] != wantLocation || lfs.Response.Text != "" || lfs.Response.SHA256 != "" {
		t.Errorf("lfs initial response %+v", lfs.Response)
	}
	if len(lfs.Hops) != 1 || lfs.Hops[0] != (Hop{Status: http.StatusFound, Location: wantLocation}) {
		t.Errorf("lfs hops %+v", lfs.Hops)
	}
	if lfs.Final == nil || lfs.Final.Status != http.StatusOK || lfs.Final.SHA256 != sha(f.payload) || lfs.Final.Size != int64(len(f.payload)) ||
		lfs.Final.Truncated || lfs.Final.Text != "" || lfs.Final.JSON != nil || lfs.Error != "" {
		t.Errorf("lfs final %+v error %q", lfs.Final, lfs.Error)
	}
	for _, name := range []string{"X-Amz-Cf-Id", "X-Amz-Cf-Pop", "X-Cache", "Age", "Date"} {
		if _, ok := lfs.Final.Headers[name]; ok {
			t.Errorf("volatile header %s kept", name)
		}
	}
	if lfs.Final.Headers["Etag"] != `"oid123"` || lfs.Final.Headers["Content-Length"] != "200000" {
		t.Errorf("lfs final headers %v", lfs.Final.Headers)
	}

	lfsHead := loadRecord(t, first["models.resolve.lfs.head.json"])
	if lfsHead.Final == nil || lfsHead.Final.Status != http.StatusOK || lfsHead.Final.SHA256 != "" || lfsHead.Final.Size != 0 ||
		lfsHead.Final.Headers["Content-Length"] != "200000" || len(lfsHead.Hops) != 1 {
		t.Errorf("lfs head %+v", lfsHead)
	}

	cfg := loadRecord(t, first["models.resolve.config.json"])
	if cfg.Response.Status != http.StatusOK || cfg.Hops != nil || cfg.Final == nil || cfg.Final.SHA256 != sha([]byte(`{"a":1}`)) ||
		cfg.Final.Size != 7 || cfg.Final.Text != "" || cfg.Final.JSON != nil {
		t.Errorf("config %+v final %+v", cfg, cfg.Final)
	}

	missing := loadRecord(t, first["models.resolve.notfound.json"])
	if missing.Response.Status != http.StatusNotFound || missing.Final == nil || string(compact(t, missing.Final.JSON)) != `{"error":"not found"}` {
		t.Errorf("resolve notfound %+v final %+v", missing, missing.Final)
	}

	infoRec := loadRecord(t, first["models.info.json"])
	if infoRec.Response.Status != http.StatusOK || !bytes.Contains(infoRec.Response.JSON, []byte("12345678901234567890123")) ||
		infoRec.Response.Text != "" || infoRec.Response.SHA256 == "" || infoRec.Final != nil {
		t.Errorf("info %+v", infoRec.Response)
	}
	for _, name := range []string{"Date", "X-Request-Id", "Set-Cookie", "Ratelimit", "Server-Timing", "Via"} {
		if _, ok := infoRec.Response.Headers[name]; ok {
			t.Errorf("volatile header %s kept", name)
		}
	}
	if infoRec.Response.Headers["Content-Type"] != "application/json; charset=utf-8" || infoRec.Response.Headers["X-Repo-Commit"] != "abc123" {
		t.Errorf("info headers %v", infoRec.Response.Headers)
	}

	token := loadRecord(t, first["models.xet-read-token.json"])
	var tokenBody map[string]json.RawMessage
	if err := json.Unmarshal(token.Response.JSON, &tokenBody); err != nil {
		t.Fatal(err)
	}
	if string(tokenBody["accessToken"]) != strconv.Quote(redacted) || string(tokenBody["casUrl"]) != `"https://cas.example"` ||
		string(tokenBody["exp"]) != "1700000000" || token.Response.Headers["X-Xet-Access-Token"] != redacted {
		t.Errorf("token record %s headers %v", token.Response.JSON, token.Response.Headers)
	}
	if token.Response.SHA256 != "" || token.Response.Size == 0 {
		t.Errorf("redacted token body keeps raw hash %q size %d", token.Response.SHA256, token.Response.Size)
	}

	list := loadRecord(t, first["models.list.json"])
	if list.Response.Headers["Link"] != `</api/models?author=ns&cursor=abc>; rel="next"` {
		t.Errorf("list Link %q", list.Response.Headers["Link"])
	}

	for name, data := range first {
		for _, secret := range []string{"secret-token-abc", "sig-secret", "secret-cookie", "Expires=999", "req-"} {
			if bytes.Contains(data, []byte(secret)) {
				t.Errorf("%s contains %q", name, secret)
			}
		}
		if !bytes.HasSuffix(data, []byte("\n")) || bytes.Contains(data, []byte(`\u003c`)) {
			t.Errorf("%s formatting", name)
		}
	}

	var meta Meta
	if err := json.Unmarshal(first["meta.json"], &meta); err != nil {
		t.Fatal(err)
	}
	if _, err := time.Parse(time.RFC3339, meta.RecordedAt); err != nil || meta.Tool != "hf-api-diff" || meta.Base != f.hub.URL || meta.Fixture.Model != "ns/name" || meta.Fixture.LFSFile != "model.bin" {
		t.Errorf("meta %+v", meta)
	}
	for _, c := range mustCases(t, meta.Fixture) {
		var rec Record
		if err := json.Unmarshal(first[c.Name+".json"], &rec); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rec.Request, c) {
			t.Errorf("%s request %+v, want %+v", c.Name, rec.Request, c)
		}
	}

	args[4] = filepath.Join(dir, "two")
	if err := run(args, io.Discard); err != nil {
		t.Fatal(err)
	}
	second := readDir(t, filepath.Join(dir, "two"))
	for name, data := range first {
		if name != "meta.json" && !bytes.Equal(data, second[name]) {
			t.Errorf("%s differs between recordings:\n%s\n---\n%s", name, data, second[name])
		}
	}
}

func mustCases(t *testing.T, f Fixture) []Request {
	t.Helper()
	cases, err := buildCases(f)
	if err != nil {
		t.Fatal(err)
	}
	return cases
}

func compact(t *testing.T, raw json.RawMessage) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		t.Fatalf("compact %q: %v", raw, err)
	}
	return buf.Bytes()
}

func TestRecordBodyLimits(t *testing.T) {
	bigJSON := "[" + strings.Repeat("1234567890,", 10000) + "1]"
	text := strings.Repeat("x", 100<<10)
	mux := http.NewServeMux()
	mux.HandleFunc("/json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, bigJSON)
	})
	mux.HandleFunc("/text", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, text)
	})
	mux.HandleFunc("/short", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		io.WriteString(w, "partial")
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"accessToken":"secret-token-abc","padding":"`+strings.Repeat("x", 100)+`"}`)
	})
	mux.HandleFunc("/malformed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"accessToken":"secret-token-abc"`)
	})
	mux.HandleFunc("/shortjson", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1000")
		io.WriteString(w, `{"accessToken":"secret-token-abc"`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	get := func(path string) Request {
		return Request{Name: "x", Method: http.MethodGet, Path: path, Headers: map[string]string{"Accept": "application/json"}}
	}
	rec := newRecorder(mustOrigin(t, srv.URL), 5*time.Second, 1<<20)

	big := rec.record(context.Background(), get("/json"))
	if big.Error != "" || big.Response.JSON == nil || big.Response.Text != "" || big.Response.Truncated ||
		big.Response.Size != int64(len(bigJSON)) || big.Response.SHA256 != sha([]byte(bigJSON)) {
		t.Errorf("big json %+v error %q", big.Response, big.Error)
	}
	long := rec.record(context.Background(), get("/text"))
	if long.Error != "" || len(long.Response.Text) != 64<<10 || long.Response.Size != int64(len(text)) || long.Response.SHA256 != sha([]byte(text)) || long.Response.Truncated {
		t.Errorf("long text size %d text %d truncated %v error %q", long.Response.Size, len(long.Response.Text), long.Response.Truncated, long.Error)
	}
	capped := newRecorder(mustOrigin(t, srv.URL), 5*time.Second, 1024).record(context.Background(), get("/json"))
	if capped.Error != "" || !capped.Response.Truncated || capped.Response.Size != 1024 || capped.Response.JSON != nil ||
		capped.Response.Text != "" || capped.Response.SHA256 != sha([]byte(bigJSON[:1024])) {
		t.Errorf("capped %+v error %q", capped.Response, capped.Error)
	}
	short := rec.record(context.Background(), get("/short"))
	if short.Error == "" || short.Response == nil || !short.Response.Truncated || short.Response.Size != 7 || short.Response.SHA256 != sha([]byte("partial")) {
		t.Errorf("short %+v error %q", short.Response, short.Error)
	}
	for _, tc := range []struct {
		path      string
		truncated bool
		failed    bool
	}{{"/token", true, false}, {"/malformed", false, false}, {"/shortjson", true, true}} {
		got := newRecorder(mustOrigin(t, srv.URL), 5*time.Second, 64).record(context.Background(), get(tc.path))
		data, _ := json.Marshal(got)
		if got.Response == nil || got.Response.Text != "" || got.Response.JSON != nil || got.Response.SHA256 == "" ||
			got.Response.Truncated != tc.truncated || (got.Error != "") != tc.failed || bytes.Contains(data, []byte("secret-token-abc")) {
			t.Errorf("%s: %s", tc.path, data)
		}
	}
}

func TestRecordIncompleteBodyAtLimit(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/cut", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		io.WriteString(w, "1234")
	})
	mux.HandleFunc("/exact", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "1234")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	dir := t.TempDir()
	cases := []Request{{Name: "cut", Method: http.MethodGet, Path: "/cut", File: true}, {Name: "exact", Method: http.MethodGet, Path: "/exact", File: true}}
	_, err := recordAll(context.Background(), newRecorder(mustOrigin(t, srv.URL), 5*time.Second, 4), cases, dir, defaultFixture, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "1 of 2") {
		t.Fatalf("recordAll error %v", err)
	}
	files := readDir(t, dir)
	cut := loadRecord(t, files["cut.json"])
	if !strings.Contains(cut.Error, "reading body") || cut.Response == nil || !cut.Response.Truncated || cut.Response.Size != 4 || cut.Final != nil {
		t.Errorf("cut %+v response %+v", cut, cut.Response)
	}
	exact := loadRecord(t, files["exact.json"])
	if exact.Error != "" || exact.Response.Truncated || exact.Response.Size != 4 || exact.Response.SHA256 != sha([]byte("1234")) {
		t.Errorf("exact %+v response %+v", exact, exact.Response)
	}
}

func TestRecordRedirectRules(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/loop", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/loop?Signature=loop-secret", http.StatusFound)
	})
	mux.HandleFunc("/bad", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "ftp://example.com/x")
		w.WriteHeader(http.StatusFound)
	})
	mux.HandleFunc("/noloc", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMovedPermanently)
	})
	mux.HandleFunc("/hop1", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "hop2", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("/hop2", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, r.Method+" final")
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		http.Redirect(w, r, "/slow", http.StatusFound)
	})
	mux.HandleFunc("/badloc", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/%zz?Signature=loop-secret")
		w.WriteHeader(http.StatusFound)
	})
	var targetHits atomic.Int32
	mux.HandleFunc("/target", func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		io.WriteString(w, "ok")
	})
	mux.HandleFunc("/userinfo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://loop-user:loop-secret@"+r.Host+"/target")
		w.WriteHeader(http.StatusFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	rec := newRecorder(mustOrigin(t, srv.URL), 5*time.Second, 1<<20)
	file := func(method, path string) Request {
		return Request{Name: "f", Method: method, Path: path, File: true}
	}

	loop := rec.record(context.Background(), file(http.MethodGet, "/loop"))
	if len(loop.Hops) != 10 || !strings.Contains(loop.Error, "redirect") || loop.Final != nil {
		t.Errorf("loop hops %d final %v error %q", len(loop.Hops), loop.Final, loop.Error)
	}
	if data, _ := json.Marshal(loop); bytes.Contains(data, []byte("loop-secret")) || loop.Hops[0].Location != "/loop?Signature=" {
		t.Errorf("loop leaks: %s", data)
	}
	for _, path := range []string{"/bad", "/noloc"} {
		if got := rec.record(context.Background(), file(http.MethodGet, path)); got.Error == "" || got.Final != nil || got.Response.Status/100 != 3 {
			t.Errorf("%s: %+v", path, got)
		}
	}
	badLoc := rec.record(context.Background(), file(http.MethodGet, "/badloc"))
	if data, _ := json.Marshal(badLoc); badLoc.Error == "" || badLoc.Final != nil || bytes.Contains(data, []byte("loop-secret")) {
		t.Errorf("bad location leaks: %s", data)
	}
	userinfo := rec.record(context.Background(), file(http.MethodGet, "/userinfo"))
	if data, _ := json.Marshal(userinfo); userinfo.Error == "" || userinfo.Final != nil || targetHits.Load() != 0 || bytes.Contains(data, []byte("loop-secret")) {
		t.Errorf("userinfo redirect: target hits %d record %s", targetHits.Load(), data)
	}
	two := rec.record(context.Background(), file(http.MethodHead, "/hop1"))
	if two.Error != "" || len(two.Hops) != 1 || two.Hops[0].Location != "/hop2" || two.Final == nil || two.Final.Status != http.StatusOK || two.Final.SHA256 != "" {
		t.Errorf("two hops %+v final %+v", two, two.Final)
	}
	twoGet := rec.record(context.Background(), file(http.MethodGet, "/hop1"))
	if twoGet.Final == nil || twoGet.Final.SHA256 != sha([]byte("GET final")) || twoGet.Final.Text != "" {
		t.Errorf("two get final %+v", twoGet.Final)
	}
	api := rec.record(context.Background(), Request{Name: "a", Method: http.MethodGet, Path: "/hop1"})
	if api.Error != "" || api.Response.Status != http.StatusTemporaryRedirect || api.Hops != nil || api.Final != nil || api.Response.Headers["Location"] != "/hop2" {
		t.Errorf("api redirect %+v", api)
	}

	start := time.Now()
	slow := newRecorder(mustOrigin(t, srv.URL), 500*time.Millisecond, 1<<20).record(context.Background(), file(http.MethodGet, "/slow"))
	if elapsed := time.Since(start); elapsed > 3*time.Second || !strings.Contains(slow.Error, "timeout") || len(slow.Hops) >= 10 {
		t.Errorf("slow: elapsed %v hops %d error %q", elapsed, len(slow.Hops), slow.Error)
	}
}

func TestRecordTransportError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	addr := srv.URL
	srv.Close()
	dir := t.TempDir()
	var out bytes.Buffer
	err := run([]string{"record", "-base", addr, "-out", dir, "-timeout", "5s"}, &out)
	if err == nil || !strings.Contains(err.Error(), "34 of 34") {
		t.Fatalf("run error %v", err)
	}
	files := readDir(t, dir)
	rec := loadRecord(t, files["models.info.json"])
	if rec.Error == "" || rec.Response != nil || rec.Request.Name != "models.info" {
		t.Errorf("record %+v", rec)
	}
	if _, ok := files["meta.json"]; !ok || !strings.Contains(out.String(), "models.info") {
		t.Errorf("files %d output %q", len(files), out.String())
	}
}

func TestBuildCasesInventory(t *testing.T) {
	cases, err := buildCases(defaultFixture)
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 34 {
		t.Fatalf("got %d cases, want 34", len(cases))
	}
	index := map[string]int{}
	for i, c := range cases {
		if _, dup := index[c.Name]; dup {
			t.Errorf("duplicate case %q", c.Name)
		}
		index[c.Name] = i
		if !safeName.MatchString(c.Name) {
			t.Errorf("unsafe case name %q", c.Name)
		}
		if i > 0 && cases[i-1].Name >= c.Name {
			t.Errorf("cases not sorted at %q", c.Name)
		}
		if !strings.HasPrefix(c.Path, "/") {
			t.Errorf("%s: path %q is not root-relative", c.Name, c.Path)
		}
		switch c.Method {
		case http.MethodGet, http.MethodHead:
			if c.Body != "" {
				t.Errorf("%s: unexpected body", c.Name)
			}
		case http.MethodPost:
			if !strings.Contains(c.Path, "/paths-info/") || c.Headers["Content-Type"] != "application/json" {
				t.Errorf("%s: unexpected POST %q %v", c.Name, c.Path, c.Headers)
			}
		default:
			t.Errorf("%s: unsafe method %s", c.Name, c.Method)
		}
		if c.File != strings.Contains(c.Path, "/resolve/") {
			t.Errorf("%s: file flag %v does not match path %q", c.Name, c.File, c.Path)
		}
		if !c.File && c.Headers["Accept"] != "application/json" {
			t.Errorf("%s: missing Accept header", c.Name)
		}
	}
	want := map[string]string{
		"agent-harnesses":              "/api/agent-harnesses",
		"whoami-v2":                    "/api/whoami-v2",
		"models.info":                  "/api/models/wzshiming/gpt2",
		"models.list":                  "/api/models?author=wzshiming&limit=5&search=gpt2",
		"models.notfound":              "/api/models/wzshiming/does-not-exist",
		"models.tree.recursive":        "/api/models/wzshiming/gpt2/tree/main?expand=true&recursive=true",
		"models.commits.limit":         "/api/models/wzshiming/gpt2/commits/main?limit=2",
		"models.xet-read-token":        "/api/models/wzshiming/gpt2/xet-read-token/main",
		"models.resolve.lfs":           "/wzshiming/gpt2/resolve/main/64-8bits.tflite",
		"models.resolve.lfs.head":      "/wzshiming/gpt2/resolve/main/64-8bits.tflite",
		"models.resolve.notfound":      "/wzshiming/gpt2/resolve/main/does-not-exist.txt",
		"datasets.resolve.readme.head": "/datasets/wzshiming/fixtures_image_utils/resolve/main/README.md",
		"datasets.paths-info":          "/api/datasets/wzshiming/fixtures_image_utils/paths-info/main",
		"spaces.commits":               "/api/spaces/wzshiming/hello_world/commits/main",
		"spaces.resolve.readme.head":   "/spaces/wzshiming/hello_world/resolve/main/README.md",
	}
	for name, path := range want {
		i, ok := index[name]
		if !ok {
			t.Errorf("missing case %q", name)
			continue
		}
		if cases[i].Path != path {
			t.Errorf("%s: path %q, want %q", name, cases[i].Path, path)
		}
	}
	if index["models.info"] > index["models.list"] {
		t.Error("models.info must precede models.list")
	}
	if got := cases[index["models.paths-info"]].Body; got != `{"paths":["config.json","64-8bits.tflite","does-not-exist.txt"],"expand":true}` {
		t.Errorf("models.paths-info body %q", got)
	}
	if got := cases[index["models.resolve.lfs.head"]].Method; got != http.MethodHead {
		t.Errorf("models.resolve.lfs.head method %s", got)
	}
}

func TestBuildCasesEscapesFixture(t *testing.T) {
	cases, err := buildCases(Fixture{Model: "ns/na me#1", Dataset: "d/x", Space: "s/y", LFSFile: "dir/we ird.bin"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, c := range cases {
		got[c.Name] = c.Path
	}
	if got["models.info"] != "/api/models/ns/na%20me%231" {
		t.Errorf("models.info path %q", got["models.info"])
	}
	if got["models.resolve.lfs"] != "/ns/na%20me%231/resolve/main/dir/we%20ird.bin" {
		t.Errorf("models.resolve.lfs path %q", got["models.resolve.lfs"])
	}
	for _, bad := range []Fixture{
		{Model: "noslash", Dataset: "d/x", Space: "s/y", LFSFile: "f"},
		{Model: "a/b/c", Dataset: "d/x", Space: "s/y", LFSFile: "f"},
		{Model: "a/b", Dataset: "/x", Space: "s/y", LFSFile: "f"},
		{Model: "a/b", Dataset: "d/x", Space: "s/y", LFSFile: ""},
	} {
		if _, err := buildCases(bad); err == nil {
			t.Errorf("fixture %+v accepted", bad)
		}
	}
}

func writeRecordings(t *testing.T, dir string, meta bool, records ...Record) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if meta {
		if err := writeJSON(filepath.Join(dir, "meta.json"), Meta{Tool: "hf-api-diff", Base: "https://hub.example", RecordedAt: "2026-01-01T00:00:00Z", Fixture: defaultFixture}); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range records {
		if err := writeJSON(filepath.Join(dir, r.Request.Name+".json"), r); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestCompareRejectsBadInput(t *testing.T) {
	trap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s %s", r.Method, r.URL)
	}))
	defer trap.Close()
	base := t.TempDir()
	good := Request{Name: "models.info", Method: http.MethodGet, Path: "/api/models/ns/name", Headers: map[string]string{"Accept": "application/json"}}
	valid := writeRecordings(t, filepath.Join(base, "valid"), true, Record{Request: good, Response: &Response{Status: 200}})
	noMeta := writeRecordings(t, filepath.Join(base, "nometa"), false, Record{Request: good})
	empty := writeRecordings(t, filepath.Join(base, "empty"), true)
	invalid := writeRecordings(t, filepath.Join(base, "invalid"), true)
	os.WriteFile(filepath.Join(invalid, "x.json"), []byte("{"), 0o644)
	renamed := writeRecordings(t, filepath.Join(base, "renamed"), true)
	os.WriteFile(filepath.Join(renamed, "other.json"), []byte(`{"request":{"name":"models.info","method":"GET","path":"/x"}}`), 0o644)
	linked, hard, outLink, out := filepath.Join(base, "linked.md"), filepath.Join(base, "hard.md"), filepath.Join(base, "outlink"), filepath.Join(base, "out")
	if err := errors.Join(os.Symlink(filepath.Join(valid, "meta.json"), linked), os.Link(filepath.Join(valid, "models.info.json"), hard), os.Symlink(valid, outLink)); err != nil {
		t.Fatal(err)
	}
	existing := writeRecordings(t, filepath.Join(base, "existing"), false, Record{Request: good})
	captureLink, captureHard, dangling, existingLink := filepath.Join(base, "capture-link.md"), filepath.Join(base, "capture-hard.md"), filepath.Join(base, "dangling.md"), filepath.Join(base, "existinglink")
	if err := errors.Join(os.Symlink(filepath.Join(existing, "models.info.json"), captureLink), os.Link(filepath.Join(existing, "models.info.json"), captureHard), os.Symlink(filepath.Join(out, "models.info.json"), dangling), os.Symlink(existing, existingLink)); err != nil {
		t.Fatal(err)
	}
	inputs, captures := readDir(t, valid), readDir(t, existing)
	n := 0
	tampered := func(mutate func(r *Request)) string {
		n++
		r := good
		r.Headers = map[string]string{"Accept": "application/json"}
		mutate(&r)
		return writeRecordings(t, filepath.Join(base, "tampered"+strconv.Itoa(n)), true, Record{Request: r})
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"compare"}, "-hfd-url"},
		{[]string{"compare", "-hfd-url", "ftp://x", "-recordings", valid}, "plain http(s) origin"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "extra"}, "unexpected arguments"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-timeout", "0"}, "timeout must be positive"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-max-body", "1MB"}, "size"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", filepath.Join(base, "missing")}, "missing"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", noMeta}, "meta.json"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", empty}, "no cases"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", invalid}, "x.json"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", renamed}, "other.json"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodPost })}, "not allowed"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodDelete })}, "not allowed"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = "" })}, "not allowed"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Body = "x" })}, "body"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Headers["Authorization"] = "Bearer x" })}, "header"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Headers["cookie"] = "a=b" })}, "header"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Path = "//evil/x" })}, "root-relative"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Path = "http://evil/x" })}, "root-relative"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Path = "" })}, "root-relative"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodPost; r.Path = "/api/models/ns/name/paths-info/../commit/main" })}, "root-relative"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodPost; r.Path = "/api/models/ns/name/paths-info/%2E%2E" })}, "root-relative"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodPost; r.Path = "/api/repos/create?unused=/paths-info/" })}, "not allowed"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodPost; r.Path = "/x/api/models/ns/name/paths-info/main" })}, "not allowed"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodPost; r.Path = "/api/models/ns/name/paths-info/main/extra" })}, "not allowed"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodPost; r.Path = "/api/repos/ns/name/paths-info/main" })}, "not allowed"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodPost; r.Path = "/api/models/ns//paths-info/main" })}, "not allowed"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodPost; r.Path = "/api/models/ns/name/paths-info/." })}, "not allowed"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Method = http.MethodPost; r.Path = "/api/models/ns/name/paths-info/refs%2Fpr%2F1" })}, "not allowed"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", tampered(func(r *Request) { r.Name = "Bad Name" })}, "unsafe case name"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", valid}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", valid + string(filepath.Separator)}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", outLink}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", out, "-report", filepath.Join(valid, "models.info.json")}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", out, "-report", filepath.Join(valid, "..", "valid", "meta.json")}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", out, "-report", linked}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", out, "-report", hard}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", out, "-report", filepath.Join(out, "meta.json")}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", out, "-report", filepath.Join(out, "sub", "..", "models.info.json")}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", existing, "-report", captureLink}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", existing, "-report", captureHard}, "overwrite"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", out, "-report", dangling}, "dangling"},
		{[]string{"compare", "-hfd-url", trap.URL, "-recordings", valid, "-out", existing, "-report", filepath.Join(existingLink, "meta.json")}, "overwrite"},
	} {
		err := run(tc.args, io.Discard)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("run(%q) = %v, want %q", tc.args, err, tc.want)
		}
	}
	if !reflect.DeepEqual(inputs, readDir(t, valid)) {
		t.Error("rejected runs modified the recordings")
	}
	if !reflect.DeepEqual(captures, readDir(t, existing)) {
		t.Error("rejected runs modified the existing captures")
	}
	if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("rejected runs created -out: %v", err)
	}
	_, records, err := loadRecordings(valid)
	if err != nil {
		t.Fatal(err)
	}
	if err := guardTargets(valid, existing, filepath.Join(existingLink, "report.md"), records); err != nil {
		t.Errorf("guardTargets rejected a report beside the captures: %v", err)
	}
}

func TestValidateRequestAcceptsRecorderCases(t *testing.T) {
	for _, f := range []Fixture{defaultFixture, {Model: "ns/na me#1", Dataset: "d/x", Space: "s/y", LFSFile: "dir/we ird.bin"}} {
		for _, c := range mustCases(t, f) {
			if err := validateRequest(c); err != nil {
				t.Errorf("%s: %v", c.Name, err)
			}
		}
	}
}

func newTargetServer(t *testing.T, payload []byte) *fixtureServers {
	tgt := &fixtureServers{payload: payload}
	mux := http.NewServeMux()
	handle := func(pattern string, h http.HandlerFunc) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			tgt.observe(r)
			h(w, r)
		})
	}
	api := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Repo-Commit", "abc123")
	}
	handle("/api/models/ns/name", func(w http.ResponseWriter, r *http.Request) {
		api(w)
		io.WriteString(w, `{"id":"ns/name","likes":1,"private":false,"tags":["a"]}`)
	})
	handle("/api/models/ns/name/xet-read-token/main", func(w http.ResponseWriter, r *http.Request) {
		api(w)
		w.Header().Set("X-Xet-Access-Token", "other-token")
		io.WriteString(w, `{"accessToken":"other-token","casUrl":"https://cas.other","exp":2}`)
	})
	handle("/api/models", func(w http.ResponseWriter, r *http.Request) {
		api(w)
		io.WriteString(w, `[]`)
	})
	handle("/api/models/ns/name/paths-info/main", func(w http.ResponseWriter, r *http.Request) {
		api(w)
		io.WriteString(w, `[{"path":"config.json","type":"file","size":7}]`)
	})
	handle("/ns/name/resolve/main/config.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Etag", `"cfg"`)
		io.WriteString(w, `{"a":2}`)
	})
	handle("/ns/name/resolve/main/model.bin", func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Type", "application/octet-stream")
		h.Set("Content-Length", strconv.Itoa(len(payload)))
		h.Set("Etag", `"oid123"`)
		h.Set("X-Linked-Size", strconv.Itoa(len(payload)))
		if r.Method != http.MethodHead {
			w.Write(payload)
		}
	})
	handle("/", func(w http.ResponseWriter, r *http.Request) {
		api(w)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"error":"not found"}`)
	})
	tgt.hub = httptest.NewServer(mux)
	t.Cleanup(tgt.hub.Close)
	return tgt
}

func TestCompareFilePayloads(t *testing.T) {
	var body atomic.Value
	body.Store(`{"a":1}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body.Load().(string))
	}))
	defer srv.Close()
	rec := newRecorder(mustOrigin(t, srv.URL), 5*time.Second, 1<<20)
	get := Request{Name: "cfg", Method: http.MethodGet, Path: "/ns/name/resolve/main/config.json", File: true}
	head := Request{Name: "cfg.head", Method: http.MethodHead, Path: get.Path, File: true}
	hf, hfHead := rec.record(context.Background(), get), rec.record(context.Background(), head)
	if hf.Error != "" || hf.Final.JSON != nil || hf.Final.SHA256 != sha([]byte(`{"a":1}`)) {
		t.Fatalf("recorded %+v error %q", hf.Final, hf.Error)
	}
	if diffs := compareRecords(hf, rec.record(context.Background(), get)); len(diffs) != 0 {
		t.Errorf("identical config.json: %s", formatDiffs(diffs))
	}
	if diffs := compareRecords(hfHead, rec.record(context.Background(), head)); len(diffs) != 0 {
		t.Errorf("identical HEAD: %s", formatDiffs(diffs))
	}
	body.Store(`{"a":2}`)
	diffs := compareRecords(hf, rec.record(context.Background(), get))
	if want := fmt.Sprintf("response.body value [%s] [%s]", digest(`{"a":1}`), digest(`{"a":2}`)); formatDiffs(diffs) != want || classify(diffs) != resultDiff {
		t.Errorf("changed config.json: %s", formatDiffs(diffs))
	}
}

func TestCompareRoundTrip(t *testing.T) {
	f := newFixtureServers(t)
	dir := t.TempDir()
	hfDir := filepath.Join(dir, "hf")
	args := []string{"record", "-base", f.hub.URL, "-out", hfDir, "-model", "ns/name", "-dataset", "dns/dname", "-space", "sns/sname", "-lfs-file", "model.bin", "-timeout", "10s"}
	if err := run(args, io.Discard); err != nil {
		t.Fatal(err)
	}
	info := loadRecord(t, readDir(t, hfDir)["models.info.json"])
	info.Request.Path = "/api/models/ns/name?custom=1"
	if err := writeJSON(filepath.Join(hfDir, "models.info.json"), info); err != nil {
		t.Fatal(err)
	}
	inputs := readDir(t, hfDir)

	target := newTargetServer(t, f.payload)
	outDir, reportPath := filepath.Join(dir, "hfd"), filepath.Join(dir, "hf-api-diff.md")
	var out bytes.Buffer
	if err := run([]string{"compare", "-hfd-url", target.hub.URL, "-recordings", hfDir, "-out", outDir, "-report", reportPath, "-timeout", "10s"}, &out); err != nil {
		t.Fatalf("compare: %v\n%s", err, out.String())
	}
	if got := target.requests(http.MethodGet, "/api/models/ns/name"); len(got) != 1 || got[0].RawQuery != "custom=1" || got[0].Header.Get("Accept") != "application/json" ||
		got[0].Header.Get("Authorization") != "" || got[0].Header.Get("Accept-Encoding") != "" {
		t.Errorf("info replay %+v", got)
	}
	if got := target.requests(http.MethodPost, "/api/models/ns/name/paths-info/main"); len(got) != 1 || got[0].Body != `{"paths":["config.json","model.bin","does-not-exist.txt"],"expand":true}` ||
		got[0].Header.Get("Content-Type") != "application/json" {
		t.Errorf("paths-info replay %+v", got)
	}
	if got := target.requests(http.MethodGet, "/api/models"); len(got) != 1 || got[0].RawQuery != "author=ns&limit=5&search=name" {
		t.Errorf("list replay %+v", got)
	}
	if got := target.requests(http.MethodHead, "/ns/name/resolve/main/model.bin"); len(got) != 1 {
		t.Errorf("lfs head replay %+v", got)
	}
	if len(f.requests(http.MethodGet, "/api/models/ns/name")) != 1 {
		t.Error("compare contacted the recorded hub")
	}
	if !reflect.DeepEqual(inputs, readDir(t, hfDir)) {
		t.Error("compare modified the recordings")
	}
	if outputs := readDir(t, outDir); len(outputs) != 35 || loadRecord(t, outputs["models.info.json"]).Request.Path != "/api/models/ns/name?custom=1" {
		t.Errorf("hfd captures: %d files", len(outputs))
	}
	report, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		f.hub.URL, target.hub.URL, "Summary: 29 match, 3 content diff, 2 status diff, 0 capture error.",
		"| ✅ match | models.paths-info |", "| ✅ match | models.xet-read-token |", "| ✅ match | models.notfound |", "| ✅ match | models.resolve.notfound |",
		"| ⚠️ content diff | models.info | 200 | 200 | 1 |", "<summary>⚠️ models.info — content diff (1 difference)</summary>\n\nRequest: `GET /api/models/ns/name?custom=1`\n",
		"@@ response.json.tags (only in hfd) @@\n- <absent>\n+ [\n+   \"a\"\n+ ]\n",
		"| ❌ status diff | models.resolve.lfs | 302 -> 200 | 200 |", "Request: `GET /ns/name/resolve/main/model.bin`\n\nPayload: identical (200000 bytes, sha256:" + sha(f.payload) + ")\n",
		"@@ response.status (status differs) @@\n- 302 -> 200\n+ 200\n", "| ❌ status diff | models.resolve.lfs.head | 302 -> 200 | 200 |",
		"| ⚠️ content diff | models.resolve.config |", "@@ response.body (value differs) @@\n- sha256:",
		"| ⚠️ content diff | models.list |", "@@ response.json[id=ns/name] (missing in hfd) @@\n- {\n-   \"id\": \"ns/name\"\n- }\n+ <absent>\n", "@@ response.headers.Link (missing in hfd) @@",
	} {
		if !bytes.Contains(report, []byte(want)) {
			t.Errorf("report lacks %q", want)
		}
	}
	if t.Failed() {
		t.Logf("report:\n%s", report)
	}
	if !strings.Contains(out.String(), "29 match, 3 differ, 2 error") {
		t.Errorf("stdout %q", out.String())
	}

	again := filepath.Join(dir, "again.md")
	if err := run([]string{"compare", "-hfd-url", target.hub.URL, "-recordings", hfDir, "-out", filepath.Join(dir, "hfd2"), "-report", again, "-timeout", "10s"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if second, _ := os.ReadFile(again); !bytes.Equal(report, second) {
		t.Errorf("reports differ between runs:\n%s\n---\n%s", report, second)
	}

	closed := httptest.NewServer(http.NotFoundHandler())
	addr := closed.URL
	closed.Close()
	failed := filepath.Join(dir, "failed.md")
	err = run([]string{"compare", "-hfd-url", addr, "-recordings", hfDir, "-out", filepath.Join(dir, "hfd3"), "-report", failed, "-timeout", "5s"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "34 of 34") {
		t.Fatalf("unreachable target: %v", err)
	}
	data, err := os.ReadFile(failed)
	if err != nil || !bytes.Contains(data, []byte("Summary: 0 match, 0 content diff, 0 status diff, 34 capture error.")) || !bytes.Contains(data, []byte("| ❌ capture error | models.info | 200 | error | 1 |")) ||
		!bytes.Contains(data, []byte("@@ response (request error) @@\n- 200\n+ Get ")) {
		t.Errorf("failed report %v:\n%s", err, data)
	}
	if _, ok := readDir(t, filepath.Join(dir, "hfd3"))["meta.json"]; !ok {
		t.Error("hfd captures missing meta.json after transport failures")
	}
}
