package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func formatDiffs(diffs []Difference) string {
	var lines []string
	for _, d := range diffs {
		lines = append(lines, fmt.Sprintf("%s %s [%s] [%s]", d.Path, d.Kind, d.HF, d.HFD))
	}
	return strings.Join(lines, "\n")
}

func TestCompareJSON(t *testing.T) {
	for _, tc := range []struct {
		name, hf, hfd string
		want          []string
	}{
		{"equal", `{"a":[1,"x",null,true]}`, `{"a":[1,"x",null,true]}`, nil},
		{"missing", `{"a":1,"b":2}`, `{"a":1}`, []string{`json.b missing [2] []`}},
		{"extra", `{"a":1}`, `{"a":1,"b":{"c":1}}`, []string{`json.b extra [] [{"c":1}]`}},
		{"type", `{"a":"1"}`, `{"a":1}`, []string{`json.a type ["1"] [1]`}},
		{"value", `{"a":{"b":"x"}}`, `{"a":{"b":"y"}}`, []string{`json.a.b value ["x"] ["y"]`}},
		{"null stays distinct", `{"a":null,"b":null}`, `{"a":{},"b":[]}`, []string{`json.a type [null] [{}]`, `json.b type [null] [[]]`}},
		{"index arrays keep order", `["a","b"]`, `["b","a"]`, []string{`json[0] value ["a"] ["b"]`, `json[1] value ["b"] ["a"]`}},
		{"length", `["a","b","c"]`, `["a"]`, []string{`json length [3] [1]`, `json[1] missing ["b"] []`, `json[2] missing ["c"] []`}},
		{"extra items", `[]`, `[1]`, []string{`json length [0] [1]`, `json[0] extra [] [1]`}},
		{"ignored values", `{"likes":1,"m":{"lastModified":"a"},"spaces":["x"]}`, `{"likes":2,"m":{"lastModified":"b"},"spaces":[]}`, nil},
		{"ignored type", `{"likes":1}`, `{"likes":"1"}`, []string{`json.likes type [1] ["1"]`}},
		{"ignored presence", `{"likes":1}`, `{}`, []string{`json.likes missing [1] []`}},
		{"ignored null", `{"lastModified":"2024"}`, `{"lastModified":null}`, []string{`json.lastModified type ["2024"] [null]`}},
		{"big integers", `{"n":9007199254740993}`, `{"n":9007199254740992}`, []string{`json.n value [9007199254740993] [9007199254740992]`}},
		{"huge integers", `12345678901234567890123`, `12345678901234567890124`, []string{`json value [12345678901234567890123] [12345678901234567890124]`}},
		{"numeric equality", `{"n":1.0,"m":1e2}`, `{"n":1,"m":100}`, nil},
		{"keyed reorder", `[{"path":"a","size":1},{"path":"b","size":2}]`, `[{"path":"b","size":2},{"path":"a","size":1}]`, nil},
		{"keyed change", `[{"path":"a","size":1},{"path":"b","size":2}]`, `[{"path":"b","size":3}]`,
			[]string{`json[path=a] missing [{"path":"a","size":1}] []`, `json[path=b].size value [2] [3]`}},
		{"keyed insertion into empty", `[]`, `[{"rfilename":"x"}]`, []string{`json[rfilename=x] extra [] [{"rfilename":"x"}]`}},
		{"keyed by number", `[{"id":1},{"id":2}]`, `[{"id":2},{"id":1,"x":1}]`, []string{`json[id=1].x extra [] [1]`}},
		{"duplicate keys fall back to index", `[{"path":"a"},{"path":"a"}]`, `[{"path":"a"},{"path":"b"}]`, []string{`json[1].path value ["a"] ["b"]`}},
		{"mixed items fall back to index", `[{"path":"a"},"x"]`, `["x",{"path":"a"}]`, []string{`json[0] type [{"path":"a"}] ["x"]`, `json[1] type ["x"] [{"path":"a"}]`}},
		{"key type mismatch", `[{"id":"1"}]`, `[{"id":1}]`, []string{`json[id=1].id type ["1"] [1]`}},
	} {
		var d diffList
		compareJSON(&d, "json", decodeJSON([]byte(tc.hf)), decodeJSON([]byte(tc.hfd)))
		if got, want := formatDiffs(d), strings.Join(tc.want, "\n"); got != want {
			t.Errorf("%s:\n got: %s\nwant: %s", tc.name, got, want)
		}
	}
}

func jsonResp(status int, contentType, body string, extra ...string) *Response {
	r := textResp(status, contentType, body, extra...)
	r.JSON, r.Text = json.RawMessage(body), ""
	return r
}

func textResp(status int, contentType, body string, extra ...string) *Response {
	r := &Response{Status: status, Headers: map[string]string{"Content-Type": contentType}, Text: body, SHA256: sha([]byte(body)), Size: int64(len(body))}
	for i := 0; i < len(extra); i += 2 {
		r.Headers[extra[i]] = extra[i+1]
	}
	return r
}

func fileResp(status int, payload string, extra ...string) *Response {
	r := textResp(status, "application/octet-stream", payload, extra...)
	r.Text = ""
	return r
}

func digest(payload string) string {
	return fmt.Sprintf("sha256:%s (%d bytes)", sha([]byte(payload))[:16], len(payload))
}

func TestCompareRecords(t *testing.T) {
	api := Request{Name: "c", Method: http.MethodGet, Path: "/x"}
	apiHead := Request{Name: "ch", Method: http.MethodHead, Path: "/x"}
	file := Request{Name: "f", Method: http.MethodGet, Path: "/x", File: true}
	head := Request{Name: "h", Method: http.MethodHead, Path: "/x", File: true}
	invalid := jsonResp(200, "application/json", `{"a":`)
	invalid.JSON = nil
	truncated := fileResp(200, "abc")
	truncated.Truncated = true
	empty := func(status int, contentType string) *Response {
		return &Response{Status: status, Headers: map[string]string{"Content-Type": contentType}}
	}
	jsonFile := func(payload string) *Response { return fileResp(200, payload, "Content-Type", "application/json") }
	redirect := func(hops ...Hop) Record {
		return Record{Request: file, Response: &Response{Status: 302, Headers: map[string]string{"Location": hops[0].Location}}, Hops: hops, Final: fileResp(200, "abc")}
	}
	for _, tc := range []struct {
		name    string
		hf, hfd Record
		result  string
		want    []string
	}{
		{"json metadata ignores size headers and charset", Record{Request: api, Response: jsonResp(200, "application/json; charset=utf-8", `{"a":1}`, "Etag", `"1"`, "Content-Length", "7", "Server", "hf")},
			Record{Request: api, Response: jsonResp(200, "application/json", `{"a":1}`, "Etag", `"2"`, "Content-Length", "8", "Server", "hfd")}, resultMatch, nil},
		{"lowercase stored header names", Record{Request: api, Response: &Response{Status: 200, Headers: map[string]string{"content-type": "text/plain"}, SHA256: sha([]byte("x")), Size: 1}},
			Record{Request: api, Response: textResp(200, "text/plain", "x")}, resultMatch, nil},
		{"media type and params", Record{Request: api, Response: jsonResp(200, "application/json", `{}`, "Link", `</a>; rel="next"`)},
			Record{Request: api, Response: jsonResp(200, "application/problem+json; v=2", `{}`)}, resultDiff,
			[]string{`response.headers.Content-Type value [application/json] [application/problem+json; v=2]`, `response.headers.Link missing [</a>; rel="next"] []`}},
		{"text bodies compare size headers and text", Record{Request: api, Response: textResp(200, "text/plain; charset=utf-8", "hello", "Content-Length", "5")},
			Record{Request: api, Response: textResp(200, "text/plain", "bye", "Content-Length", "3", "X-Total-Count", "1")}, resultDiff,
			[]string{`response.headers.Content-Length value [5] [3]`, `response.headers.X-Total-Count extra [] [1]`, `response.body value [hello] [bye]`}},
		{"file etag and payload", Record{Request: file, Response: fileResp(200, "abc", "Etag", `"a"`, "X-Linked-Etag", `"a"`)},
			Record{Request: file, Response: fileResp(200, "abd", "Etag", `"b"`)}, resultDiff,
			[]string{`response.headers.Etag value ["a"] ["b"]`, `response.headers.X-Linked-Etag missing ["a"] []`, fmt.Sprintf("response.body value [%s] [%s]", digest("abc"), digest("abd"))}},
		{"status mismatch still diffs the rest", Record{Request: api, Response: jsonResp(200, "application/json", `{"a":1}`)},
			Record{Request: api, Response: jsonResp(404, "application/json", `{"error":"x"}`)}, resultError,
			[]string{`response.status status [200] [404]`, `response.json.a missing [1] []`, `response.json.error extra [] ["x"]`}},
		{"redirect chain versus direct", Record{Request: file, Response: &Response{Status: 302, Headers: map[string]string{"Location": "https://cdn/x"}}, Hops: []Hop{{302, "https://cdn/x"}}, Final: fileResp(200, "abc", "Etag", `"a"`)},
			Record{Request: file, Response: fileResp(200, "abc", "Etag", `"a"`), Final: fileResp(200, "abc", "Etag", `"a"`)}, resultError,
			[]string{`response.status status [302 -> 200] [200]`, `response.headers.Content-Type extra [] [application/octet-stream]`, `response.headers.Etag extra [] ["a"]`, `response.headers.Location missing [https://cdn/x] []`}},
		{"final payload differs", Record{Request: file, Response: &Response{Status: 302, Headers: map[string]string{}}, Hops: []Hop{{302, "/a"}}, Final: fileResp(200, "abc")},
			Record{Request: file, Response: &Response{Status: 307, Headers: map[string]string{}}, Hops: []Hop{{307, "/a"}}, Final: fileResp(200, "abd")}, resultError,
			[]string{`response.status status [302 -> 200] [307 -> 200]`, fmt.Sprintf("final.body value [%s] [%s]", digest("abc"), digest("abd"))}},
		{"head responses without bodies", Record{Request: head, Response: &Response{Status: 200, Headers: map[string]string{"Content-Length": "3"}}},
			Record{Request: head, Response: &Response{Status: 200, Headers: map[string]string{"Content-Length": "3"}}}, resultMatch, nil},
		{"head keeps json content type without body", Record{Request: apiHead, Response: empty(200, "application/json")}, Record{Request: apiHead, Response: empty(200, "application/json")}, resultMatch, nil},
		{"not modified without body", Record{Request: api, Response: empty(304, "application/json")}, Record{Request: api, Response: empty(304, "application/json")}, resultMatch, nil},
		{"file get without digest is incomplete", Record{Request: file, Response: &Response{Status: 200, Headers: map[string]string{"Content-Length": "3"}}},
			Record{Request: file, Response: &Response{Status: 200, Headers: map[string]string{"Content-Length": "3"}}}, resultError, []string{`response.body incomplete [no body] [no body]`}},
		{"text without digest is incomplete", Record{Request: api, Response: empty(200, "text/plain")}, Record{Request: api, Response: empty(200, "text/plain")}, resultError,
			[]string{`response.body incomplete [no body] [no body]`}},
		{"json metadata without body is incomplete", Record{Request: api, Response: empty(200, "application/json")}, Record{Request: api, Response: empty(200, "application/json")}, resultError,
			[]string{`response.body incomplete [no body] [no body]`}},
		{"json metadata missing on one side", Record{Request: api, Response: jsonResp(200, "application/json", `{}`)}, Record{Request: api, Response: empty(200, "application/json")}, resultError,
			[]string{`response.body incomplete [json] [no body]`}},
		{"json typed file payloads compare digests", Record{Request: file, Response: jsonFile(`{"a":1}`)}, Record{Request: file, Response: jsonFile(`{"a":1}`)}, resultMatch, nil},
		{"json typed file payloads differ", Record{Request: file, Response: jsonFile(`{"a":1}`)}, Record{Request: file, Response: jsonFile(`{"a":2}`)}, resultDiff,
			[]string{fmt.Sprintf("response.body value [%s] [%s]", digest(`{"a":1}`), digest(`{"a":2}`))}},
		{"error json on file request compares structurally", Record{Request: file, Response: jsonResp(404, "application/json", `{"error":"x"}`)},
			Record{Request: file, Response: jsonResp(404, "application/json", `{"error":"y"}`)}, resultDiff, []string{`response.json.error value ["x"] ["y"]`}},
		{"second hop location differs", redirect(Hop{302, "/a"}, Hop{302, "/b"}), redirect(Hop{302, "/a"}, Hop{302, "/c"}), resultDiff, []string{`hops[1].location value [/b] [/c]`}},
		{"same transport errors never match", Record{Request: api, Error: "timeout"}, Record{Request: api, Error: "timeout"}, resultError, []string{`response error [timeout] [timeout]`}},
		{"missing responses never match", Record{Request: api}, Record{Request: api}, resultError, []string{`response error [-] [-]`}},
		{"error alongside a response", Record{Request: file, Response: &Response{Status: 302, Headers: map[string]string{}}, Hops: []Hop{{302, "/a"}}, Final: fileResp(200, "abc")},
			Record{Request: file, Response: &Response{Status: 302, Headers: map[string]string{}}, Hops: []Hop{{302, "/a"}}, Error: "stopped after 10 redirects"}, resultError,
			[]string{`error error [] [stopped after 10 redirects]`, `response.status status [302 -> 200] [302]`, `final error [200] [-]`}},
		{"identical truncated payloads are incomplete", Record{Request: file, Response: truncated}, Record{Request: file, Response: truncated}, resultError,
			[]string{`response.body incomplete [truncated after 3 bytes] [truncated after 3 bytes]`}},
		{"invalid json on both sides is incomplete", Record{Request: api, Response: invalid}, Record{Request: api, Response: invalid}, resultError,
			[]string{`response.body incomplete [invalid json (5 bytes)] [invalid json (5 bytes)]`}},
		{"json versus invalid json", Record{Request: api, Response: jsonResp(200, "application/json", `{"a":1}`)}, Record{Request: api, Response: invalid}, resultError,
			[]string{`response.body incomplete [json] [invalid json (5 bytes)]`}},
		{"json versus text", Record{Request: api, Response: jsonResp(404, "application/json", `{"a":1}`)}, Record{Request: api, Response: textResp(404, "text/plain", "nope")}, resultDiff,
			[]string{`response.headers.Content-Type value [application/json] [text/plain]`, `response.body type [json] [nope]`}},
	} {
		diffs := compareRecords(tc.hf, tc.hfd)
		if got, want := formatDiffs(diffs), strings.Join(tc.want, "\n"); got != want {
			t.Errorf("%s:\n got: %s\nwant: %s", tc.name, got, want)
		}
		if got := classify(diffs); got != tc.result {
			t.Errorf("%s: result %s, want %s", tc.name, got, tc.result)
		}
	}
}

func TestRenderReport(t *testing.T) {
	meta := Meta{Tool: "hf-api-diff", Base: "https://huggingface.co", RecordedAt: "2026-09-22T10:00:00Z", Fixture: defaultFixture}
	ok := &Response{Status: 200}
	req := func(name, method, path string) Record {
		return Record{Request: Request{Name: name, Method: method, Path: path}, Response: ok}
	}
	var many []Difference
	for i := range 60 {
		many = append(many, Difference{Path: fmt.Sprintf("response.json[%d]", i), Kind: kindValue, HF: "1", HFD: "2"})
	}
	long := strings.Repeat("é", 100)
	post := req("b.diff", "POST", "/api/b|c")
	post.Request.Body = `{"paths":["x"]}`
	results := []caseResult{
		{HF: req("a.match", "GET", "/api/a"), HFD: req("a.match", "GET", "/api/a")},
		{HF: post, HFD: post, Diffs: []Difference{{Path: "response.json.a|b", Kind: kindValue, HF: "x|y\nz`<", HFD: long}}},
		{HF: Record{Request: Request{Name: "c.error", Method: "GET", Path: "/c", File: true}, Response: &Response{Status: 302}, Hops: []Hop{{302, "/x"}}, Final: ok},
			HFD: Record{Error: "dial tcp: refused"}, Diffs: []Difference{{Path: "response", Kind: kindError, HF: "302 -> 200", HFD: "dial tcp: refused"}}},
		{HF: req("d.many", "GET", "/d"), HFD: req("d.many", "GET", "/d"), Diffs: many},
	}
	report := string(renderReport(meta, "http://127.0.0.1:8080", results))
	if !bytes.Equal([]byte(report), renderReport(meta, "http://127.0.0.1:8080", results)) {
		t.Error("report is not deterministic")
	}
	for _, want := range []string{
		"# ", "https://huggingface.co", "2026-09-22T10:00:00Z", "http://127.0.0.1:8080", defaultFixture.Model, defaultFixture.Dataset, defaultFixture.Space,
		"Summary: 1 match, 2 content diff, 0 status diff, 1 capture error.",
		"| Result | Case | HF | hfd | Diffs |",
		"| ✅ match | a.match | 200 | 200 | 0 |",
		"| ⚠️ content diff | b.diff | 200 | 200 | 1 |",
		"| ❌ capture error | c.error | 302 -> 200 | error | 1 |",
		"| ⚠️ content diff | d.many | 200 | 200 | 60 |",
		"<summary>Comparison rules</summary>", "Content-Type without charset", "</details>\n\n## Differences\n",
		"`-` is the Hugging Face value (expected)", "`+` is the hfd value (actual)", "`<absent>`",
		"<details>\n<summary>⚠️ b.diff — content diff (1 difference)</summary>\n\nRequest: `POST /api/b|c` with body `{\"paths\":[\"x\"]}`\n\n" +
			"```diff\n@@ response.json.a|b (value differs) @@\n- x|y\n- z`<\n+ " + long + "\n```\n\n</details>\n",
		"<summary>❌ c.error — capture error (1 difference)</summary>\n\nRequest: `GET /c`\n\n```diff\n@@ response (request error) @@\n- 302 -> 200\n+ dial tcp: refused\n```\n",
		"<summary>⚠️ d.many — content diff (60 differences)</summary>",
		"@@ response.json[49] (value differs) @@\n- 1\n+ 2\n@@ response.json[50] (value differs) @@", "@@ response.json[59] (value differs) @@\n- 1\n+ 2\n```\n",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report lacks %q:\n%s", want, report)
		}
	}
	for _, unwanted := range []string{"<summary>✅", "a.match —", "more differences", "...", "Payload:", "\r"} {
		if strings.Contains(report, unwanted) {
			t.Errorf("report contains %q", unwanted)
		}
	}
	if !strings.HasSuffix(report, "</details>\n") || strings.HasSuffix(report, "\n\n") {
		t.Errorf("report must end with one newline, got %q", report[len(report)-20:])
	}
	if strings.Count(report, "| ✅ match | a.match") != 1 || strings.Count(report, "| ❌ ") != 1 || strings.Count(report, "@@ response.json[") != 60 {
		t.Errorf("rows repeated or dropped:\n%s", report)
	}
	if open, closed := strings.Count(report, "<details>"), strings.Count(report, "</details>"); open != 4 || closed != 4 {
		t.Errorf("%d <details> and %d </details>", open, closed)
	}
	for i, line := range strings.Split(report, "\n") {
		if cells := strings.Count(strings.ReplaceAll(line, `\|`, ""), "|"); strings.HasPrefix(line, "|") && cells != 6 {
			t.Errorf("line %d has %d unescaped pipes: %q", i, cells, line)
		}
	}
}

func renderCase(hf, hfd Record, diffs []Difference) string {
	return string(renderReport(Meta{Fixture: defaultFixture}, "http://hfd", []caseResult{{HF: hf, HFD: hfd, Diffs: diffs}}))
}

func TestRenderReportHunks(t *testing.T) {
	api := Record{Request: Request{Name: "c", Method: http.MethodGet, Path: "/x"}, Response: &Response{Status: 200}}
	for _, tc := range []struct {
		name string
		diff Difference
		want string
	}{
		{"missing empty string", Difference{"response.json.a", kindMissing, `""`, ""}, "@@ response.json.a (missing in hfd) @@\n- \"\"\n+ <absent>\n"},
		{"extra null", Difference{"response.json.b", kindExtra, "", "null"}, "@@ response.json.b (only in hfd) @@\n- <absent>\n+ null\n"},
		{"null versus empty string", Difference{"response.json.c", kindType, "null", `""`}, "@@ response.json.c (type differs) @@\n- null\n+ \"\"\n"},
		{"number versus string", Difference{"response.json.n", kindType, "1", `"1"`}, "@@ response.json.n (type differs) @@\n- 1\n+ \"1\"\n"},
		{"array length", Difference{"final.json", kindLength, "3", "1"}, "@@ final.json (length differs) @@\n- 3\n+ 1\n"},
		{"structured json is indented", Difference{"response.json[path=x]", kindMissing, `{"oid":"1","tags":["a",{"b":null}]}`, ""},
			"@@ response.json[path=x] (missing in hfd) @@\n- {\n-   \"oid\": \"1\",\n-   \"tags\": [\n-     \"a\",\n-     {\n-       \"b\": null\n-     }\n-   ]\n- }\n+ <absent>\n"},
		{"extra array", Difference{"final.json.tags", kindExtra, "", `["a"]`}, "@@ final.json.tags (only in hfd) @@\n- <absent>\n+ [\n+   \"a\"\n+ ]\n"},
		{"escaped newline stays inside the string", Difference{"response.json.m", kindValue, `"a\nb"`, `"c"`}, "@@ response.json.m (value differs) @@\n- \"a\\nb\"\n+ \"c\"\n"},
		{"header values stay verbatim", Difference{"response.headers.X-Json", kindValue, `{"a":1}`, `{"a":2}`}, "@@ response.headers.X-Json (value differs) @@\n- {\"a\":1}\n+ {\"a\":2}\n"},
		{"missing header", Difference{"response.headers.Link", kindMissing, `</a>; rel="next"`, ""}, "@@ response.headers.Link (missing in hfd) @@\n- </a>; rel=\"next\"\n+ <absent>\n"},
		{"present empty header", Difference{"response.headers.X-Empty", kindValue, "", "x"}, "@@ response.headers.X-Empty (value differs) @@\n-\n+ x\n"},
		{"multi-line text body", Difference{"response.body", kindValue, "hello\r\nworld", "bye"}, "@@ response.body (value differs) @@\n- hello\\r\n- world\n+ bye\n"},
		{"long value is not clipped", Difference{"response.json.d", kindValue, `"` + strings.Repeat("x", 300) + `"`, `"y"`}, "@@ response.json.d (value differs) @@\n- \"" + strings.Repeat("x", 300) + "\"\n+ \"y\"\n"},
		{"status chain", Difference{"response.status", kindStatus, "302 -> 200", "200"}, "@@ response.status (status differs) @@\n- 302 -> 200\n+ 200\n"},
		{"one-sided error", Difference{"error", kindError, "", "stopped after 10 redirects"}, "@@ error (request error) @@\n-\n+ stopped after 10 redirects\n"},
		{"incomplete capture", Difference{"response.body", kindIncomplete, "json", "truncated after 3 bytes"}, "@@ response.body (incomplete capture) @@\n- json\n+ truncated after 3 bytes\n"},
	} {
		report := renderCase(api, api, []Difference{tc.diff})
		if !strings.Contains(report, "\n```diff\n"+tc.want+"```\n") {
			t.Errorf("%s: report lacks %q:\n%s", tc.name, tc.want, report)
		}
	}
}

func TestRenderReportLabels(t *testing.T) {
	api := Record{Request: Request{Name: "c", Method: http.MethodGet, Path: "/x"}, Response: &Response{Status: 200}}
	status := Difference{"response.status", kindStatus, "200", "404"}
	value := Difference{"response.json.a", kindValue, "1", "2"}
	failure := Difference{"error", kindError, "", "timeout"}
	incomplete := Difference{"response.body", kindIncomplete, "json", "no body"}
	for _, tc := range []struct {
		name    string
		diffs   []Difference
		row     string
		summary string
	}{
		{"match", nil, "| ✅ match | c | 200 | 200 | 0 |", "1 match, 0 content diff, 0 status diff, 0 capture error"},
		{"content", []Difference{value}, "| ⚠️ content diff | c | 200 | 200 | 1 |", "0 match, 1 content diff, 0 status diff, 0 capture error"},
		{"status outranks content", []Difference{status, value}, "| ❌ status diff | c | 200 | 200 | 2 |", "0 match, 0 content diff, 1 status diff, 0 capture error"},
		{"error outranks status", []Difference{failure, status}, "| ❌ capture error | c | 200 | 200 | 2 |", "0 match, 0 content diff, 0 status diff, 1 capture error"},
		{"incomplete is a capture error", []Difference{value, incomplete}, "| ❌ capture error | c | 200 | 200 | 2 |", "0 match, 0 content diff, 0 status diff, 1 capture error"},
	} {
		report := renderCase(api, api, tc.diffs)
		if !strings.Contains(report, tc.row+"\n") || !strings.Contains(report, "Summary: "+tc.summary+".\n") {
			t.Errorf("%s: report lacks %q or %q:\n%s", tc.name, tc.row, tc.summary, report)
		}
		if got := classify(tc.diffs); (tc.name == "match") != (got == resultMatch) || strings.Contains(tc.row, "❌") != (got == resultError) {
			t.Errorf("%s: label disagrees with classify %s", tc.name, got)
		}
	}
}

func TestRenderReportFences(t *testing.T) {
	api := Record{Request: Request{Name: "c", Method: http.MethodGet, Path: "/x`y`"}, Response: &Response{Status: 200}}
	report := renderCase(api, api, []Difference{{"response.body", kindValue, "```\n</details>\n````diff", "x"}})
	for _, want := range []string{
		"Request: `` GET /x`y` ``\n",
		"\n`````diff\n@@ response.body (value differs) @@\n- ```\n- </details>\n- ````diff\n+ x\n`````\n\n</details>\n",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report lacks %q:\n%s", want, report)
		}
	}
	fences, details := 0, 0
	for _, line := range strings.Split(report, "\n") {
		if strings.HasPrefix(line, "```") {
			fences++
		}
		if line == "<details>" || line == "</details>" {
			details++
		}
	}
	if fences != 2 || details != 4 {
		t.Errorf("%d fence lines and %d details lines:\n%s", fences, details, report)
	}
}

func TestRenderReportPayload(t *testing.T) {
	file := Request{Name: "f", Method: http.MethodGet, Path: "/x", File: true}
	head := Request{Name: "h", Method: http.MethodHead, Path: "/x", File: true}
	redirected := func(req Request, payload string) Record {
		return Record{Request: req, Response: &Response{Status: 302, Headers: map[string]string{"Location": "https://cdn/x"}}, Hops: []Hop{{302, "https://cdn/x"}}, Final: fileResp(200, payload, "Etag", `"a"`)}
	}
	direct := func(req Request, payload string, extra ...string) Record {
		r := fileResp(200, payload, extra...)
		return Record{Request: req, Response: r, Final: r}
	}
	truncated := direct(file, "abc", "Etag", `"a"`)
	truncated.Response.Truncated = true
	identical := fmt.Sprintf("Payload: identical (3 bytes, sha256:%s)\n", sha([]byte("abc")))
	for _, tc := range []struct {
		name    string
		hf, hfd Record
		want    bool
	}{
		{"redirect versus direct", redirected(file, "abc"), direct(file, "abc", "Etag", `"a"`), true},
		{"headers only", direct(file, "abc", "Etag", `"a"`), direct(file, "abc", "Etag", `"b"`), true},
		{"different payload", redirected(file, "abc"), direct(file, "abd", "Etag", `"a"`), false},
		{"head never vouches for bytes", redirected(head, "abc"), direct(head, "abc", "Etag", `"a"`), false},
		{"truncated", redirected(file, "abc"), truncated, false},
		{"redirect error", redirected(file, "abc"), Record{Request: file, Response: &Response{Status: 302, Headers: map[string]string{}}, Hops: []Hop{{302, "/a"}}, Error: "stopped after 10 redirects"}, false},
		{"non-2xx", direct(file, "abc", "Etag", `"a"`), Record{Request: file, Response: fileResp(404, "abc")}, false},
		{"final missing", redirected(file, "abc"), Record{Request: file, Response: fileResp(200, "abc", "Etag", `"a"`)}, false},
	} {
		diffs := compareRecords(tc.hf, tc.hfd)
		report := renderCase(tc.hf, tc.hfd, diffs)
		if len(diffs) == 0 || strings.Contains(report, identical) != tc.want || strings.Count(report, "@@ ") != len(diffs) {
			t.Errorf("%s: %d diffs, want payload line %v:\n%s", tc.name, len(diffs), tc.want, report)
		}
	}
}
