package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"html"
	"maps"
	"net/http"
	"slices"
	"strings"
)

const (
	labelMatch   = "match"
	labelContent = "content diff"
	labelStatus  = "status diff"
	labelCapture = "capture error"
)

type caseResult struct {
	HF, HFD Record
	Diffs   []Difference
}

var (
	marker      = map[string]string{resultMatch: "✅", resultDiff: "⚠️", resultError: "❌"}
	cellEscaper = strings.NewReplacer(`\`, `\\`, "|", `\|`, "`", "\\`", "<", `\<`, "\n", `\n`, "\r", `\r`, "\t", `\t`)
	kindLabel   = map[string]string{kindMissing: "missing in hfd", kindExtra: "only in hfd", kindType: "type differs", kindValue: "value differs",
		kindLength: "length differs", kindStatus: "status differs", kindError: "request error", kindIncomplete: "incomplete capture"}
)

func cell(s string) string { return cellEscaper.Replace(s) }

func summaryStatus(r Record) string {
	if r.Response == nil {
		return "error"
	}
	return statusChain(r)
}

func resultCounts(results []caseResult) map[string]int {
	counts := map[string]int{}
	for _, r := range results {
		counts[classify(r.Diffs)]++
	}
	return counts
}

// label refines classify for readers: an error case is a status diff unless a request failed or a capture is unusable.
func label(diffs []Difference) string {
	switch classify(diffs) {
	case resultMatch:
		return labelMatch
	case resultDiff:
		return labelContent
	}
	for _, diff := range diffs {
		if diff.Kind == kindError || diff.Kind == kindIncomplete {
			return labelCapture
		}
	}
	return labelStatus
}

func backticks(s string) int {
	best, run := 0, 0
	for _, r := range s {
		if r == '`' {
			run++
			best = max(best, run)
		} else {
			run = 0
		}
	}
	return best
}

// code wraps s in a code span delimited by more backticks than any run inside it.
func code(s string) string {
	n := backticks(s)
	if n == 0 {
		return "`" + s + "`"
	}
	delim := strings.Repeat("`", n+1)
	return delim + " " + s + " " + delim
}

func indentJSON(raw string) string {
	var out bytes.Buffer
	if json.Indent(&out, []byte(raw), "", "  ") != nil {
		return raw
	}
	return out.String()
}

func jsonPath(path string) bool {
	return strings.HasPrefix(path, "response.json") || strings.HasPrefix(path, "final.json")
}

func writeSide(b *bytes.Buffer, prefix, value string) {
	value = strings.ReplaceAll(value, "\r", `\r`)
	for _, line := range strings.Split(value, "\n") {
		b.WriteString(prefix)
		if line != "" {
			b.WriteByte(' ')
			b.WriteString(line)
		}
		b.WriteByte('\n')
	}
}

func writeHunk(b *bytes.Buffer, d Difference) {
	fmt.Fprintf(b, "@@ %s (%s) @@\n", d.Path, cmp.Or(kindLabel[d.Kind], d.Kind))
	hf, hfd := d.HF, d.HFD
	if jsonPath(d.Path) {
		hf, hfd = indentJSON(hf), indentJSON(hfd)
	}
	switch d.Kind {
	case kindMissing:
		hfd = "<absent>"
	case kindExtra:
		hf = "<absent>"
	}
	writeSide(b, "-", hf)
	writeSide(b, "+", hfd)
}

func finalResponse(r Record) *Response {
	if r.Final != nil {
		return r.Final
	}
	return r.Response
}

// payloadNote vouches for identical file bytes only when both complete GET captures hash the same payload.
func payloadNote(r caseResult, result string) string {
	req := r.HF.Request
	if !req.File || req.Method != http.MethodGet || r.HF.Error != "" || r.HFD.Error != "" || result == labelCapture {
		return ""
	}
	a, b := finalResponse(r.HF), finalResponse(r.HFD)
	if a == nil || b == nil || a.Status/100 != 2 || b.Status/100 != 2 || a.Truncated || b.Truncated || a.SHA256 == "" || a.SHA256 != b.SHA256 || a.Size != b.Size {
		return ""
	}
	return fmt.Sprintf("Payload: identical (%d bytes, sha256:%s)", a.Size, a.SHA256)
}

func writeCase(b *bytes.Buffer, r caseResult) {
	req, result := r.HF.Request, label(r.Diffs)
	suffix := "s"
	if len(r.Diffs) == 1 {
		suffix = ""
	}
	fmt.Fprintf(b, "<details>\n<summary>%s %s — %s (%d difference%s)</summary>\n\nRequest: %s", marker[classify(r.Diffs)], html.EscapeString(req.Name), result, len(r.Diffs), suffix, code(req.Method+" "+req.Path))
	if req.Body != "" {
		fmt.Fprintf(b, " with body %s", code(req.Body))
	}
	b.WriteString("\n\n")
	if note := payloadNote(r, result); note != "" {
		b.WriteString(note)
		b.WriteString("\n\n")
	}
	var hunks bytes.Buffer
	for _, d := range r.Diffs {
		writeHunk(&hunks, d)
	}
	fence := strings.Repeat("`", max(3, backticks(hunks.String())+1))
	fmt.Fprintf(b, "%sdiff\n%s%s\n\n</details>\n\n", fence, hunks.Bytes(), fence)
}

// renderReport writes the Markdown comparison; it carries no run timestamp so repeated compares of the same data are byte-identical.
func renderReport(meta Meta, target string, results []caseResult) []byte {
	var b bytes.Buffer
	counts := map[string]int{}
	for _, r := range results {
		counts[label(r.Diffs)]++
	}
	fmt.Fprintf(&b, "# Hugging Face API comparison\n\nHugging Face responses recorded from %s at %s, replayed against %s.\n", meta.Base, meta.RecordedAt, target)
	fmt.Fprintf(&b, "Fixture repositories: model `%s`, dataset `%s`, space `%s`; LFS file `%s`.\n\n", meta.Fixture.Model, meta.Fixture.Dataset, meta.Fixture.Space, meta.Fixture.LFSFile)
	fmt.Fprintf(&b, "Summary: %d match, %d content diff, %d status diff, %d capture error.\n\n", counts[labelMatch], counts[labelContent], counts[labelStatus], counts[labelCapture])
	b.WriteString("| Result | Case | HF | hfd | Diffs |\n|:-------|------|----|-----|------:|\n")
	for _, r := range results {
		fmt.Fprintf(&b, "| %s %s | %s | %s | %s | %d |\n", marker[classify(r.Diffs)], label(r.Diffs), cell(r.HF.Request.Name), cell(summaryStatus(r.HF)), cell(summaryStatus(r.HFD)), len(r.Diffs))
	}
	fmt.Fprintf(&b, "\n<details>\n<summary>Comparison rules</summary>\n\n"+
		"- Recordings drop volatile headers, Xet access tokens and signed URL query values.\n"+
		"- Compared headers: %s (Content-Type without charset), plus ETag and Content-Length for file and non-JSON responses.\n"+
		"- JSON bodies are compared structurally; the values of %s are ignored while their presence and type are still compared, "+
		"and arrays of objects are paired by %s when that key is unique on both sides.\n"+
		"- File payloads are compared by redirect chain and final SHA-256 digest.\n"+
		"- A case is a status diff when the status chains differ and a capture error when a request failed or a capture is truncated or invalid; neither counts as a match.\n\n</details>\n",
		strings.Join(slices.Sorted(maps.Keys(comparedHeader)), ", "), strings.Join(slices.Sorted(maps.Keys(ignoredKey)), ", "), strings.Join(arrayKeys, ", "))
	legend := false
	for _, r := range results {
		if len(r.Diffs) == 0 {
			continue
		}
		if !legend {
			b.WriteString("\n## Differences\n\nIn each hunk `-` is the Hugging Face value (expected) and `+` is the hfd value (actual); `<absent>` marks a side without that field.\n\n")
			legend = true
		}
		writeCase(&b, r)
	}
	return append(bytes.TrimRight(b.Bytes(), "\n"), '\n')
}
