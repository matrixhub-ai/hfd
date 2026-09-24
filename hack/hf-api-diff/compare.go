package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

// loadRecordings reads meta.json and every case file, validating each stored request before it can be replayed.
func loadRecordings(dir string) (Meta, []Record, error) {
	var meta Meta
	entries, err := os.ReadDir(dir)
	if err != nil {
		return meta, nil, err
	}
	data, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return meta, nil, err
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, nil, fmt.Errorf("meta.json: %w", err)
	}
	var records []Record
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == "meta.json" || !strings.HasSuffix(name, ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return meta, nil, err
		}
		var rec Record
		if err := json.Unmarshal(data, &rec); err != nil {
			return meta, nil, fmt.Errorf("%s: %w", name, err)
		}
		if err := validateRequest(rec.Request); err != nil {
			return meta, nil, fmt.Errorf("%s: %w", name, err)
		}
		if rec.Request.Name+".json" != name {
			return meta, nil, fmt.Errorf("%s: case name %q does not match the file name", name, rec.Request.Name)
		}
		records = append(records, rec)
	}
	if len(records) == 0 {
		return meta, nil, fmt.Errorf("%s: no cases", dir)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Request.Name < records[j].Request.Name })
	return meta, records, nil
}

// validateRequest limits replay to the read-only requests the recorder itself produces.
func validateRequest(r Request) error {
	if !caseFile.MatchString(r.Name) {
		return fmt.Errorf("unsafe case name %q", r.Name)
	}
	u, err := url.Parse(r.Path)
	if err != nil || !strings.HasPrefix(r.Path, "/") || u.Scheme != "" || u.Host != "" || u.User != nil || slices.Contains(strings.Split(u.Path, "/"), "..") {
		return fmt.Errorf("path %q must be root-relative without dot-dot segments", r.Path)
	}
	readOnly := r.Method == http.MethodGet || r.Method == http.MethodHead || (r.Method == http.MethodPost && pathsInfoRoute(u.Path))
	if !readOnly {
		return fmt.Errorf("method %q not allowed for %q", r.Method, r.Path)
	}
	if r.Body != "" && r.Method != http.MethodPost {
		return fmt.Errorf("body not allowed for %s", r.Method)
	}
	for name := range r.Headers {
		switch http.CanonicalHeaderKey(name) {
		case "Authorization", "Proxy-Authorization", "Cookie":
			return fmt.Errorf("header %s not allowed", name)
		}
	}
	return nil
}

// pathsInfoRoute matches the decoded path /api/{models|datasets|spaces}/{namespace}/{repo}/paths-info/{rev} and nothing else.
func pathsInfoRoute(path string) bool {
	seg := strings.Split(path, "/")
	if len(seg) != 7 || seg[0] != "" || seg[1] != "api" || !slices.Contains([]string{"models", "datasets", "spaces"}, seg[2]) || seg[5] != "paths-info" {
		return false
	}
	for _, s := range []string{seg[3], seg[4], seg[6]} {
		if s == "" || s == "." || s == ".." {
			return false
		}
	}
	return true
}

// compareAll replays the recorded requests and writes the report before surfacing transport failures.
func compareAll(ctx context.Context, rec *recorder, meta Meta, records []Record, dir, reportPath string, stdout io.Writer) error {
	cases := make([]Request, len(records))
	for i, r := range records {
		cases[i] = r.Request
	}
	captures, recordErr := recordAll(ctx, rec, cases, dir, meta.Fixture, stdout)
	byName := map[string]Record{}
	for _, c := range captures {
		byName[c.Request.Name] = c
	}
	results := make([]caseResult, len(records))
	for i, hf := range records {
		hfd, ok := byName[hf.Request.Name]
		if !ok {
			hfd = Record{Request: hf.Request, Error: "not captured"}
		}
		results[i] = caseResult{HF: hf, HFD: hfd, Diffs: compareRecords(hf, hfd)}
	}
	if err := os.WriteFile(reportPath, renderReport(meta, rec.base.String(), results), 0o644); err != nil {
		return errors.Join(recordErr, err)
	}
	counts := resultCounts(results)
	fmt.Fprintf(stdout, "%d match, %d differ, %d error; report written to %s\n", counts[resultMatch], counts[resultDiff], counts[resultError], reportPath)
	return recordErr
}
