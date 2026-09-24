// hf-api-diff records anonymous Hugging Face API responses of public fixture repositories and replays them against hfd.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const usage = `usage: hf-api-diff record [flags]
       hf-api-diff compare -hfd-url URL [flags]

record captures the fixture API requests against -base into -out, one JSON
file per case plus meta.json. compare replays the recorded requests against
-hfd-url, stores the captures in -out and writes a Markdown -report.
Run "hf-api-diff <command> -h" for the flags.`

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "record":
		return runRecord(args[1:], stdout)
	case "compare":
		return runCompare(args[1:], stdout)
	}
	return fmt.Errorf("unknown command %q\n%s", args[0], usage)
}

func runRecord(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("record", flag.ContinueOnError)
	flags.SetOutput(stdout)
	base := flags.String("base", "https://huggingface.co", "HTTP(S) origin to record")
	out := flags.String("out", "hack/hf-api-diff/recordings/huggingface", "output directory")
	fixture := defaultFixture
	flags.StringVar(&fixture.Model, "model", fixture.Model, "model repository")
	flags.StringVar(&fixture.Dataset, "dataset", fixture.Dataset, "dataset repository")
	flags.StringVar(&fixture.Space, "space", fixture.Space, "space repository")
	flags.StringVar(&fixture.LFSFile, "lfs-file", fixture.LFSFile, "LFS file path inside the model repository")
	timeout := flags.Duration("timeout", 2*time.Minute, "timeout per case including redirects")
	maxBody := sizeFlag(64 << 20)
	flags.Var(&maxBody, "max-body", "maximum body bytes read per response (KiB/MiB/GiB suffixes)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q", flags.Args())
	}
	origin, err := parseOrigin(*base)
	if err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got %v", *timeout)
	}
	cases, err := buildCases(fixture)
	if err != nil {
		return err
	}
	rec := newRecorder(origin, *timeout, int64(maxBody))
	_, err = recordAll(context.Background(), rec, cases, *out, fixture, stdout)
	return err
}

func runCompare(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(stdout)
	hfdURL := flags.String("hfd-url", "", "HTTP(S) origin of the hfd pull mirror to replay against (required)")
	recordings := flags.String("recordings", "hack/hf-api-diff/recordings/huggingface", "directory of recorded Hugging Face cases")
	out := flags.String("out", filepath.Join(os.TempDir(), "hf-api-diff", "hfd"), "directory for the hfd captures")
	report := flags.String("report", "hf-api-diff.md", "Markdown report path")
	timeout := flags.Duration("timeout", 2*time.Minute, "timeout per case including redirects")
	maxBody := sizeFlag(64 << 20)
	flags.Var(&maxBody, "max-body", "maximum body bytes read per response (KiB/MiB/GiB suffixes)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q", flags.Args())
	}
	if *hfdURL == "" {
		return errors.New("-hfd-url is required")
	}
	origin, err := parseOrigin(*hfdURL)
	if err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got %v", *timeout)
	}
	meta, records, err := loadRecordings(*recordings)
	if err != nil {
		return err
	}
	if err := guardTargets(*recordings, *out, *report, records); err != nil {
		return err
	}
	rec := newRecorder(origin, *timeout, int64(maxBody))
	return compareAll(context.Background(), rec, meta, records, *out, *report, stdout)
}

// guardTargets rejects capture and report paths that would overwrite a recording or each other.
func guardTargets(recordings, out, report string, records []Record) error {
	names := []string{"meta.json"}
	for _, r := range records {
		names = append(names, r.Request.Name+".json")
	}
	inputs := make([]os.FileInfo, len(names))
	for i, name := range names {
		info, err := os.Stat(filepath.Join(recordings, name))
		if err != nil {
			return err
		}
		inputs[i] = info
	}
	targets := []string{report}
	reportPath, err := canonical(report)
	if err != nil {
		return err
	}
	reportInfo, _ := os.Stat(report)
	for _, name := range names {
		capture := filepath.Join(out, name)
		path, err := canonical(capture)
		if err != nil {
			return err
		}
		if info, _ := os.Stat(capture); path == reportPath || os.SameFile(info, reportInfo) {
			return fmt.Errorf("-report %q would overwrite capture %q", report, capture)
		}
		targets = append(targets, capture)
	}
	for _, target := range targets {
		info, err := os.Stat(target)
		if err != nil {
			continue
		}
		for i, in := range inputs {
			if os.SameFile(info, in) {
				return fmt.Errorf("%q would overwrite recording %q", target, filepath.Join(recordings, names[i]))
			}
		}
	}
	return nil
}

// canonical resolves the symlinks of the existing prefix of path so aliases of files still to be written compare equal.
func canonical(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if _, err := os.Lstat(abs); err == nil {
		return "", fmt.Errorf("%q is a dangling symlink", abs)
	}
	dir, err := canonical(filepath.Dir(abs))
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, filepath.Base(abs)), nil
}

func parseOrigin(raw string) (*url.URL, error) {
	origin, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if (origin.Scheme != "http" && origin.Scheme != "https") || origin.Host == "" || origin.User != nil ||
		(origin.Path != "" && origin.Path != "/") || origin.RawQuery != "" || origin.Fragment != "" {
		return nil, fmt.Errorf("base %q must be a plain http(s) origin", raw)
	}
	origin.Path, origin.RawPath, origin.ForceQuery = "", "", false
	return origin, nil
}

type sizeFlag int64

func (s *sizeFlag) String() string { return strconv.FormatInt(int64(*s), 10) }

func (s *sizeFlag) Set(value string) error {
	size, err := parseSize(value)
	*s = sizeFlag(size)
	return err
}

func parseSize(value string) (int64, error) {
	digits, multiplier := value, int64(1)
	for suffix, unit := range map[string]int64{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30} {
		if strings.HasSuffix(value, suffix) {
			digits, multiplier = strings.TrimSuffix(value, suffix), unit
		}
	}
	size, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || size <= 0 || size > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("size %q must be a positive integer with an optional KiB/MiB/GiB suffix", value)
	}
	return size * multiplier, nil
}
