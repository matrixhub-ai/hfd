package main

import (
	"flag"
	"net/url"
	"os"
	"testing"
)

func TestParseConfigHostURL(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"default", nil, "http://localhost:8080"},
		{"empty host", []string{"-addr", ":9090"}, "http://localhost:9090"},
		{"IPv4", []string{"-addr", "127.0.0.1:8080"}, "http://127.0.0.1:8080"},
		{"hostname", []string{"-addr", "hub.example:9090"}, "http://hub.example:9090"},
		{"IPv6 loopback", []string{"-addr", "[::1]:8080"}, "http://[::1]:8080"},
		{"IPv6", []string{"-addr", "[2001:db8::1]:9090"}, "http://[2001:db8::1]:9090"},
		{"IPv6 zone", []string{"-addr", "[fe80::1%eth0]:8080"}, "http://[fe80::1%25eth0]:8080"},
		{"explicit URL", []string{"-addr", "[::1]:8080", "-host-url", "https://hub.example"}, "https://hub.example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfigArgs(t, tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.HostURL != tc.want {
				t.Errorf("HostURL = %q, want %q", cfg.HostURL, tc.want)
			}
			if _, err := url.Parse(cfg.HostURL); err != nil {
				t.Errorf("HostURL is not a valid URL: %v", err)
			}
		})
	}
}

func TestParseConfigInvalidAddress(t *testing.T) {
	if _, err := parseConfigArgs(t, []string{"-addr", "localhost"}); err == nil {
		t.Fatal("expected invalid address error")
	}
}

func TestParseConfigReconstructionTermSplit(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		wantTerms int
		wantBytes int64
		wantErr   bool
	}{
		{name: "xetd defaults", wantTerms: 16, wantBytes: 1 << 20},
		{name: "disabled", args: []string{"-reconstruction-min-terms", "0"}, wantTerms: 0, wantBytes: 1 << 20},
		{name: "tuned", args: []string{"-reconstruction-min-terms", "32", "-reconstruction-min-term-bytes", "4194304"}, wantTerms: 32, wantBytes: 4 << 20},
		{name: "negative terms", args: []string{"-reconstruction-min-terms", "-1"}, wantErr: true},
		{name: "negative bytes", args: []string{"-reconstruction-min-term-bytes", "-1"}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfigArgs(t, tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.ReconstructionMinTerms != tc.wantTerms || cfg.ReconstructionMinTermBytes != tc.wantBytes {
				t.Errorf("term split = (%d, %d), want (%d, %d)", cfg.ReconstructionMinTerms, cfg.ReconstructionMinTermBytes, tc.wantTerms, tc.wantBytes)
			}
		})
	}
}

func parseConfigArgs(t *testing.T, args []string) (*config, error) {
	t.Helper()
	oldFlags, oldArgs := flag.CommandLine, os.Args
	defer func() {
		flag.CommandLine, os.Args = oldFlags, oldArgs
	}()
	flag.CommandLine = flag.NewFlagSet("hfd", flag.ContinueOnError)
	os.Args = append([]string{"hfd"}, args...)
	return parseConfig()
}

func TestParseConfigProxyConcurrency(t *testing.T) {
	cases := []struct {
		name                                                string
		args                                                []string
		downloadPerFile, uploadPerFile, ingests, prefetches int
	}{
		{name: "defaults", downloadPerFile: 4, uploadPerFile: 4, ingests: 16, prefetches: 1},
		{
			name:            "each flag sets its own knob",
			args:            []string{"-proxy-download-concurrency-per-file", "8", "-proxy-upload-concurrency-per-file", "2", "-proxy-max-concurrent-ingests", "32", "-proxy-prefetch-concurrency", "4"},
			downloadPerFile: 8, uploadPerFile: 2, ingests: 32, prefetches: 4,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfigArgs(t, tc.args)
			if err != nil {
				t.Fatal(err)
			}
			got := [4]int{cfg.ProxyDownloadConcurrencyPerFile, cfg.ProxyUploadConcurrencyPerFile, cfg.ProxyMaxConcurrentIngests, cfg.ProxyPrefetchConcurrency}
			if want := [4]int{tc.downloadPerFile, tc.uploadPerFile, tc.ingests, tc.prefetches}; got != want {
				t.Errorf("(download per file, upload per file, ingests, prefetches) = %v, want %v", got, want)
			}
		})
	}
}
