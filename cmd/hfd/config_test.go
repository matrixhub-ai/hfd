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

func TestParseConfigLFSPolicy(t *testing.T) {
	cases := []struct {
		name            string
		args            []string
		regular, binary int64
	}{
		{"default", nil, 10485760, 100000},
		{"custom", []string{"-max-regular-file-size", "5242880", "-max-binary-file-size", "1024"}, 5242880, 1024},
		{"disabled", []string{"-max-regular-file-size", "0", "-max-binary-file-size", "0"}, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfigArgs(t, tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.MaxRegularFileSize != tc.regular || cfg.MaxBinaryFileSize != tc.binary {
				t.Errorf("limits = %d, %d, want %d, %d", cfg.MaxRegularFileSize, cfg.MaxBinaryFileSize, tc.regular, tc.binary)
			}
		})
	}
}

func TestParseConfigNegativeFileSize(t *testing.T) {
	for _, arg := range []string{"-max-regular-file-size=-1", "-max-binary-file-size=-1"} {
		if _, err := parseConfigArgs(t, []string{arg}); err == nil {
			t.Errorf("%s: expected negative size error", arg)
		}
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
