package repository

// Tests for the upload-pack want filter, which refuses a want its allowed
// predicate rejects with git's "not our ref" error.

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
)

// wantTestDelim stands for a delim-pkt in wantTestPkts.
const wantTestDelim = "\x00delim"

// wantTestNotOurRef starts the ERR pkt-line refusing a want.
const wantTestNotOurRef = "ERR upload-pack: not our ref "

// wantTestPkts encodes pkt-lines like pktLines, with wantTestDelim encoding a delim-pkt.
func wantTestPkts(t *testing.T, lines ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	for _, line := range lines {
		if line == wantTestDelim {
			if err := pktline.WriteDelim(&buf); err != nil {
				t.Fatalf("write delim: %v", err)
			}
			continue
		}
		buf.Write(pktLines(t, line))
	}
	return buf.Bytes()
}

// wantTestPktLines decodes pkt-lines like readPktLines, but keeps an ERR pkt-line, which pktline reports as an error.
func wantTestPktLines(t *testing.T, data []byte) []string {
	t.Helper()
	rd := bytes.NewReader(data)
	var lines []string
	for rd.Len() > 0 {
		_, line, err := pktline.ReadLine(rd)
		var errLine *pktline.ErrorLine
		if err != nil && !errors.As(err, &errLine) {
			t.Fatalf("read pkt-line from %q: %v", data, err)
		}
		lines = append(lines, strings.TrimSuffix(string(line), "\n"))
	}
	return lines
}

// wantTestAllow returns an allowed predicate admitting only oids, appending each hash it is asked about to asked when non-nil.
func wantTestAllow(asked *[]string, oids ...string) func(plumbing.Hash) (bool, error) {
	return func(h plumbing.Hash) (bool, error) {
		if asked != nil {
			*asked = append(*asked, h.String())
		}
		return slices.Contains(oids, h.String()), nil
	}
}

// wantTestDrain reads r three bytes at a time, splitting every pkt-line, until a read fails; it returns the bytes read and that error.
func wantTestDrain(r io.Reader) ([]byte, error) {
	var got []byte
	buf := make([]byte, 3)
	idle := 0
	for idle < 100 {
		n, err := r.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			return got, err
		}
		if n == 0 {
			idle++
		} else {
			idle = 0
		}
	}
	return got, io.ErrNoProgress
}

func TestWantFilter(t *testing.T) {
	oidA, oidB, oidC := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	v0First := "want " + oidA + " multi_ack_detailed side-band-64k\n"
	v0 := pktLines(t, v0First, "want "+oidB+"\n", "", "have "+strings.Repeat("d", 40)+"\n", "done\n")
	v2 := func(oid string) []byte {
		return wantTestPkts(t, "command=fetch\n", "agent=x\n", wantTestDelim, "want "+oid+"\n", "done\n", "")
	}
	wantRef := wantTestPkts(t, "command=fetch\n", wantTestDelim, "want-ref refs/heads/main\n", "done\n", "")
	// A delim-pkt, a response-end-pkt and a flush-pkt.
	special := []byte("0001" + "0002" + "0000")

	cases := []struct {
		name    string
		input   []byte
		allowed []string
		// forward is what the filter yields before it stops.
		forward []byte
		// refused prefixes the one ERR pkt-line written to output; "" expects io.EOF and no output.
		refused string
	}{
		{"V0AllowedWantsPassThrough", v0, []string{oidA, oidB}, v0, ""},
		{"V0SecondWantNotOurRef", v0, []string{oidA}, pktLines(t, v0First), wantTestNotOurRef + oidB},
		{"V2AllowedWantPassesThrough", v2(oidA), []string{oidA}, v2(oidA), ""},
		{"V2WantNotOurRef", v2(oidC), []string{oidA}, wantTestPkts(t, "command=fetch\n", "agent=x\n", wantTestDelim), wantTestNotOurRef + oidC},
		// A whitespace-padded want is still a want: go-git's v2 fetch decoder trims each line.
		{"V2LeadingSpaceWantNotOurRef", wantTestPkts(t, "command=fetch\n", "agent=x\n", wantTestDelim, " want "+oidC+"\n", "done\n", ""), []string{oidA}, wantTestPkts(t, "command=fetch\n", "agent=x\n", wantTestDelim), wantTestNotOurRef + oidC},
		{"V0TabWantNotOurRef", pktLines(t, "\twant "+oidC+" multi_ack_detailed\n", "", "done\n"), []string{oidA}, nil, wantTestNotOurRef + oidC},
		{"WantRefNotChecked", wantRef, nil, wantRef, ""},
		{"MalformedOIDNotOurRef", pktLines(t, "want "+strings.Repeat("z", 40)+"\n", "", "done\n"), []string{oidA}, nil, wantTestNotOurRef},
		{"SpecialPacketsPassThrough", special, nil, special, ""},
		{"EmptyInputEOF", nil, nil, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			r := newWantFilter(bytes.NewReader(tc.input), &out, wantTestAllow(nil, tc.allowed...))
			got, err := wantTestDrain(r)
			if !bytes.Equal(got, tc.forward) {
				t.Errorf("forwarded %q, want %q", got, tc.forward)
			}
			if tc.refused == "" {
				if !errors.Is(err, io.EOF) {
					t.Errorf("read error = %v, want io.EOF", err)
				}
				if out.Len() != 0 {
					t.Errorf("output = %q, want nothing", out.Bytes())
				}
				return
			}
			if !errors.Is(err, errNotOurRef) {
				t.Fatalf("read error = %v, want errNotOurRef", err)
			}
			if n, err := r.Read(make([]byte, 64)); n != 0 || !errors.Is(err, errNotOurRef) {
				t.Errorf("Read after the refusal = %d, %v; want 0, errNotOurRef", n, err)
			}
			if lines := wantTestPktLines(t, out.Bytes()); len(lines) != 1 || !strings.HasPrefix(lines[0], tc.refused) {
				t.Errorf("output lines = %q, want one starting %q", lines, tc.refused)
			}
		})
	}

	t.Run("InvalidPktLenFails", func(t *testing.T) {
		_, err := wantTestDrain(newWantFilter(strings.NewReader("0003"), io.Discard, wantTestAllow(nil)))
		if err == nil || errors.Is(err, io.EOF) || errors.Is(err, errNotOurRef) {
			t.Fatalf("read error = %v, want a pkt-line length error", err)
		}
	})

	t.Run("AllowedErrorSurfaces", func(t *testing.T) {
		unreadable := errors.New("refs unreadable")
		allowed := func(plumbing.Hash) (bool, error) { return false, unreadable }
		if _, err := wantTestDrain(newWantFilter(bytes.NewReader(v0), io.Discard, allowed)); !errors.Is(err, unreadable) {
			t.Fatalf("read error = %v, want the allowed error %q", err, unreadable)
		}
	})

	t.Run("AllowedAskedOncePerWant", func(t *testing.T) {
		var asked []string
		if _, err := wantTestDrain(newWantFilter(bytes.NewReader(v0), io.Discard, wantTestAllow(&asked, oidA, oidB))); !errors.Is(err, io.EOF) {
			t.Fatalf("read error = %v, want io.EOF", err)
		}
		if want := []string{oidA, oidB}; !slices.Equal(asked, want) {
			t.Errorf("allowed asked about %q, want each want once: %q", asked, want)
		}
	})

	t.Run("AllowedNotCalledWithoutWants", func(t *testing.T) {
		var asked []string
		if _, err := wantTestDrain(newWantFilter(bytes.NewReader(pktLines(t, "")), io.Discard, wantTestAllow(&asked))); !errors.Is(err, io.EOF) {
			t.Fatalf("read error = %v, want io.EOF", err)
		}
		if len(asked) != 0 {
			t.Errorf("allowed asked about %q for a request without wants, want never", asked)
		}
	})
}
