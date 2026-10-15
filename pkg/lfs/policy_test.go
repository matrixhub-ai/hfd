package lfs

import (
	"bytes"
	"slices"
	"strings"
	"testing"
)

const bannerRule = "-------------------------------------------------------------------------"

func TestPolicyEnabled(t *testing.T) {
	if got, want := DefaultPolicy(), (Policy{MaxRegularFileSize: 10485760, MaxBinaryFileSize: 100000}); got != want {
		t.Errorf("DefaultPolicy() = %+v, want %+v", got, want)
	}
	tests := []struct {
		name   string
		policy Policy
		want   bool
	}{
		{"zero", Policy{}, false},
		{"default", DefaultPolicy(), true},
		{"regular only", Policy{MaxRegularFileSize: 1}, true},
		{"binary only", Policy{MaxBinaryFileSize: 1}, true},
	}
	for _, tt := range tests {
		if got := tt.policy.Enabled(); got != tt.want {
			t.Errorf("%s: Enabled() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestPolicyCheck(t *testing.T) {
	text := bytes.Repeat([]byte("a"), BinarySniffSize)
	nul := make([]byte, BinarySniffSize)
	pointer := []byte("version https://git-lfs.github.com/spec/v1\n" +
		"oid sha256:4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e2393\n" +
		"size 12345\n")
	if len(pointer) != 130 {
		t.Fatalf("pointer fixture is %d bytes, want 130", len(pointer))
	}
	// A head longer than BinarySniffSize whose only NUL is at offset i.
	longNULAt := func(i int) []byte {
		head := bytes.Repeat([]byte("a"), 9000)
		head[i] = 0
		return head
	}
	tests := []struct {
		name   string
		policy Policy
		size   int64
		head   []byte
		want   Violation
	}{
		{"regular at the limit", DefaultPolicy(), 10485760, text, ViolationNone},
		{"regular over the limit", DefaultPolicy(), 10485761, text, ViolationLarge},
		{"binary at the limit", DefaultPolicy(), 100000, nul, ViolationNone},
		{"binary over the limit", DefaultPolicy(), 100001, nul, ViolationBinary},
		{"text over the binary limit", DefaultPolicy(), 100001, text, ViolationNone},
		{"nul past the sniff window", DefaultPolicy(), 100001, longNULAt(8500), ViolationNone},
		{"nul at the sniff window's last byte", DefaultPolicy(), 100001, longNULAt(7999), ViolationBinary},
		{"large wins over binary", DefaultPolicy(), 10485761, nul, ViolationLarge},
		{"zero policy large", Policy{}, 1 << 40, text, ViolationNone},
		{"zero policy binary", Policy{}, 100001, nul, ViolationNone},
		{"binary rule disabled", Policy{MaxRegularFileSize: 10485760}, 100001, nul, ViolationNone},
		{"regular rule disabled", Policy{MaxBinaryFileSize: 100000}, 10485761, text, ViolationNone},
		{"pointer is exempt", Policy{MaxRegularFileSize: 100}, 130, pointer, ViolationNone},
		{"same size non-pointer", Policy{MaxRegularFileSize: 100}, 130, text[:130], ViolationLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.policy.Check(tt.size, tt.head); got != tt.want {
				t.Errorf("Check(%d) = %v, want %v", tt.size, got, tt.want)
			}
		})
	}
}

func TestPolicyIsBinaryContent(t *testing.T) {
	controls := func(n int) []byte {
		return append(bytes.Repeat([]byte{0x01}, n), bytes.Repeat([]byte("a"), BinarySniffSize-n)...)
	}
	bomControls := append([]byte{0xef, 0xbb, 0xbf}, controls(62)[:BinarySniffSize-3]...)
	high := make([]byte, BinarySniffSize)
	for i := range high {
		high[i] = byte(0x80 + i%0x80)
	}
	nulAtEnd := bytes.Repeat([]byte("a"), BinarySniffSize)
	nulAtEnd[BinarySniffSize-1] = 0
	tests := []struct {
		name string
		head []byte
		want bool
	}{
		{"63 control bytes", controls(63), true},
		{"62 control bytes", controls(62), false},
		{"bytes 0x80-0xff", high, false},
		{"utf-8 text", []byte(strings.Repeat("héllo wörld, 你好\n", 100)), false},
		{"utf-8 bom", []byte("\xef\xbb\xbfhello\n"), false},
		{"utf-8 bom is not counted", bomControls, true},
		{"utf-16 le bom", []byte("\xff\xfehello"), true},
		{"utf-16 be bom", []byte("\xfe\xffhello"), true},
		{"utf-32 le bom", []byte("\xff\xfe\x00\x00hello"), true},
		{"utf-32 be bom", []byte("\x00\x00\xfe\xffhello"), true},
		{"nul at offset 7999", nulAtEnd, true},
		{"8000 clean bytes", bytes.Repeat([]byte("a"), BinarySniffSize), false},
		{"del only", bytes.Repeat([]byte{0x7f}, BinarySniffSize), true},
		{"esc bs ff only", bytes.Repeat([]byte{0x1b, 0x08, 0x0c}, 100), false},
		{"lone cr", []byte("\r"), false},
		{"pdf header", []byte("%PDF-1.7\n%ascii only\n"), false},
		{"empty", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBinaryContent(tt.head); got != tt.want {
				t.Errorf("IsBinaryContent = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPolicyIsBinarySample(t *testing.T) {
	nuls := make([]byte, 16)
	nulAt := func(i int) []byte {
		b := bytes.Repeat([]byte("a"), 1024)
		b[i] = 0
		return b
	}
	escapes := func(n, total int) []byte {
		return append(bytes.Repeat([]byte{0x1b}, n), bytes.Repeat([]byte("a"), total-n)...)
	}
	harmless := make([]byte, 512)
	for i := range harmless {
		harmless[i] = []byte{0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x7f}[i%9]
	}
	tests := []struct {
		name   string
		sample []byte
		want   bool
	}{
		{"empty", nil, false},
		{"utf-8 bom", append([]byte{0xef, 0xbb, 0xbf}, nuls...), false},
		{"utf-16 le bom", append([]byte{0xff, 0xfe, 'h', 0x00}, nuls...), false},
		{"utf-16 be bom", append([]byte{0xfe, 0xff}, nuls...), false},
		{"utf-32 le bom", append([]byte{0xff, 0xfe, 0x00, 0x00}, nuls...), false},
		{"utf-32 be bom", append([]byte{0x00, 0x00, 0xfe, 0xff}, nuls...), false},
		{"gb18030 bom", append([]byte{0x84, 0x31, 0x95, 0x33}, nuls...), false},
		{"pdf header", []byte("%PDF-1.7\n%ascii only\n"), true},
		{"nul at byte 0", nulAt(0), true},
		{"nul at byte 511", nulAt(511), true},
		{"nul at byte 600", nulAt(600), false},
		{"52 escapes of 512", escapes(52, 512), true},
		{"51 escapes of 512", escapes(51, 512), false},
		{"exactly 10 percent", escapes(50, 500), false},
		{"bytes 0x07-0x0e and del", harmless, false},
		{"invalid utf-8", bytes.Repeat([]byte("caf\xe9 "), 100), true},
		{"valid 2-byte utf-8", []byte(strings.Repeat("héllo wörld\n", 40)), false},
		{"valid cjk utf-8", []byte(strings.Repeat("中文文本", 50)), false},
		{"valid 4-byte utf-8", []byte(strings.Repeat("😀", 128)), false},
		{"overlong 2-byte lead 0xc0", bytes.Repeat(append([]byte{0xc0, 0x80}, bytes.Repeat([]byte("a"), 8)...), 51), false},
		// Below 10 % suspicious bytes the sample is tasted as protobuf: 'a' is field 12 wire type 1, eight bytes follow.
		{"protobuf taste", append(bytes.Repeat([]byte("a"), 510), 0xe9, 0xe9), true},
		{"one suspicious byte is not tasted", append(bytes.Repeat([]byte("a"), 511), 0xe9), false},
		{"protobuf taste rejects wire type 3", append(bytes.Repeat([]byte("c"), 510), 0xe9, 0xe9), false},
		{"protobuf taste length-delimited fields", append(bytes.Repeat([]byte("\n\x05hello"), 72), bytes.Repeat([]byte{0xe9}, 8)...), true},
		{"protobuf taste short final field", append(bytes.Repeat([]byte("a"), 30), 0xe9, 0xe9), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBinarySample(tt.sample); got != tt.want {
				t.Errorf("IsBinarySample = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPolicyError(t *testing.T) {
	tests := []struct {
		name string
		err  *PolicyError
		want string
	}{
		{"large", &PolicyError{Kind: ViolationLarge, Files: []OffendingFile{{Path: "b2.txt", Ref: "refs/heads/main"}}, Limit: 10 << 20},
			"Your push was rejected because it contains files larger than 10 MiB. Please use https://git-lfs.github.com/ to store large files. See also: https://hf.co/docs/hub/repositories-getting-started#terminal  Offending files:   - b2.txt (ref: refs/heads/main)"},
		{"binary", &PolicyError{Kind: ViolationBinary, Files: []OffendingFile{{Path: "d3-1.txt", Ref: "refs/heads/main"}}, Limit: 10 << 20},
			"Your push was rejected because it contains binary files. Please use https://huggingface.co/docs/hub/xet to store binary files. See also: https://huggingface.co/docs/hub/xet/using-xet-storage#git  Offending files:   - d3-1.txt (ref: refs/heads/main)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPolicyBanner(t *testing.T) {
	if len(bannerRule) != 73 {
		t.Fatalf("rule fixture is %d dashes, want 73", len(bannerRule))
	}
	tests := []struct {
		name string
		err  *PolicyError
		want []string
	}{
		{"two large files", &PolicyError{Kind: ViolationLarge, Files: []OffendingFile{{Path: "big.txt", Ref: "refs/heads/P1"}, {Path: "dir/big.bin", Ref: "refs/heads/P1"}}, Limit: 10 << 20}, []string{
			bannerRule,
			"Your push was rejected because it contains files larger than 10 MiB.",
			"Please use https://git-lfs.github.com/ to store large files.",
			"See also: https://hf.co/docs/hub/repositories-getting-started#terminal",
			"",
			"Offending files:",
			"  - big.txt (ref: refs/heads/P1)",
			"  - dir/big.bin (ref: refs/heads/P1)",
			bannerRule,
		}},
		{"tag ref", &PolicyError{Kind: ViolationLarge, Files: []OffendingFile{{Path: "big.txt", Ref: "refs/tags/bad"}}, Limit: 10 << 20}, []string{
			bannerRule,
			"Your push was rejected because it contains files larger than 10 MiB.",
			"Please use https://git-lfs.github.com/ to store large files.",
			"See also: https://hf.co/docs/hub/repositories-getting-started#terminal",
			"",
			"Offending files:",
			"  - big.txt (ref: refs/tags/bad)",
			bannerRule,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Banner(); !slices.Equal(got, tt.want) {
				t.Errorf("Banner() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPolicyWriteBanner(t *testing.T) {
	var buf bytes.Buffer
	err := (&PolicyError{Kind: ViolationBinary, Files: []OffendingFile{{Path: "d3-1.txt", Ref: "refs/heads/main"}}, Limit: 10 << 20}).WriteBanner(&buf)
	if err != nil {
		t.Fatalf("WriteBanner: %v", err)
	}
	want := "\x1b[31m" + bannerRule + "\x1b[0m\n" +
		"\x1b[31mYour push was rejected because it contains binary files.\x1b[0m\n" +
		"\x1b[31mPlease use https://huggingface.co/docs/hub/xet to store binary files.\x1b[0m\n" +
		"\x1b[31mSee also: https://huggingface.co/docs/hub/xet/using-xet-storage#git\x1b[0m\n" +
		"\x1b[31m\x1b[0m\n" +
		"\x1b[31mOffending files:\x1b[0m\n" +
		"\x1b[31m  - d3-1.txt (ref: refs/heads/main)\x1b[0m\n" +
		"\x1b[31m" + bannerRule + "\x1b[0m\n"
	if got := buf.String(); got != want {
		t.Errorf("WriteBanner wrote %q, want %q", got, want)
	}
}

func TestPolicyLimitText(t *testing.T) {
	tests := []struct {
		limit int64
		want  string
	}{
		{10 << 20, "10 MiB"},
		{5 << 20, "5 MiB"},
		{512 << 10, "512 KiB"},
		{12345, "12345 bytes"},
	}
	for _, tt := range tests {
		e := &PolicyError{Kind: ViolationLarge, Files: []OffendingFile{{Path: "a.txt", Ref: "refs/heads/main"}}, Limit: tt.limit}
		if got, want := e.Banner()[1], "Your push was rejected because it contains files larger than "+tt.want+"."; got != want {
			t.Errorf("Limit %d: reason = %q, want %q", tt.limit, got, want)
		}
	}
}
