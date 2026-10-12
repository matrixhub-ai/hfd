package lfs

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

// Policy refuses regular (non-LFS) git blobs the way huggingface.co does; the zero value disables every rule.
type Policy struct {
	MaxRegularFileSize int64 // regular blobs larger than this are refused; 0 disables the rule
	MaxBinaryFileSize  int64 // binary blobs larger than this are refused; 0 disables the rule
}

// DefaultPolicy returns huggingface.co's limits.
func DefaultPolicy() Policy {
	return Policy{MaxRegularFileSize: 10 << 20, MaxBinaryFileSize: 100000}
}

// Enabled reports whether any rule applies.
func (p Policy) Enabled() bool {
	return p.MaxRegularFileSize > 0 || p.MaxBinaryFileSize > 0
}

// BinarySniffSize is libgit2's GIT_FILTER_BYTES_TO_CHECK_FOR_BINARY.
const BinarySniffSize = 8000

// Violation is the rule a blob breaks.
type Violation int

const (
	ViolationNone Violation = iota
	ViolationLarge
	ViolationBinary
)

// Check classifies a blob of size bytes whose first min(size, BinarySniffSize) bytes are head.
func (p Policy) Check(size int64, head []byte) Violation {
	head = head[:min(len(head), BinarySniffSize)]
	var v Violation
	switch {
	case p.MaxRegularFileSize > 0 && size > p.MaxRegularFileSize:
		v = ViolationLarge
	case p.MaxBinaryFileSize > 0 && size > p.MaxBinaryFileSize && IsBinaryContent(head):
		v = ViolationBinary
	default:
		return ViolationNone
	}
	if size < MaxLFSPointerSize {
		if ptr, _ := DecodePointer(bytes.NewReader(head)); ptr != nil {
			return ViolationNone
		}
	}
	return v
}

// IsBinaryContent is libgit2's git_str_is_binary, huggingface.co's detector for committed and pushed blobs.
func IsBinaryContent(head []byte) bool {
	switch {
	case bytes.HasPrefix(head, []byte{0xEF, 0xBB, 0xBF}):
		head = head[3:]
	case bytes.HasPrefix(head, []byte{0xFE, 0xFF}), bytes.HasPrefix(head, []byte{0xFF, 0xFE}), bytes.HasPrefix(head, []byte{0x00, 0x00, 0xFE, 0xFF}):
		return true // UTF-16 and UTF-32 byte order marks
	}
	printable, nonprintable := 0, 0
	for _, c := range head {
		switch {
		case (c > 0x1F && c != 0x7F) || c == '\b' || c == 0x1B || c == '\f':
			printable++
		case c == 0:
			return true
		case strings.IndexByte(" \t\n\f\r\v", c) < 0:
			nonprintable++
		}
	}
	return printable>>7 < nonprintable
}

// IsBinarySample is isbinaryfile 5.0.4's isBinaryCheck, huggingface.co's detector for preupload samples (the hub's release is not known).
func IsBinarySample(sample []byte) bool {
	if len(sample) == 0 {
		return false
	}
	switch {
	case bytes.HasPrefix(sample, []byte{0xEF, 0xBB, 0xBF}),
		bytes.HasPrefix(sample, []byte{0x00, 0x00, 0xFE, 0xFF}),
		bytes.HasPrefix(sample, []byte{0xFF, 0xFE, 0x00, 0x00}),
		bytes.HasPrefix(sample, []byte{0x84, 0x31, 0x95, 0x33}):
		return false // UTF-8, UTF-32 and GB18030 byte order marks
	case bytes.HasPrefix(sample, []byte("%PDF-")):
		return true
	case bytes.HasPrefix(sample, []byte{0xFE, 0xFF}), bytes.HasPrefix(sample, []byte{0xFF, 0xFE}):
		return false // UTF-16 byte order marks
	}
	b := sample[:min(len(sample), 512)]
	total := len(b)
	continuation := func(c byte) bool { return c > 127 && c < 192 }
	suspicious := 0
	for i := 0; i < total; i++ {
		c := b[i]
		if c == 0 {
			return true
		}
		if (c >= 7 && c <= 14) || (c >= 32 && c <= 127) {
			continue
		}
		switch {
		case c >= 0xC0 && c < 224 && i+1 < total:
			i++
			if continuation(b[i]) {
				continue
			}
		case c > 223 && c < 240 && i+2 < total:
			i++
			if continuation(b[i]) && continuation(b[i+1]) {
				i++
				continue
			}
		case c > 239 && c < 248 && i+3 < total:
			i++
			if continuation(b[i]) && continuation(b[i+1]) && continuation(b[i+2]) {
				i += 2
				continue
			}
		}
		suspicious++
		if i >= 32 && suspicious*10 > total {
			return true
		}
	}
	return suspicious*10 > total || (suspicious > 1 && isBinaryProto(b))
}

// isBinaryProto is isbinaryfile's protobuf taste: b parses as at least one wire-format field, a short final field allowed.
func isBinaryProto(b []byte) bool {
	r := &protoReader{b: b}
	fields := 0
	for {
		if !r.field() && !r.err {
			return false
		}
		if r.err {
			return fields > 0
		}
		fields++
	}
}

// protoReader mirrors isbinaryfile's Reader: reads past the end set err and yield 0xff.
type protoReader struct {
	b   []byte
	off int
	err bool
}

func (r *protoReader) next() byte {
	if r.off == len(r.b) || r.err {
		r.err = true
		return 0xff
	}
	c := r.b[r.off]
	r.off++
	return c
}

func (r *protoReader) skip(n int32) {
	if n <= 0 {
		return
	}
	if r.err || int(n) > len(r.b)-r.off {
		r.off, r.err = len(r.b), true
		return
	}
	r.off += int(n)
}

// varint keeps JavaScript's 32-bit arithmetic: shift counts wrap at 32 and the result may go negative.
func (r *protoReader) varint() int32 {
	var v int32
	for idx := 0; !r.err; idx++ {
		c := r.next()
		v |= int32(c&0x7f) << (uint(7*idx) & 31)
		if c&0x80 == 0 {
			break
		}
	}
	return v
}

func (r *protoReader) field() bool {
	switch r.varint() & 7 {
	case 0:
		r.varint()
	case 1:
		r.skip(8)
	case 2:
		r.skip(r.varint())
	case 5:
		r.skip(4)
	default:
		return false
	}
	return true
}

// OffendingFile is a refused blob's path and the full name of the ref that introduced it.
type OffendingFile struct {
	Path string
	Ref  string
}

// PolicyError is huggingface.co's rejection of a commit or push that carries refused blobs.
type PolicyError struct {
	Kind  Violation
	Files []OffendingFile
	Limit int64 // MaxRegularFileSize, named in the large-file text
}

// Error is the Hub commit API's message: Banner without its rules, on one line.
func (e *PolicyError) Error() string {
	lines := e.Banner()
	return strings.Join(lines[1:len(lines)-1], " ")
}

// Banner is the message huggingface.co's git remote prints for a rejected push, without colour codes.
func (e *PolicyError) Banner() []string {
	rule := strings.Repeat("-", 73)
	lines := []string{rule}
	if e.Kind == ViolationBinary {
		lines = append(lines,
			"Your push was rejected because it contains binary files.",
			"Please use https://huggingface.co/docs/hub/xet to store binary files.",
			"See also: https://huggingface.co/docs/hub/xet/using-xet-storage#git")
	} else {
		lines = append(lines,
			"Your push was rejected because it contains files larger than "+sizeText(e.Limit)+".",
			"Please use https://git-lfs.github.com/ to store large files.",
			"See also: https://hf.co/docs/hub/repositories-getting-started#terminal")
	}
	lines = append(lines, "", "Offending files:")
	for _, f := range e.Files {
		lines = append(lines, "  - "+f.Path+" (ref: "+f.Ref+")")
	}
	return append(lines, rule)
}

// WriteBanner writes every Banner line in red, as huggingface.co's git remote does.
func (e *PolicyError) WriteBanner(w io.Writer) error {
	var b strings.Builder
	for _, line := range e.Banner() {
		b.WriteString("\x1b[31m" + line + "\x1b[0m\n")
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func sizeText(n int64) string {
	switch {
	case n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	case n%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", n>>10)
	}
	return fmt.Sprintf("%d bytes", n)
}
