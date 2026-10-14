package seekable_test

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/matrixhub-ai/hfd/internal/seekable"
)

// counts records how the streams of a fakeSource were used.
type counts struct {
	opens     int
	closes    int
	bytesRead int64
}

// fakeSource opens fresh streams over content and counts their use.
type fakeSource struct {
	content  []byte
	canSeek  bool
	openErr  error
	closeErr error
	// entered, if set, is closed when a stream Read starts; Reads then wait until release is closed.
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once

	mu     sync.Mutex
	counts counts
}

func (s *fakeSource) open() (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts.opens++
	if s.openErr != nil {
		return nil, s.openErr
	}
	st := &fakeStream{src: s, r: bytes.NewReader(s.content)}
	if s.canSeek {
		return &seekableStream{st}, nil
	}
	return st, nil
}

func (s *fakeSource) snapshot() counts {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts
}

// fakeStream is a forward-only stream that does not implement io.Seeker.
type fakeStream struct {
	src *fakeSource
	r   *bytes.Reader
}

func (f *fakeStream) Read(p []byte) (int, error) {
	if f.src.release != nil {
		f.src.enterOnce.Do(func() { close(f.src.entered) })
		<-f.src.release
	}
	n, err := f.r.Read(p)
	f.src.mu.Lock()
	f.src.counts.bytesRead += int64(n)
	f.src.mu.Unlock()
	return n, err
}

func (f *fakeStream) Close() error {
	f.src.mu.Lock()
	defer f.src.mu.Unlock()
	f.src.counts.closes++
	return f.src.closeErr
}

// seekableStream is a fakeStream that also implements io.Seeker.
type seekableStream struct{ *fakeStream }

func (s *seekableStream) Seek(offset int64, whence int) (int64, error) {
	return s.r.Seek(offset, whence)
}

// testData returns n deterministic bytes without a short period.
func testData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i ^ i>>8)
	}
	return b
}

// newReader returns a Reader of size over src that is closed when the test ends.
func newReader(t *testing.T, src *fakeSource, size int64) *seekable.Reader {
	t.Helper()
	r, err := seekable.New(size, src.open)
	if err != nil {
		t.Fatalf("New(%d) error = %v", size, err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// mustSeek seeks r and fails the test on error.
func mustSeek(t *testing.T, r io.Seeker, offset int64, whence int) {
	t.Helper()
	if _, err := r.Seek(offset, whence); err != nil {
		t.Fatalf("Seek(%d, %d) error = %v", offset, whence, err)
	}
}

// mustRead reads and drops exactly n bytes from r, failing the test on error.
func mustRead(t *testing.T, r io.Reader, n int) {
	t.Helper()
	if _, err := io.ReadFull(r, make([]byte, n)); err != nil {
		t.Fatalf("ReadFull(%d bytes) error = %v", n, err)
	}
}

func TestNew(t *testing.T) {
	errOpen := errors.New("open failed")
	tests := []struct {
		name      string
		size      int64
		openErr   error
		wantErr   bool
		wantOpens int
	}{
		{name: "negative size fails without opening", size: -1, wantErr: true, wantOpens: 0},
		{name: "open error is returned", size: 10, openErr: errOpen, wantErr: true, wantOpens: 1},
		{name: "zero size opens once", size: 0, wantOpens: 1},
		{name: "positive size opens once", size: 10, wantOpens: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &fakeSource{content: testData(10), openErr: tt.openErr}
			r, err := seekable.New(tt.size, src.open)
			if err == nil {
				t.Cleanup(func() { r.Close() })
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("New(%d) error = %v, want error: %v", tt.size, err, tt.wantErr)
			}
			if tt.openErr != nil && (!errors.Is(err, tt.openErr) || r != nil) {
				t.Errorf("New(%d) = (%v, %v), want (nil, %v)", tt.size, r, err, tt.openErr)
			}
			if got, want := src.snapshot(), (counts{opens: tt.wantOpens}); got != want {
				t.Errorf("source counts = %+v, want %+v", got, want)
			}
		})
	}
}

func TestSeek(t *testing.T) {
	const size, start = 100, 10
	tests := []struct {
		name    string
		offset  int64
		whence  int
		wantPos int64
		wantErr bool
	}{
		{name: "from start", offset: 30, whence: io.SeekStart, wantPos: 30},
		{name: "forward from current", offset: 5, whence: io.SeekCurrent, wantPos: 15},
		{name: "backward from current", offset: -10, whence: io.SeekCurrent, wantPos: 0},
		{name: "end is size", offset: 0, whence: io.SeekEnd, wantPos: size},
		{name: "back from end", offset: -40, whence: io.SeekEnd, wantPos: 60},
		{name: "past end", offset: 50, whence: io.SeekEnd, wantPos: 150},
		{name: "past size from start", offset: 200, whence: io.SeekStart, wantPos: 200},
		{name: "negative from start keeps position", offset: -1, whence: io.SeekStart, wantPos: start, wantErr: true},
		{name: "negative from current keeps position", offset: -11, whence: io.SeekCurrent, wantPos: start, wantErr: true},
		{name: "negative from end keeps position", offset: -101, whence: io.SeekEnd, wantPos: start, wantErr: true},
		{name: "invalid whence keeps position", offset: 0, whence: 7, wantPos: start, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newReader(t, &fakeSource{content: testData(size)}, size)
			mustSeek(t, r, start, io.SeekStart)
			got, err := r.Seek(tt.offset, tt.whence)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Seek(%d, %d) error = %v, want error: %v", tt.offset, tt.whence, err, tt.wantErr)
			}
			if err == nil && got != tt.wantPos {
				t.Errorf("Seek(%d, %d) = %d, want %d", tt.offset, tt.whence, got, tt.wantPos)
			}
			if pos, err := r.Seek(0, io.SeekCurrent); err != nil || pos != tt.wantPos {
				t.Errorf("position after Seek(%d, %d) = (%d, %v), want (%d, nil)", tt.offset, tt.whence, pos, err, tt.wantPos)
			}
		})
	}
}

func TestSeekDoesNoIO(t *testing.T) {
	src := &fakeSource{content: testData(100)}
	r := newReader(t, src, 100)
	mustSeek(t, r, 50, io.SeekStart)
	mustSeek(t, r, -20, io.SeekCurrent)
	mustSeek(t, r, 0, io.SeekEnd)
	mustSeek(t, r, 200, io.SeekStart)
	mustSeek(t, r, 0, io.SeekStart)
	if got, want := src.snapshot(), (counts{opens: 1}); got != want {
		t.Errorf("source counts after seeks = %+v, want %+v", got, want)
	}
}

func TestRead(t *testing.T) {
	data := testData(1000)
	readThenRewind := func(t *testing.T, r *seekable.Reader) {
		mustRead(t, r, 200)
		mustSeek(t, r, 50, io.SeekStart)
	}
	tests := []struct {
		name       string
		content    []byte
		size       int64
		canSeek    bool
		prepare    func(t *testing.T, r *seekable.Reader)
		want       []byte
		wantErr    error
		wantCounts counts // zero means unspecified
	}{
		{
			name:       "forward seek discards from the open stream",
			content:    data,
			size:       1000,
			prepare:    func(t *testing.T, r *seekable.Reader) { mustSeek(t, r, 100, io.SeekStart) },
			want:       data[100:],
			wantCounts: counts{opens: 1, bytesRead: 100 + 900},
		},
		{
			name:       "backward seek reopens and discards",
			content:    data,
			size:       1000,
			prepare:    readThenRewind,
			want:       data[50:],
			wantCounts: counts{opens: 2, closes: 1, bytesRead: 200 + 50 + 950},
		},
		{
			name:       "source longer than size is cut at size",
			content:    data[:20],
			size:       10,
			want:       data[:10],
			wantCounts: counts{opens: 1, bytesRead: 10},
		},
		{
			name:    "source shorter than size fails with unexpected EOF",
			content: data[:5],
			size:    10,
			want:    data[:5],
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:       "seekable backward seek seeks the open stream",
			content:    data,
			size:       1000,
			canSeek:    true,
			prepare:    readThenRewind,
			want:       data[50:],
			wantCounts: counts{opens: 1, bytesRead: 200 + 950},
		},
		{
			name:       "seekable forward seek reads nothing to skip",
			content:    data,
			size:       1000,
			canSeek:    true,
			prepare:    func(t *testing.T, r *seekable.Reader) { mustSeek(t, r, 500, io.SeekStart) },
			want:       data[500:],
			wantCounts: counts{opens: 1, bytesRead: 500},
		},
		{
			name:    "seekable rewind after reading past size keeps the stream",
			content: data,
			size:    1000,
			canSeek: true,
			prepare: func(t *testing.T, r *seekable.Reader) {
				mustSeek(t, r, 1100, io.SeekStart)
				if n, err := r.Read(make([]byte, 10)); n != 0 || err != io.EOF {
					t.Fatalf("Read past size = (%d, %v), want (0, EOF)", n, err)
				}
				mustSeek(t, r, 0, io.SeekStart)
			},
			want:       data,
			wantCounts: counts{opens: 1, bytesRead: 1000},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &fakeSource{content: tt.content, canSeek: tt.canSeek}
			r := newReader(t, src, tt.size)
			if tt.prepare != nil {
				tt.prepare(t, r)
			}
			got, err := io.ReadAll(r)
			if err != tt.wantErr {
				t.Errorf("ReadAll error = %v, want %v", err, tt.wantErr)
			}
			if !bytes.Equal(got, tt.want) {
				t.Errorf("ReadAll = %d bytes, want %d matching bytes", len(got), len(tt.want))
			}
			if c := src.snapshot(); tt.wantCounts != (counts{}) && c != tt.wantCounts {
				t.Errorf("source counts = %+v, want %+v", c, tt.wantCounts)
			}
		})
	}
}

func TestReadAtOrPastSize(t *testing.T) {
	tests := []struct {
		name      string
		size      int64
		readFirst int
		pos       int64
	}{
		{name: "at size", size: 100, pos: 100},
		{name: "past size", size: 100, pos: 150},
		{name: "past size after a partial read", size: 100, readFirst: 10, pos: 150},
		{name: "empty", size: 0, pos: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &fakeSource{content: testData(200)}
			r := newReader(t, src, tt.size)
			mustRead(t, r, tt.readFirst)
			mustSeek(t, r, tt.pos, io.SeekStart)
			before := src.snapshot()
			if n, err := r.Read(make([]byte, 10)); n != 0 || err != io.EOF {
				t.Errorf("Read at %d of %d = (%d, %v), want (0, EOF)", tt.pos, tt.size, n, err)
			}
			if after := src.snapshot(); after != before {
				t.Errorf("Read at %d of %d touched the source: counts %+v, want %+v", tt.pos, tt.size, after, before)
			}
		})
	}
}

func TestClose(t *testing.T) {
	t.Run("returns the stream close error", func(t *testing.T) {
		errClose := errors.New("close failed")
		r := newReader(t, &fakeSource{content: testData(10), closeErr: errClose}, 10)
		if err := r.Close(); !errors.Is(err, errClose) {
			t.Errorf("Close error = %v, want %v", err, errClose)
		}
	})
	t.Run("second close is a no-op", func(t *testing.T) {
		src := &fakeSource{content: testData(10)}
		r := newReader(t, src, 10)
		if err := r.Close(); err != nil {
			t.Fatalf("first Close error = %v", err)
		}
		if err := r.Close(); err != nil {
			t.Errorf("second Close error = %v, want nil", err)
		}
		if got := src.snapshot().closes; got != 1 {
			t.Errorf("stream closed %d times, want 1", got)
		}
	})
	ops := []struct {
		name string
		call func(r *seekable.Reader) error
	}{
		{
			name: "Read",
			call: func(r *seekable.Reader) error {
				_, err := r.Read(make([]byte, 1))
				return err
			},
		},
		{
			name: "Seek",
			call: func(r *seekable.Reader) error {
				_, err := r.Seek(0, io.SeekStart)
				return err
			},
		},
	}
	for _, op := range ops {
		t.Run(op.name+" after close fails with ErrClosed", func(t *testing.T) {
			r := newReader(t, &fakeSource{content: testData(10)}, 10)
			if err := r.Close(); err != nil {
				t.Fatalf("Close error = %v", err)
			}
			if err := op.call(r); !errors.Is(err, fs.ErrClosed) {
				t.Errorf("%s after Close error = %v, want fs.ErrClosed", op.name, err)
			}
		})
	}
}

func TestConcurrentClose(t *testing.T) {
	t.Run("during Read", func(t *testing.T) {
		src := &fakeSource{content: testData(100), entered: make(chan struct{}), release: make(chan struct{})}
		r, err := seekable.New(100, src.open)
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		readErr := make(chan error, 1)
		go func() {
			_, err := r.Read(make([]byte, 10))
			readErr <- err
		}()
		select {
		case <-src.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("Read never reached the stream")
		}
		closing := make(chan struct{})
		closeErr := make(chan error, 1)
		go func() {
			close(closing)
			closeErr <- r.Close()
		}()
		<-closing
		time.Sleep(10 * time.Millisecond) // let Close overlap the blocked Read
		close(src.release)

		timeout := time.After(5 * time.Second)
		for range 2 {
			select {
			case err := <-readErr:
				if err != nil && !errors.Is(err, fs.ErrClosed) {
					t.Errorf("in-flight Read error = %v, want nil or fs.ErrClosed", err)
				}
			case err := <-closeErr:
				if err != nil {
					t.Errorf("Close error = %v, want nil", err)
				}
			case <-timeout:
				t.Fatal("Read and Close did not both return within 5s")
			}
		}
	})
	t.Run("during Seek", func(t *testing.T) {
		r, err := seekable.New(100, (&fakeSource{content: testData(100)}).open)
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		seekErr := make(chan error, 1)
		go func() {
			for i := range 1000 {
				if _, err := r.Seek(int64(i%100), io.SeekStart); err != nil {
					seekErr <- err
					return
				}
			}
			seekErr <- nil
		}()
		if err := r.Close(); err != nil {
			t.Errorf("Close error = %v, want nil", err)
		}
		select {
		case err := <-seekErr:
			if err != nil && !errors.Is(err, fs.ErrClosed) {
				t.Errorf("concurrent Seek error = %v, want nil or fs.ErrClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Seek did not return within 5s")
		}
	})
}

func TestServeContent(t *testing.T) {
	data := testData(4096)
	type part struct {
		contentRange string
		body         []byte
	}
	// Counts follow net/http serveContent: no known extension sniffs 512 bytes then rewinds; a range seeks then copies.
	tests := []struct {
		name          string
		method        string
		file          string
		rangeHeader   string
		wantStatus    int
		wantHeader    map[string]string
		wantBody      []byte
		wantParts     []part
		wantOpens     int
		wantBytesRead int64
	}{
		{
			name:          "GET sniffs then reopens for the body",
			method:        http.MethodGet,
			file:          "blob",
			wantStatus:    http.StatusOK,
			wantHeader:    map[string]string{"Content-Length": "4096", "Accept-Ranges": "bytes"},
			wantBody:      data,
			wantOpens:     2,
			wantBytesRead: 512 + 4096,
		},
		{
			name:          "HEAD reads only the sniff block",
			method:        http.MethodHead,
			file:          "blob",
			wantStatus:    http.StatusOK,
			wantHeader:    map[string]string{"Content-Length": "4096"},
			wantOpens:     1,
			wantBytesRead: 512,
		},
		{
			name:          "single range discards up to its start",
			method:        http.MethodGet,
			file:          "x.json",
			rangeHeader:   "bytes=100-199",
			wantStatus:    http.StatusPartialContent,
			wantHeader:    map[string]string{"Content-Range": "bytes 100-199/4096"},
			wantBody:      data[100:200],
			wantOpens:     1,
			wantBytesRead: 200,
		},
		{
			name:        "out-of-order ranges reopen for the earlier range",
			method:      http.MethodGet,
			file:        "x.json",
			rangeHeader: "bytes=2000-2099,0-99",
			wantStatus:  http.StatusPartialContent,
			wantParts: []part{
				{contentRange: "bytes 2000-2099/4096", body: data[2000:2100]},
				{contentRange: "bytes 0-99/4096", body: data[:100]},
			},
			wantOpens:     2,
			wantBytesRead: 2100 + 100,
		},
		{
			name:          "suffix range discards up to its start",
			method:        http.MethodGet,
			file:          "x.json",
			rangeHeader:   "bytes=-100",
			wantStatus:    http.StatusPartialContent,
			wantHeader:    map[string]string{"Content-Range": "bytes 3996-4095/4096"},
			wantBody:      data[3996:],
			wantOpens:     1,
			wantBytesRead: 4096,
		},
		{
			name:          "unsatisfiable range reads nothing",
			method:        http.MethodGet,
			file:          "x.json",
			rangeHeader:   "bytes=4096-",
			wantStatus:    http.StatusRequestedRangeNotSatisfiable,
			wantHeader:    map[string]string{"Content-Range": "bytes */4096"},
			wantOpens:     1,
			wantBytesRead: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &fakeSource{content: data}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				rs, err := seekable.New(int64(len(data)), src.open)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				defer rs.Close()
				http.ServeContent(w, r, tt.file, time.Time{}, rs)
			}))
			defer srv.Close()

			req, err := http.NewRequest(tt.method, srv.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tt.rangeHeader != "" {
				req.Header.Set("Range", tt.rangeHeader)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			for k, want := range tt.wantHeader {
				if got := resp.Header.Get(k); got != want {
					t.Errorf("%s = %q, want %q", k, got, want)
				}
			}
			if tt.wantBody != nil && !bytes.Equal(body, tt.wantBody) {
				t.Errorf("body = %d bytes, want %d matching bytes", len(body), len(tt.wantBody))
			}
			if tt.wantParts != nil {
				ct := resp.Header.Get("Content-Type")
				mediaType, params, err := mime.ParseMediaType(ct)
				if err != nil || mediaType != "multipart/byteranges" {
					t.Fatalf("Content-Type = %q, want multipart/byteranges", ct)
				}
				mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
				for i, want := range tt.wantParts {
					p, err := mr.NextPart()
					if err != nil {
						t.Fatalf("part %d: %v", i, err)
					}
					if got := p.Header.Get("Content-Range"); got != want.contentRange {
						t.Errorf("part %d Content-Range = %q, want %q", i, got, want.contentRange)
					}
					if got, err := io.ReadAll(p); err != nil || !bytes.Equal(got, want.body) {
						t.Errorf("part %d body = %d bytes (error %v), want %d matching bytes", i, len(got), err, len(want.body))
					}
				}
				if _, err := mr.NextPart(); err != io.EOF {
					t.Errorf("NextPart after %d parts error = %v, want EOF", len(tt.wantParts), err)
				}
			}
			got := src.snapshot()
			if got.opens != tt.wantOpens {
				t.Errorf("source opens = %d, want %d", got.opens, tt.wantOpens)
			}
			if got.bytesRead != tt.wantBytesRead {
				t.Errorf("source bytes read = %d, want %d", got.bytesRead, tt.wantBytesRead)
			}
		})
	}
}
