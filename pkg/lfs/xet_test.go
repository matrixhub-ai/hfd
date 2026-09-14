package lfs

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSetXETLinkHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()
	oid := strings.Repeat("ab", 32)
	fileHash := strings.Repeat("0123456789abcdef", 4)
	SetObjectHeaders(recorder, oid, 5)
	SetXETLinkHeaders(recorder, fileHash, "http://cas.example", "http://hub.example/api/models/org/repo/xet-read-token/main")
	for key, want := range map[string]string{
		"ETag":          `"` + oid + `"`,
		"X-Linked-Etag": `"` + oid + `"`,
		"X-Linked-Size": "5",
		"Link":          `<http://hub.example/api/models/org/repo/xet-read-token/main>; rel="xet-auth", <http://cas.example/v1/reconstructions/` + fileHash + `>; rel="xet-reconstruction-info"`,
		"X-Xet-Hash":    fileHash,
	} {
		if got := recorder.Header().Get(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
}
