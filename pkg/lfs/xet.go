package lfs

import (
	"fmt"
	"net/http"
	"strconv"
)

// SetObjectHeaders writes the hub metadata headers describing an LFS object.
func SetObjectHeaders(w http.ResponseWriter, oid string, size int64) {
	w.Header().Set("ETag", fmt.Sprintf("%q", oid))
	w.Header().Set("X-Linked-Etag", fmt.Sprintf("%q", oid))
	w.Header().Set("X-Linked-Size", strconv.FormatInt(size, 10))
}

// SetXETLinkHeaders steers xet-capable clients to the CAS reconstruction of an
// object: the xet-auth and xet-reconstruction-info Link pair plus X-Xet-Hash.
func SetXETLinkHeaders(w http.ResponseWriter, fileHash, casURL, tokenURL string) {
	w.Header().Add("Link", fmt.Sprintf("<%s>; rel=\"xet-auth\", <%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", tokenURL, casURL, fileHash))
	w.Header().Set("X-Xet-Hash", fileHash)
}
