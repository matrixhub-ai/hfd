package internalapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6/util"

	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/gc/xetstore"
)

// TestHandlerUsageSharedObjects pins the SharedObjects section: the loose objects of the store the repositories share, which their own section leaves out.
func TestHandlerUsageSharedObjects(t *testing.T) {
	ctx := context.Background()
	t.Run("ReportsStore", func(t *testing.T) {
		f := newFixture(t)
		f.commitPointer(t, "org/a", f.put(t, "usage-s "))
		g := gc.NewGC(f.st.RepositoriesFS(), xetstore.New(f.xs))
		shared, err := g.SharedObjectsUsage(ctx)
		if err != nil || shared.Count != 3 || shared.Bytes <= 0 {
			t.Fatalf("shared objects usage = %+v, %v; want the commit, tree and pointer blob", shared, err)
		}
		git, err := g.Usage(ctx)
		if err != nil || git.Objects.Count != 0 || git.Other.Count == 0 {
			t.Fatalf("repository usage = %+v, %v; want metadata only", git, err)
		}
		xet, err := f.xs.Usage(ctx)
		if err != nil || xet.Xorbs.Count == 0 {
			t.Fatalf("xet usage = %+v, %v; want the stored object", xet, err)
		}
		rec := do(f.handler(), http.MethodGet, "/internal/usage")
		want := usageResponse{Repository: git, SharedObjects: shared, Xet: xet}
		var got usageResponse
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &got) != nil || got != want {
			t.Fatalf("usage %d %s; want %+v", rec.Code, rec.Body, want)
		}
		if key := fmt.Sprintf(`"SharedObjects":{"Count":3,"Bytes":%d}`, shared.Bytes); !strings.Contains(rec.Body.String(), key) {
			t.Fatalf("usage body %s: missing %s", rec.Body, key)
		}
	})
	t.Run("WalkError", func(t *testing.T) {
		f := newFixture(t)
		// A regular file where the store's objects directory belongs fails ReadDir with an error other than ErrNotExist.
		if err := util.WriteFile(f.st.FS(), "/git/sha1/objects", []byte("not a directory"), 0o644); err != nil {
			t.Fatal(err)
		}
		rec := do(f.handler(), http.MethodGet, "/internal/usage")
		if rec.Code != http.StatusInternalServerError || !strings.HasPrefix(rec.Body.String(), "Usage failed: shared objects usage: ") {
			t.Fatalf("usage %d %q; want 500 with the shared objects usage error", rec.Code, rec.Body)
		}
	})
}
