package xetstore

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/gc"
)

// The pin lives in the test: pkg/gc's tests import this package, so it cannot import pkg/gc itself.
var _ gc.LFSStore = (*Store)(nil)

// fakeStore serves fixed shards and sha256 index entries and records every index delete; other calls hit the nil Storage.
type fakeStore struct {
	xetstorage.Storage
	shards              map[string]time.Time // shard hash -> mtime
	index               [][2]string          // sha256 hex and shard hash, in walk order
	shardsErr, indexErr error
	deleted             []string
	removed             bool
	deleteErr           error
}

func (s *fakeStore) WalkShards(_ context.Context, fn func(string, int64, time.Time) error) error {
	if s.shardsErr != nil {
		return s.shardsErr
	}
	for hash, modTime := range s.shards {
		if err := fn(hash, 0, modTime); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeStore) WalkSHA256Index(_ context.Context, fn func(string, string) error) error {
	if s.indexErr != nil {
		return s.indexErr
	}
	for _, e := range s.index {
		if err := fn(e[0], e[1]); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeStore) DeleteSHA256IndexEntry(_ context.Context, sha256Hex string) (bool, error) {
	s.deleted = append(s.deleted, sha256Hex)
	return s.removed, s.deleteErr
}

// object is one Walk callback.
type object struct {
	oid      string
	storedAt time.Time
}

// walk collects every Walk callback.
func walk(s *Store) ([]object, error) {
	var got []object
	err := s.Walk(context.Background(), func(oid string, storedAt time.Time) error {
		got = append(got, object{oid, storedAt})
		return nil
	})
	return got, err
}

func hex64(digit string) string { return strings.Repeat(digit, 64) }

func TestWalkObjects(t *testing.T) {
	mtime := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	t.Run("JoinsShardMtimes", func(t *testing.T) {
		store := &fakeStore{
			shards: map[string]time.Time{"a": mtime, "b": mtime.Add(time.Hour)},
			index:  [][2]string{{hex64("1"), "a"}, {hex64("2"), "a"}, {hex64("3"), "missing"}},
		}
		got, err := walk(New(store))
		want := []object{{hex64("1"), mtime}, {hex64("2"), mtime}, {hex64("3"), time.Time{}}}
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("Walk = %v, %v; want %v", got, err, want)
		}
	})
	t.Run("SkipsZeroMarker", func(t *testing.T) {
		store := &fakeStore{
			shards: map[string]time.Time{"a": mtime},
			index:  [][2]string{{hex64("0"), "a"}, {hex64("1"), "a"}},
		}
		got, err := walk(New(store))
		if want := []object{{hex64("1"), mtime}}; err != nil || !slices.Equal(got, want) {
			t.Fatalf("Walk = %v, %v; want %v", got, err, want)
		}
	})
	t.Run("CallbackErrorVerbatim", func(t *testing.T) {
		injected := errors.New("injected callback failure")
		store := &fakeStore{index: [][2]string{{hex64("1"), "a"}, {hex64("2"), "a"}, {hex64("3"), "a"}}}
		calls := 0
		err := New(store).Walk(context.Background(), func(string, time.Time) error {
			calls++
			if calls == 2 {
				return injected
			}
			return nil
		})
		if err != injected || calls != 2 {
			t.Fatalf("Walk = %v after %d calls; want the callback's error itself after 2", err, calls)
		}
	})
	t.Run("WrapsStoreErrors", func(t *testing.T) {
		injected := errors.New("injected walk failure")
		for prefix, store := range map[string]*fakeStore{
			"walk shards: ":       {shardsErr: injected, index: [][2]string{{hex64("1"), "a"}}},
			"walk sha256 index: ": {indexErr: injected},
		} {
			got, err := walk(New(store))
			if !errors.Is(err, injected) || !strings.HasPrefix(err.Error(), prefix) || len(got) != 0 {
				t.Errorf("Walk = %v, %v; want no object and the injected error behind %q", got, err, prefix)
			}
		}
	})
}

func TestUnlink(t *testing.T) {
	ctx := context.Background()
	t.Run("PassesThrough", func(t *testing.T) {
		// Mixed case shows the OID reaches the store as is, not re-encoded.
		oid := strings.Repeat("aB", 32)
		injected := errors.New("injected delete failure")
		for _, tc := range []struct {
			removed bool
			err     error
		}{{true, nil}, {false, nil}, {false, injected}} {
			store := &fakeStore{removed: tc.removed, deleteErr: tc.err}
			removed, err := New(store).Unlink(ctx, oid)
			if removed != tc.removed || err != tc.err || !slices.Equal(store.deleted, []string{oid}) {
				t.Errorf("Unlink = %t, %v; deleted %v; want %t, %v and %s deleted", removed, err, store.deleted, tc.removed, tc.err, oid)
			}
		}
	})
	t.Run("RejectsZeroMarker", func(t *testing.T) {
		store := &fakeStore{removed: true}
		if removed, err := New(store).Unlink(ctx, hex64("0")); err == nil || removed || len(store.deleted) != 0 {
			t.Fatalf("Unlink(zero) = %t, %v; deleted %v; want an error and no delete", removed, err, store.deleted)
		}
	})
}
