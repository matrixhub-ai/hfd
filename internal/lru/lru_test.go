package lru

import "testing"

// TestEvictionPastCapacity fills the cache beyond MaxEntries; eviction runs
// inside Add/GetOrNew while the lock is held and must not self-deadlock.
func TestEvictionPastCapacity(t *testing.T) {
	c := New[int, int](2)
	for i := 0; i < 5; i++ {
		c.Add(i, i)
	}
	if _, ok := c.Get(0); ok {
		t.Fatal("oldest entry should have been evicted")
	}
	if v, ok := c.Get(4); !ok || v != 4 {
		t.Fatalf("newest entry missing: %v, %v", v, ok)
	}

	c = New[int, int](2)
	for i := 0; i < 5; i++ {
		c.GetOrNew(i, func() (int, bool) { return i, true })
	}
	if _, ok := c.Get(0); ok {
		t.Fatal("oldest entry should have been evicted")
	}
	if v, ok := c.Get(4); !ok || v != 4 {
		t.Fatalf("newest entry missing: %v, %v", v, ok)
	}
}
