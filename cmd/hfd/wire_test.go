package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestBuildXETLayout pins the tree under <data>/xet to the one xetd writes under its -storage root.
func TestBuildXETLayout(t *testing.T) {
	cfg := &config{DataDir: t.TempDir(), PullMirrorURL: "https://hub.invalid"}
	st, err := buildStorage(t.Context(), cfg)
	if err != nil {
		t.Fatalf("buildStorage: %v", err)
	}
	sp, err := buildXETSpool(st)
	if err != nil {
		t.Fatalf("buildXETSpool: %v", err)
	}
	if _, err := buildXETMirror(cfg, st, buildXETCache(cfg, st), sp); err != nil {
		t.Fatalf("buildXETMirror: %v", err)
	}
	xetDir := filepath.Join(cfg.DataDir, "xet")
	for _, dir := range []string{"xorbs", "shards", "index", "spool", filepath.Join("mirror", "index")} {
		fi, err := os.Stat(filepath.Join(xetDir, dir))
		if err != nil || !fi.IsDir() {
			t.Errorf("stat xet/%s: %v, want directory", dir, err)
		}
	}
	for _, dir := range []string{"storage", "chunks", filepath.Join("mirror", "spool")} {
		if _, err := os.Stat(filepath.Join(xetDir, dir)); err == nil {
			t.Errorf("xet/%s exists, want the xetd layout without it", dir)
		}
	}
}
