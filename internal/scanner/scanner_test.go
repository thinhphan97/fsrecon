//go:build linux || darwin

package scanner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestScannerRecursiveAndSymlinkPolicies(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	nested := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nested, "file"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(nested, "file"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	var paths []string
	err := (Scanner{Recursive: true, SymlinkPolicy: IgnoreSymlinks}).Scan(context.Background(), root, func(entry Entry) error {
		paths = append(paths, entry.Path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 3 {
		t.Fatalf("got %d entries: %v", len(paths), paths)
	}
	err = (Scanner{Recursive: true, SymlinkPolicy: RejectSymlinks}).Scan(context.Background(), root, func(Entry) error { return nil })
	if !errors.Is(err, ErrSymlink) {
		t.Fatalf("reject error = %v", err)
	}
}

func TestScannerCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := (Scanner{Recursive: true}).Scan(ctx, t.TempDir(), func(Entry) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Scan() error = %v", err)
	}
}
