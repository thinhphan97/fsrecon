package watchtree

import (
	"path/filepath"
	"testing"
)

type fakeBackend struct {
	added   []string
	removed []string
}

func (f *fakeBackend) Add(path string) error {
	f.added = append(f.added, path)
	return nil
}

func (f *fakeBackend) Remove(path string) error {
	f.removed = append(f.removed, path)
	return nil
}

func TestTreeAddsOnceAndRemovesMissingSubtree(t *testing.T) {
	root := filepath.Clean(filepath.Join("root", "data"))
	a := filepath.Join(root, "a")
	b := filepath.Join(a, "b")
	backend := &fakeBackend{}
	tree := New(root, backend)
	if err := tree.Add(a); err != nil {
		t.Fatal(err)
	}
	if err := tree.Add(a); err != nil {
		t.Fatal(err)
	}
	if err := tree.Add(b); err != nil {
		t.Fatal(err)
	}
	if len(backend.added) != 2 {
		t.Fatalf("added = %v", backend.added)
	}
	if err := tree.Sync([]string{root}, map[string]struct{}{a: {}}); err != nil {
		t.Fatal(err)
	}
	if len(backend.removed) != 1 || backend.removed[0] != b {
		t.Fatalf("removed = %v", backend.removed)
	}
}

func TestTreeRejectsPathOutsideRoot(t *testing.T) {
	root := filepath.Clean(filepath.Join("root", "data"))
	if err := New(root, &fakeBackend{}).Add(filepath.Join("root", "database")); err == nil {
		t.Fatal("expected outside-root error")
	}
}
