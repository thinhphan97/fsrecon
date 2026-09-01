package dirtyset

import (
	"path/filepath"
	"testing"
)

func TestSetCollapsesHierarchy(t *testing.T) {
	root := filepath.Clean(filepath.Join("root", "data"))
	set := New(root)
	set.Add(filepath.Join(root, "a", "b"))
	set.Add(filepath.Join(root, "a", "b", "c"))
	set.Add(filepath.Join(root, "a"))
	set.Add(filepath.Join(root, "x"))
	paths := set.Drain()
	if len(paths) != 2 || paths[0] != filepath.Join(root, "a") || paths[1] != filepath.Join(root, "x") {
		t.Fatalf("paths = %v", paths)
	}
}

func TestSetOutsidePathFallsBackToRoot(t *testing.T) {
	root := filepath.Clean(filepath.Join("root", "data"))
	set := New(root)
	set.Add(filepath.Join("root", "database"))
	paths := set.Drain()
	if len(paths) != 1 || paths[0] != root {
		t.Fatalf("paths = %v", paths)
	}
}
