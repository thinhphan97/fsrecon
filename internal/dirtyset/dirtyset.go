// Package dirtyset maintains a minimal set of filesystem subtrees to reconcile.
package dirtyset

import (
	"path/filepath"
	"sort"
	"strings"
)

type Set struct {
	root string
	tree node
	size int
}

type node struct {
	dirty    bool
	children map[string]*node
}

func New(root string) *Set {
	return &Set{root: filepath.Clean(root)}
}

// Add returns true when the minimal set changed.
func (s *Set) Add(path string) bool {
	path = filepath.Clean(path)
	if !within(s.root, path) {
		path = s.root
	}
	relative, err := filepath.Rel(s.root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		relative = "."
	}
	current := &s.tree
	if current.dirty {
		return false
	}
	if relative != "." {
		for _, part := range strings.Split(relative, string(filepath.Separator)) {
			if current.dirty {
				return false
			}
			if current.children == nil {
				current.children = make(map[string]*node)
			}
			child := current.children[part]
			if child == nil {
				child = &node{}
				current.children[part] = child
			}
			current = child
		}
	}
	if current.dirty {
		return false
	}
	removed := countDirty(current)
	current.dirty = true
	current.children = nil
	s.size += 1 - removed
	return true
}

func countDirty(current *node) int {
	if current == nil {
		return 0
	}
	count := 0
	if current.dirty {
		count++
	}
	for _, child := range current.children {
		count += countDirty(child)
	}
	return count
}

func (s *Set) Len() int { return s.size }

func (s *Set) Drain() []string {
	paths := make([]string, 0, s.size)
	collect(&s.tree, s.root, &paths)
	s.tree = node{}
	s.size = 0
	sort.Strings(paths)
	return paths
}

func collect(current *node, path string, paths *[]string) {
	if current.dirty {
		*paths = append(*paths, path)
		return
	}
	for name, child := range current.children {
		collect(child, filepath.Join(path, name), paths)
	}
}

func within(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." &&
		!(len(relative) > 3 && relative[:3] == ".."+string(filepath.Separator))
}
