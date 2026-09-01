// Package watchtree manages recursive native watch registrations.
package watchtree

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type Backend interface {
	Add(path string) error
	Remove(path string) error
}

type Tree struct {
	mu      sync.Mutex
	backend Backend
	root    string
	watched map[string]struct{}
}

// New records root as already registered by the backend Start operation.
func New(root string, backend Backend) *Tree {
	root = filepath.Clean(root)
	return &Tree{backend: backend, root: root, watched: map[string]struct{}{root: {}}}
}

// Add registers path once. Call it before scanning that directory's children.
func (t *Tree) Add(path string) error {
	path = filepath.Clean(path)
	if !within(t.root, path) {
		return fmt.Errorf("watch path %q is outside root %q", path, t.root)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.watched[path]; ok {
		return nil
	}
	if err := t.backend.Add(path); err != nil {
		return err
	}
	t.watched[path] = struct{}{}
	return nil
}

// Sync removes registrations below scopes that were not observed in a scan.
// Root is never removed.
func (t *Tree) Sync(scopes []string, observed map[string]struct{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var remove []string
	for path := range t.watched {
		if path == t.root || !inAnyScope(path, scopes) {
			continue
		}
		if _, ok := observed[path]; !ok {
			remove = append(remove, path)
		}
	}
	// Remove children first so platforms do not retain orphan descendants.
	sort.Slice(remove, func(i, j int) bool { return len(remove[i]) > len(remove[j]) })
	for _, path := range remove {
		if err := t.backend.Remove(path); err != nil {
			return err
		}
		delete(t.watched, path)
	}
	return nil
}

func (t *Tree) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.watched)
}

// IsWatched reports whether path has an active registration.
func (t *Tree) IsWatched(path string) bool {
	path = filepath.Clean(path)
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.watched[path]
	return ok
}

// RemoveSubtree removes a path and all descendant registrations.
func (t *Tree) RemoveSubtree(path string) error {
	path = filepath.Clean(path)
	t.mu.Lock()
	defer t.mu.Unlock()
	var remove []string
	for watched := range t.watched {
		if watched != t.root && within(path, watched) {
			remove = append(remove, watched)
		}
	}
	sort.Slice(remove, func(i, j int) bool { return len(remove[i]) > len(remove[j]) })
	for _, watched := range remove {
		if err := t.backend.Remove(watched); err != nil && !isAlreadyGone(err) {
			return err
		}
		delete(t.watched, watched)
	}
	return nil
}

func isAlreadyGone(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "invalid argument"))
}

func inAnyScope(path string, scopes []string) bool {
	for _, scope := range scopes {
		if within(filepath.Clean(scope), path) {
			return true
		}
	}
	return false
}

func within(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !startsWithParent(relative)
}

func startsWithParent(path string) bool {
	return len(path) > 3 && path[:3] == ".."+string(filepath.Separator)
}
