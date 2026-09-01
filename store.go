package fsrecon

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// SnapshotStore persists the last successfully observed state.
type SnapshotStore interface {
	Get(ctx context.Context, path string) (FileState, bool, error)
	Put(ctx context.Context, state FileState) error
	Delete(ctx context.Context, path string) error
	Walk(ctx context.Context, prefix string, fn func(FileState) error) error
}

// ScopedSnapshotStore optionally provides an optimized subtree traversal.
// Tracker falls back to SnapshotStore.Walk when it is not implemented.
type ScopedSnapshotStore interface {
	SnapshotStore
	WalkScope(ctx context.Context, scope string, fn func(FileState) error) error
}

// BatchSnapshotStore atomically applies one reconciliation when supported.
// Tracker falls back to individual SnapshotStore operations otherwise.
type BatchSnapshotStore interface {
	SnapshotStore
	Apply(ctx context.Context, puts []FileState, deletes []string) error
}

// MemoryStore is a concurrency-safe in-memory SnapshotStore.
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]FileState
	paths   memoryPathNode
}

type memoryPathNode struct {
	terminal bool
	path     string
	children map[string]*memoryPathNode
}

// NewMemoryStore constructs an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string]FileState)}
}

func (s *MemoryStore) Get(ctx context.Context, path string) (FileState, bool, error) {
	if err := ctx.Err(); err != nil {
		return FileState{}, false, err
	}
	s.mu.RLock()
	state, ok := s.entries[path]
	s.mu.RUnlock()
	return state, ok, nil
}

func (s *MemoryStore) Put(ctx context.Context, state FileState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.ensureInitialized()
	if _, exists := s.entries[state.Path]; !exists {
		s.paths.insert(pathParts(state.Path), state.Path)
	}
	s.entries[state.Path] = state
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.ensureInitialized()
	delete(s.entries, path)
	s.paths.remove(pathParts(path))
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Walk(ctx context.Context, prefix string, fn func(FileState) error) error {
	return s.WalkScope(ctx, prefix, fn)
}

// WalkScope visits only the indexed subtree rooted at scope.
func (s *MemoryStore) WalkScope(ctx context.Context, scope string, fn func(FileState) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	node := s.paths.find(pathParts(scope))
	if node == nil {
		s.mu.RUnlock()
		return nil
	}
	paths := make([]string, 0)
	node.collect(pathParts(scope), &paths)
	sort.Strings(paths)
	states := make([]FileState, len(paths))
	for i, path := range paths {
		states[i] = s.entries[path]
	}
	s.mu.RUnlock()
	for _, state := range states {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(state); err != nil {
			return err
		}
	}
	return nil
}

func (s *MemoryStore) Apply(ctx context.Context, puts []FileState, deletes []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureInitialized()
	for _, path := range deletes {
		delete(s.entries, path)
		s.paths.remove(pathParts(path))
	}
	for _, state := range puts {
		if _, exists := s.entries[state.Path]; !exists {
			s.paths.insert(pathParts(state.Path), state.Path)
		}
		s.entries[state.Path] = state
	}
	return nil
}

func (s *MemoryStore) ensureInitialized() {
	if s.entries == nil {
		s.entries = make(map[string]FileState)
	}
}

func pathParts(path string) []string {
	path = filepath.Clean(path)
	volume := filepath.VolumeName(path)
	rest := strings.TrimPrefix(path, volume)
	anchor := volume + "."
	if filepath.IsAbs(path) {
		anchor = volume + string(filepath.Separator)
	}
	parts := []string{anchor}
	for _, part := range strings.Split(rest, string(filepath.Separator)) {
		if part != "" && part != "." {
			parts = append(parts, part)
		}
	}
	return parts
}

func (n *memoryPathNode) insert(parts []string, path string) {
	current := n
	for _, part := range parts {
		if current.children == nil {
			current.children = make(map[string]*memoryPathNode)
		}
		child := current.children[part]
		if child == nil {
			child = &memoryPathNode{}
			current.children[part] = child
		}
		current = child
	}
	current.terminal = true
	current.path = path
}

func (n *memoryPathNode) find(parts []string) *memoryPathNode {
	current := n
	for _, part := range parts {
		current = current.children[part]
		if current == nil {
			return nil
		}
	}
	return current
}

func (n *memoryPathNode) remove(parts []string) {
	removeMemoryPath(n, parts, 0)
}

func removeMemoryPath(node *memoryPathNode, parts []string, index int) bool {
	if index == len(parts) {
		node.terminal = false
		node.path = ""
		return len(node.children) == 0
	}
	child := node.children[parts[index]]
	if child == nil {
		return false
	}
	if removeMemoryPath(child, parts, index+1) {
		delete(node.children, parts[index])
	}
	return !node.terminal && len(node.children) == 0
}

func (n *memoryPathNode) collect(parts []string, paths *[]string) {
	if n.terminal {
		*paths = append(*paths, n.path)
	}
	for name, child := range n.children {
		child.collect(append(parts, name), paths)
	}
}

func pathHasPrefix(path, prefix string) bool {
	if prefix == "" || path == prefix {
		return true
	}
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	return strings.HasSuffix(prefix, "/") || strings.HasSuffix(prefix, `\`) ||
		len(path) > len(prefix) && (path[len(prefix)] == '/' || path[len(prefix)] == '\\')
}
