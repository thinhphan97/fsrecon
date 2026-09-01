package fsrecon

import (
	"context"
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

// MemoryStore is a concurrency-safe in-memory SnapshotStore.
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]FileState
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
	s.entries[state.Path] = state
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.entries, path)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Walk(ctx context.Context, prefix string, fn func(FileState) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	paths := make([]string, 0, len(s.entries))
	for path := range s.entries {
		if pathHasPrefix(path, prefix) {
			paths = append(paths, path)
		}
	}
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
