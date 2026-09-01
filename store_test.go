package fsrecon

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestMemoryStoreCRUDWalkAndCancellation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryStore()
	root := filepath.Join("root", "a")
	child := filepath.Join(root, "child")
	sibling := filepath.Join("root", "ab")
	for _, path := range []string{root, child, sibling} {
		if err := store.Put(ctx, FileState{Path: path, Size: 1}); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := store.Get(ctx, child)
	if err != nil || !ok || got.Path != child {
		t.Fatalf("Get() = (%v, %v, %v)", got, ok, err)
	}
	var walked []string
	if err := store.Walk(ctx, root, func(state FileState) error {
		walked = append(walked, state.Path)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(walked) != 2 || walked[0] != root || walked[1] != child {
		t.Fatalf("Walk() = %v", walked)
	}
	if err := store.Delete(ctx, child); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := store.Get(ctx, child); ok {
		t.Fatal("deleted entry still exists")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.Put(canceled, FileState{Path: "x"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put() error = %v", err)
	}
}

func TestMemoryStoreConcurrentAccess(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			path := filepath.Join("root", string(rune('a'+i)))
			_ = store.Put(context.Background(), FileState{Path: path, Size: int64(i)})
			_, _, _ = store.Get(context.Background(), path)
		}(i)
	}
	wg.Wait()
}
