package fsrecon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	internalbackend "github.com/thinhphan97/fsrecon/internal/backend"
)

type observerFakeBackend struct {
	events  chan internalbackend.RawEvent
	errs    chan error
	mu      sync.Mutex
	watched map[string]struct{}
	closed  bool
	addErr  error
}

func newObserverFakeBackend() *observerFakeBackend {
	return &observerFakeBackend{events: make(chan internalbackend.RawEvent, 64), errs: make(chan error, 16), watched: map[string]struct{}{}}
}
func (b *observerFakeBackend) Start(context.Context, string) error { return nil }
func (b *observerFakeBackend) Add(path string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.addErr != nil {
		return b.addErr
	}
	b.watched[filepath.Clean(path)] = struct{}{}
	return nil
}
func (b *observerFakeBackend) Remove(path string) error {
	b.mu.Lock()
	delete(b.watched, filepath.Clean(path))
	b.mu.Unlock()
	return nil
}
func (b *observerFakeBackend) Events() <-chan internalbackend.RawEvent { return b.events }
func (b *observerFakeBackend) Errors() <-chan error                    { return b.errs }
func (b *observerFakeBackend) Close() error {
	b.mu.Lock()
	if !b.closed {
		close(b.events)
		close(b.errs)
		b.closed = true
	}
	b.mu.Unlock()
	return nil
}
func (b *observerFakeBackend) inject(e internalbackend.RawEvent) { b.events <- e }
func waitObserverHint(t *testing.T, c <-chan Hint, want func(Hint) bool) Hint {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case h := <-c:
			if want(h) {
				return h
			}
		case <-timer.C:
			t.Fatal("timed out waiting for observer hint")
		}
	}
}

func TestObserverStartupPreservesHintCause(t *testing.T) {
	root := t.TempDir()
	o, _ := NewObserver(ObserverConfig{Root: root})
	b := newObserverFakeBackend()
	o.newBackend = func(uint) internalbackend.Backend { return b }
	if err := o.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	h := waitObserverHint(t, o.Hints(), func(h Hint) bool { return h.Cause == HintStartup })
	if h.Path != o.root || h.Scope != HintSubtree {
		t.Fatalf("hint=%+v", h)
	}
}
func TestObserverEmitsExactPathHint(t *testing.T) {
	root := t.TempDir()
	o, _ := NewObserver(ObserverConfig{Root: root, DebounceWindow: 10 * time.Millisecond})
	b := newObserverFakeBackend()
	o.newBackend = func(uint) internalbackend.Backend { return b }
	if err := o.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	_ = waitObserverHint(t, o.Hints(), func(h Hint) bool { return h.Cause == HintStartup })
	p := filepath.Join(root, "a")
	b.inject(internalbackend.RawEvent{Path: p, Op: internalbackend.OpWrite})
	h := waitObserverHint(t, o.Hints(), func(h Hint) bool { return h.Path == p })
	if h.Scope != HintPath {
		t.Fatalf("hint=%+v", h)
	}
}
func TestObserverPendingHintsEscalateToValidRootHint(t *testing.T) {
	root := t.TempDir()
	o, _ := NewObserver(ObserverConfig{Root: root, MaxPendingHints: 2, HintBuffer: 1, DebounceWindow: 10 * time.Millisecond})
	b := newObserverFakeBackend()
	o.newBackend = func(uint) internalbackend.Backend { return b }
	if err := o.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	_ = waitObserverHint(t, o.Hints(), func(h Hint) bool { return h.Cause == HintStartup })
	for i := 0; i < 4; i++ {
		b.inject(internalbackend.RawEvent{Path: filepath.Join(root, string(rune('a'+i))), Op: internalbackend.OpWrite})
	}
	time.Sleep(30 * time.Millisecond)
	h := waitObserverHint(t, o.Hints(), func(h Hint) bool { return h.Scope == HintSubtree })
	if h.Path != root || h.Cause != HintNativeChange {
		t.Fatalf("hint=%+v", h)
	}
}
func TestObserverDirectoryRemoveBypassesFilter(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "dir")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	o, _ := NewObserver(ObserverConfig{Root: root, Recursive: true, Filter: func(string) bool { return false }, DebounceWindow: 10 * time.Millisecond})
	b := newObserverFakeBackend()
	o.newBackend = func(uint) internalbackend.Backend { return b }
	if err := o.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	_ = waitObserverHint(t, o.Hints(), func(h Hint) bool { return h.Cause == HintStartup })
	b.inject(internalbackend.RawEvent{Path: dir, Op: internalbackend.OpRemove})
	h := waitObserverHint(t, o.Hints(), func(h Hint) bool { return h.Path == dir })
	if h.Scope != HintSubtree {
		t.Fatalf("hint=%+v", h)
	}
}
func TestObserverFollowSymlinksUnsupported(t *testing.T) {
	if _, err := NewObserver(ObserverConfig{Root: t.TempDir(), SymlinkPolicy: FollowSymlinks}); !errors.Is(err, ErrObserverFollowSymlinksUnsupported) {
		t.Fatalf("err=%v", err)
	}
}
