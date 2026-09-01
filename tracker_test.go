package fsrecon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	internalreconcile "github.com/thinhphan97/fsrecon/internal/reconcile"
)

type expectedProviderFunc func(context.Context, string, func(ExpectedEntry) error) error

func (fn expectedProviderFunc) WalkExpected(ctx context.Context, root string, emit func(ExpectedEntry) error) error {
	return fn(ctx, root, emit)
}

type countingScopedExpectedProvider struct {
	mu          sync.Mutex
	entries     []ExpectedEntry
	fullCalls   int
	scopedCalls int
	emitted     int
}

func (p *countingScopedExpectedProvider) WalkExpected(_ context.Context, _ string, emit func(ExpectedEntry) error) error {
	p.mu.Lock()
	p.fullCalls++
	entries := append([]ExpectedEntry(nil), p.entries...)
	p.mu.Unlock()
	for _, entry := range entries {
		if err := emit(entry); err != nil {
			return err
		}
	}
	return nil
}

func (p *countingScopedExpectedProvider) WalkExpectedScope(_ context.Context, root, scope string, emit func(ExpectedEntry) error) error {
	p.mu.Lock()
	p.scopedCalls++
	entries := append([]ExpectedEntry(nil), p.entries...)
	p.mu.Unlock()
	for _, entry := range entries {
		path := entry.Path
		if !filepath.IsAbs(path) {
			path = filepath.Join(root, path)
		}
		if !pathHasPrefix(path, scope) {
			continue
		}
		p.mu.Lock()
		p.emitted++
		p.mu.Unlock()
		if err := emit(entry); err != nil {
			return err
		}
	}
	return nil
}

type countingScopedStore struct {
	*MemoryStore
	mu            sync.Mutex
	fullCalls     int
	scopedCalls   int
	unrelatedRoot string
	unrelatedSeen int
}

func (s *countingScopedStore) Walk(ctx context.Context, prefix string, fn func(FileState) error) error {
	s.mu.Lock()
	s.fullCalls++
	s.mu.Unlock()
	return s.MemoryStore.Walk(ctx, prefix, fn)
}

func (s *countingScopedStore) WalkScope(ctx context.Context, scope string, fn func(FileState) error) error {
	s.mu.Lock()
	s.scopedCalls++
	s.mu.Unlock()
	return s.MemoryStore.WalkScope(ctx, scope, func(state FileState) error {
		if pathHasPrefix(state.Path, s.unrelatedRoot) {
			s.mu.Lock()
			s.unrelatedSeen++
			s.mu.Unlock()
		}
		return fn(state)
	})
}

func TestTrackerManualReconcileAndIdentity(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	if err := os.WriteFile(a, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	tracker, err := New(Config{Root: root, Recursive: true, EventBuffer: 32})
	if err != nil {
		t.Fatal(err)
	}
	if tracker.State() != StateCreated {
		t.Fatalf("state = %v", tracker.State())
	}
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tracker.State() != StateSynced {
		t.Fatalf("state = %v", tracker.State())
	}

	b := filepath.Join(root, "b")
	if err := os.Rename(a, b); err != nil {
		t.Fatal(err)
	}
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Moved != 1 || report.Events[0].OldPath != a || report.Events[0].Path != b {
		t.Fatalf("rename report = %+v", report)
	}

	temporary := filepath.Join(root, "replacement")
	if err := os.WriteFile(temporary, []byte("two-two"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, b); err != nil {
		t.Fatal(err)
	}
	report, err = tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Replaced != 1 {
		t.Fatalf("replace report = %+v", report)
	}

	if err := os.Remove(b); err != nil {
		t.Fatal(err)
	}
	report, err = tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Deleted != 1 {
		t.Fatalf("delete report = %+v", report)
	}
	if err := tracker.Close(); err != nil {
		t.Fatal(err)
	}
	if tracker.State() != StateStopped {
		t.Fatalf("state = %v", tracker.State())
	}
	if _, err := tracker.Reconcile(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Reconcile() after close error = %v", err)
	}
}

func TestInternalEventKindMappingsAreExplicit(t *testing.T) {
	tests := []struct {
		internal internalreconcile.Kind
		public   EventKind
	}{
		{internalreconcile.Created, EventCreated},
		{internalreconcile.Modified, EventModified},
		{internalreconcile.Deleted, EventDeleted},
		{internalreconcile.Moved, EventMoved},
		{internalreconcile.AttributeChanged, EventAttributeChanged},
		{internalreconcile.Missing, EventMissing},
		{internalreconcile.Orphan, EventOrphan},
		{internalreconcile.Replaced, EventReplaced},
		{internalreconcile.Invalid, EventInvalid},
	}
	for _, test := range tests {
		got, err := eventKindFromInternal(test.internal)
		if err != nil || got != test.public {
			t.Fatalf("eventKindFromInternal(%d) = (%v, %v), want %v", test.internal, got, err, test.public)
		}
	}
	if _, err := eventKindFromInternal(internalreconcile.Kind(255)); err == nil {
		t.Fatal("unknown internal event kind did not return an error")
	}
}

func TestInternalFileTypeMappingsAreExplicit(t *testing.T) {
	tests := []struct {
		internal internalreconcile.FileType
		public   FileType
	}{
		{internalreconcile.TypeUnknown, FileTypeUnknown},
		{internalreconcile.TypeRegular, FileTypeRegular},
		{internalreconcile.TypeDirectory, FileTypeDirectory},
		{internalreconcile.TypeSymlink, FileTypeSymlink},
		{internalreconcile.TypeOther, FileTypeOther},
	}
	for _, test := range tests {
		if got := fileTypeFromInternal(test.internal); got != test.public {
			t.Fatalf("fileTypeFromInternal(%d) = %v, want %v", test.internal, got, test.public)
		}
		if got := internalTypeFromPublic(test.public); got != test.internal {
			t.Fatalf("internalTypeFromPublic(%v) = %d, want %d", test.public, got, test.internal)
		}
	}
}

func TestTrackerDetectsNativeFilesystemEvents(t *testing.T) {
	root := t.TempDir()
	tracker, err := New(Config{Root: root, Recursive: true, EventBuffer: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	a := filepath.Join(root, "native-a")
	if err := os.WriteFile(a, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForEvent(t, tracker, EventCreated, a, "")

	if err := os.WriteFile(a, []byte("a longer value"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForEvent(t, tracker, EventModified, a, "")

	b := filepath.Join(root, "native-b")
	if err := os.Rename(a, b); err != nil {
		t.Fatal(err)
	}
	waitForEvent(t, tracker, EventMoved, b, a)

	if err := os.Remove(b); err != nil {
		t.Fatal(err)
	}
	waitForEvent(t, tracker, EventDeleted, b, "")

	if tracker.Stats().EventsReceived == 0 {
		t.Fatal("native backend did not record raw events")
	}
}

func TestTrackerWatchesExistingAndNewNestedDirectories(t *testing.T) {
	root := t.TempDir()
	existing := filepath.Join(root, "existing")
	if err := os.Mkdir(existing, 0o755); err != nil {
		t.Fatal(err)
	}
	tracker, err := New(Config{Root: root, Recursive: true, EventBuffer: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	// Drain the initial directory event.
	waitForEvent(t, tracker, EventCreated, existing, "")

	existingFile := filepath.Join(existing, "file.txt")
	if err := os.WriteFile(existingFile, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForEvent(t, tracker, EventCreated, existingFile, "")

	created := filepath.Join(root, "created")
	if err := os.Mkdir(created, 0o755); err != nil {
		t.Fatal(err)
	}
	waitForEvent(t, tracker, EventCreated, created, "")
	createdFile := filepath.Join(created, "nested.txt")
	if err := os.WriteFile(createdFile, []byte("nested"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForEvent(t, tracker, EventCreated, createdFile, "")
}

func TestPartialReconcileDetectsMoveAcrossDirtySubtrees(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	if err := os.MkdirAll(a, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(b, 0o755); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(a, "file")
	newPath := filepath.Join(b, "file")
	if err := os.WriteFile(oldPath, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	tracker, err := New(Config{Root: root, Recursive: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	report, err := tracker.reconcileScopes(context.Background(), []string{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if report.Moved != 1 || len(report.Events) != 1 || report.Events[0].OldPath != oldPath || report.Events[0].Path != newPath {
		t.Fatalf("report = %+v", report)
	}
}

func TestPartialReconcileScansOnlyDirtySubtree(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	if err := os.MkdirAll(a, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(b, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := os.WriteFile(filepath.Join(b, fmt.Sprintf("file-%02d", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	target := filepath.Join(a, "target")
	if err := os.WriteFile(target, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	tracker, err := New(Config{Root: root, Recursive: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("longer"), 0o644); err != nil {
		t.Fatal(err)
	}
	report, err := tracker.reconcileScopes(context.Background(), []string{a})
	if err != nil {
		t.Fatal(err)
	}
	if report.Scanned != 1 || report.Modified != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestPartialReconcileUsesScopedExpectedProvider(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	if err := os.MkdirAll(a, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(b, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(a, "file"), filepath.Join(b, "file")} {
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	provider := &countingScopedExpectedProvider{entries: []ExpectedEntry{
		{Path: filepath.Join("a", "file")}, {Path: filepath.Join("b", "file")},
	}}
	tracker, err := New(Config{Root: root, Recursive: true, Expected: provider})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	provider.fullCalls, provider.scopedCalls, provider.emitted = 0, 0, 0
	provider.mu.Unlock()
	if _, err := tracker.reconcileScopes(context.Background(), []string{a}); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.fullCalls != 0 || provider.scopedCalls != 1 || provider.emitted != 1 {
		t.Fatalf("provider calls: full=%d scoped=%d emitted=%d", provider.fullCalls, provider.scopedCalls, provider.emitted)
	}
}

func TestPartialReconcileUsesScopedSnapshotStore(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	if err := os.MkdirAll(a, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(b, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(a, "file"), filepath.Join(b, "file")} {
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	store := &countingScopedStore{MemoryStore: NewMemoryStore(), unrelatedRoot: b}
	tracker, err := New(Config{Root: root, Recursive: true, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.fullCalls, store.scopedCalls, store.unrelatedSeen = 0, 0, 0
	store.mu.Unlock()
	if _, err := tracker.reconcileScopes(context.Background(), []string{a}); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.fullCalls != 0 || store.scopedCalls != 1 || store.unrelatedSeen != 0 {
		t.Fatalf("store calls: full=%d scoped=%d unrelated=%d", store.fullCalls, store.scopedCalls, store.unrelatedSeen)
	}
}

func waitForEvent(t *testing.T, tracker *Tracker, kind EventKind, path, oldPath string) Event {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-tracker.Events():
			if !ok {
				t.Fatal("event channel closed")
			}
			if event.Kind == kind && event.Path == path && (oldPath == "" || event.OldPath == oldPath) {
				return event
			}
		case err, ok := <-tracker.Errors():
			if ok {
				t.Fatalf("tracker error: %v", err)
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for %s on %s", kind, path)
		}
	}
}

func TestTrackerExpectedMissingOrphanInvalid(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "actual"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	wrongSize := int64(99)
	provider := expectedProviderFunc(func(ctx context.Context, root string, emit func(ExpectedEntry) error) error {
		if err := emit(ExpectedEntry{Path: "missing"}); err != nil {
			return err
		}
		return emit(ExpectedEntry{Path: "actual", Size: &wrongSize})
	})
	tracker, err := New(Config{Root: root, Recursive: true, Expected: provider})
	if err != nil {
		t.Fatal(err)
	}
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Missing != 1 || report.Invalid != 1 || report.Orphan != 0 {
		t.Fatalf("report = %+v", report)
	}
	_ = tracker.Close()
}

func TestExpectedFileManifestDoesNotOrphanParentDirectories(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "objects")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "a.dat"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	provider := expectedProviderFunc(func(_ context.Context, _ string, emit func(ExpectedEntry) error) error {
		return emit(ExpectedEntry{Path: filepath.Join("objects", "a.dat")})
	})
	tracker, err := New(Config{Root: root, Recursive: true, Expected: provider})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Healthy != 1 || report.Orphan != 0 || report.Invalid != 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestExpectedTypeMismatchIsInvalid(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	provider := expectedProviderFunc(func(_ context.Context, _ string, emit func(ExpectedEntry) error) error {
		return emit(ExpectedEntry{Path: "a"}) // Default expected type is regular file.
	})
	tracker, err := New(Config{Root: root, Recursive: true, Expected: provider})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Invalid != 1 || report.Healthy != 0 || report.Orphan != 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestExpectedDirectoryCanBeExplicit(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "directory"), 0o755); err != nil {
		t.Fatal(err)
	}
	directoryType := FileTypeDirectory
	provider := expectedProviderFunc(func(_ context.Context, _ string, emit func(ExpectedEntry) error) error {
		return emit(ExpectedEntry{Path: "directory", Type: &directoryType})
	})
	tracker, err := New(Config{Root: root, Recursive: true, Expected: provider})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Healthy != 1 || report.Invalid != 0 || report.Orphan != 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestExpectedStateSupportsFileRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "file.dat")
	if err := os.WriteFile(root, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	typ := FileTypeRegular
	provider := expectedProviderFunc(func(_ context.Context, _ string, emit func(ExpectedEntry) error) error {
		return emit(ExpectedEntry{Path: root, Type: &typ, Size: ptrInt64(4)})
	})
	tracker, err := New(Config{Root: root, Expected: provider})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Healthy != 1 || report.Missing != 0 || report.Invalid != 0 {
		t.Fatalf("report=%+v", report)
	}
}

func TestExpectedStateFileRootMissing(t *testing.T) {
	root := filepath.Join(t.TempDir(), "file.dat")
	if err := os.WriteFile(root, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	typ := FileTypeRegular
	provider := expectedProviderFunc(func(_ context.Context, _ string, emit func(ExpectedEntry) error) error {
		return emit(ExpectedEntry{Path: root, Type: &typ})
	})
	tracker, err := New(Config{Root: root, Expected: provider})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Missing != 1 {
		t.Fatalf("report=%+v", report)
	}
}

func TestExpectedStateFileRootInvalid(t *testing.T) {
	root := filepath.Join(t.TempDir(), "file.dat")
	if err := os.WriteFile(root, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	want := int64(99)
	typ := FileTypeRegular
	provider := expectedProviderFunc(func(_ context.Context, _ string, emit func(ExpectedEntry) error) error {
		return emit(ExpectedEntry{Path: root, Type: &typ, Size: &want})
	})
	tracker, err := New(Config{Root: root, Expected: provider})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Invalid != 1 {
		t.Fatalf("report=%+v", report)
	}
}

func ptrInt64(v int64) *int64 { return &v }

func TestTrackerContextCancellationClosesChannels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tracker, err := New(Config{Root: t.TempDir(), Recursive: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-tracker.Events():
		if ok {
			for range tracker.Events() {
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("events channel did not close")
	}
	if tracker.State() != StateStopped {
		t.Fatalf("state = %v", tracker.State())
	}
}
