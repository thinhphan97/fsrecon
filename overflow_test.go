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

	internalbackend "github.com/thinhphan97/fsrecon/internal/backend"
)

type fakeNativeBackend struct {
	events     chan internalbackend.RawEvent
	errors     chan error
	eventsOnce sync.Once
	errorsOnce sync.Once
}

type recordingChangeSink struct {
	mu      sync.Mutex
	batches []ChangeBatch
	fail    error
}

func (s *recordingChangeSink) ApplyChanges(_ context.Context, batch ChangeBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	batch.Events = append([]Event(nil), batch.Events...)
	s.batches = append(s.batches, batch)
	return nil
}

func (s *recordingChangeSink) eventCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, batch := range s.batches {
		count += len(batch.Events)
	}
	return count
}

func (s *recordingChangeSink) snapshotBatches() []ChangeBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ChangeBatch(nil), s.batches...)
}

func TestEventStormCoalescesToSingleDirtyScope(t *testing.T) {
	backend := newFakeNativeBackend()
	root := t.TempDir()
	tracker, err := New(Config{
		Root: root, Recursive: true, EventBuffer: 16, DebounceWindow: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	tracker.newBackend = func(uint) internalbackend.Backend { return backend }
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	path := filepath.Join(root, "storm")
	if err := os.WriteFile(path, []byte("final state"), 0o644); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		backend.events <- internalbackend.RawEvent{Path: path, Op: internalbackend.OpWrite}
	}
	waitForEvent(t, tracker, EventCreated, path, "")
	stats := tracker.Stats()
	if stats.EventsReceived != 100 || stats.EventsCoalesced < 99 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestBoundedPublicQueueDropsWithoutBlockingReconcile(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 20; i++ {
		path := filepath.Join(root, string(rune('a'+i)))
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sink := &recordingChangeSink{}
	tracker, err := New(Config{Root: root, Recursive: true, EventBuffer: 1, ChangeSink: sink, ChangeBatchSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Created != 20 || tracker.Stats().EventsDropped != 19 || tracker.Stats().QueueDepth != 1 {
		t.Fatalf("report=%+v stats=%+v", report, tracker.Stats())
	}
	if sink.eventCount() != 20 {
		t.Fatalf("authoritative sink received %d events, want 20", sink.eventCount())
	}
	batches := sink.snapshotBatches()
	for i, batch := range batches {
		if len(batch.Events) > 3 || batch.Sequence != uint64(i) || batch.Generation != report.Generation {
			t.Fatalf("batch %d = %+v", i, batch)
		}
		if batch.Final != (i == len(batches)-1) {
			t.Fatalf("batch %d Final=%v", i, batch.Final)
		}
	}
}

func TestReconcileReportEventLimitBoundsLargeDiff(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 20; i++ {
		path := filepath.Join(root, string(rune('a'+i)))
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sink := &recordingChangeSink{}
	tracker, err := New(Config{
		Root: root, Recursive: true, EventBuffer: 64, ReportEventLimit: 5,
		ChangeSink: sink, ChangeBatchSize: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Created != 20 || len(report.Events) != 5 || report.EventsTruncated != 15 {
		t.Fatalf("report = %+v", report)
	}
	if tracker.Stats().ReportEventsTruncated != 15 || tracker.Stats().EventsDropped != 0 {
		t.Fatalf("stats = %+v", tracker.Stats())
	}
	if sink.eventCount() != 20 {
		t.Fatalf("authoritative sink received %d events, want 20", sink.eventCount())
	}
	for _, batch := range sink.snapshotBatches() {
		if len(batch.Events) > 4 {
			t.Fatalf("unbounded delivery batch: %d events", len(batch.Events))
		}
	}
}

func TestChangeSinkFailureDoesNotAdvanceSnapshot(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	sink := &recordingChangeSink{}
	store := NewMemoryStore()
	tracker, err := New(Config{Root: root, Recursive: true, Store: store, ChangeSink: sink})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, ok, err := store.Get(context.Background(), path)
	if err != nil || !ok {
		t.Fatalf("baseline snapshot = (%+v, %v, %v)", before, ok, err)
	}
	if err := os.WriteFile(path, []byte("new-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	sink.fail = errors.New("sink unavailable")
	sink.mu.Unlock()
	if _, err := tracker.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile() succeeded despite sink failure")
	}
	if tracker.State() != StateDirty {
		t.Fatalf("state = %v, want DIRTY", tracker.State())
	}
	afterFailure, ok, err := store.Get(context.Background(), path)
	if err != nil || !ok || afterFailure.Size != before.Size {
		t.Fatalf("snapshot advanced after failed delivery: before=%+v after=%+v ok=%v err=%v", before, afterFailure, ok, err)
	}
	sink.mu.Lock()
	sink.fail = nil
	sink.mu.Unlock()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Modified != 1 || report.Generation != 2 || tracker.State() != StateSynced {
		t.Fatalf("recovery report=%+v state=%v", report, tracker.State())
	}
}

func newFakeNativeBackend() *fakeNativeBackend {
	return &fakeNativeBackend{
		events: make(chan internalbackend.RawEvent, 8),
		errors: make(chan error, 8),
	}
}

func (b *fakeNativeBackend) Start(context.Context, string) error { return nil }
func (b *fakeNativeBackend) Add(string) error                    { return nil }
func (b *fakeNativeBackend) Remove(string) error                 { return nil }
func (b *fakeNativeBackend) Events() <-chan internalbackend.RawEvent {
	return b.events
}
func (b *fakeNativeBackend) Errors() <-chan error { return b.errors }
func (b *fakeNativeBackend) Close() error {
	b.eventsOnce.Do(func() { close(b.events) })
	b.errorsOnce.Do(func() { close(b.errors) })
	return nil
}

func (b *fakeNativeBackend) stopEvents() {
	b.eventsOnce.Do(func() { close(b.events) })
}

type blockingWalkStore struct {
	SnapshotStore
	mu      sync.Mutex
	block   bool
	started chan struct{}
	release chan struct{}
}

func (s *blockingWalkStore) Walk(ctx context.Context, prefix string, fn func(FileState) error) error {
	s.mu.Lock()
	block := s.block
	started := s.started
	release := s.release
	s.mu.Unlock()
	if block {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.SnapshotStore.Walk(ctx, prefix, fn)
}

func TestOverflowEmitsRecoveryEventsAndReconciles(t *testing.T) {
	backend := newFakeNativeBackend()
	tracker, err := New(Config{Root: t.TempDir(), Recursive: true, EventBuffer: 16})
	if err != nil {
		t.Fatal(err)
	}
	tracker.newBackend = func(uint) internalbackend.Backend { return backend }
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	before := tracker.Stats().Reconciliations
	backend.errors <- internalbackend.ErrOverflow

	want := map[EventKind]bool{EventOverflow: false, EventRescanRequired: false}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for !want[EventOverflow] || !want[EventRescanRequired] {
		select {
		case event := <-tracker.Events():
			if _, ok := want[event.Kind]; ok {
				want[event.Kind] = true
			}
		case err := <-tracker.Errors():
			if !errors.Is(err, internalbackend.ErrOverflow) {
				t.Fatalf("unexpected error: %v", err)
			}
		case <-deadline.C:
			t.Fatalf("timed out; events = %v", want)
		}
	}
	reconcileDeadline := time.NewTimer(5 * time.Second)
	poll := time.NewTicker(10 * time.Millisecond)
	defer reconcileDeadline.Stop()
	defer poll.Stop()
	for tracker.Stats().Reconciliations <= before {
		select {
		case <-poll.C:
		case <-reconcileDeadline.C:
			t.Fatalf("reconciliation did not run: before=%d after=%d", before, tracker.Stats().Reconciliations)
		}
	}
	if tracker.State() != StateSynced {
		t.Fatalf("state = %v", tracker.State())
	}
	if tracker.Stats().BackendOverflows != 1 {
		t.Fatalf("stats = %+v, want one backend overflow", tracker.Stats())
	}
}

func TestClosedBackendRemainsDegradedAfterReconciliation(t *testing.T) {
	backend := newFakeNativeBackend()
	tracker, err := New(Config{Root: t.TempDir(), Recursive: true, ReconcileInterval: 0})
	if err != nil {
		t.Fatal(err)
	}
	tracker.newBackend = func(uint) internalbackend.Backend { return backend }
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	before := tracker.Stats().Reconciliations
	backend.stopEvents()
	waitForCondition(t, func() bool {
		return tracker.State() == StateDegraded && tracker.Stats().Reconciliations > before
	}, "backend closure reconciliation did not remain degraded")
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tracker.State() != StateDegraded {
		t.Fatalf("state after manual reconciliation = %v, want DEGRADED", tracker.State())
	}
}

func TestNativeCollectorDrainsWhileReconciliationIsBlocked(t *testing.T) {
	root := t.TempDir()
	backend := newFakeNativeBackend()
	store := &blockingWalkStore{
		SnapshotStore: NewMemoryStore(),
		started:       make(chan struct{}, 1),
		release:       make(chan struct{}),
	}
	tracker, err := New(Config{
		Root: root, Recursive: true, Store: store, DebounceWindow: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	tracker.newBackend = func(uint) internalbackend.Backend { return backend }
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	store.mu.Lock()
	store.block = true
	store.mu.Unlock()
	baseline := tracker.Stats().Reconciliations
	reconcileDone := make(chan error, 1)
	go func() {
		_, err := tracker.Reconcile(context.Background())
		reconcileDone <- err
	}()
	select {
	case <-store.started:
	case <-time.After(2 * time.Second):
		t.Fatal("reconciliation did not block in store walk")
	}
	sent := make(chan struct{})
	go func() {
		for i := 0; i < 20; i++ {
			backend.events <- internalbackend.RawEvent{
				Path: filepath.Join(root, fmt.Sprintf("event-%02d", i)),
				Op:   internalbackend.OpWrite,
			}
		}
		close(sent)
	}()
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("native collector stopped draining while reconciliation was blocked")
	}
	close(store.release)
	if err := <-reconcileDone; err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, func() bool {
		return tracker.Stats().EventsReceived == 20 && tracker.Stats().Reconciliations >= baseline+2
	}, "dirty scopes collected during reconciliation were not processed")
}

func waitForCondition(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal(message)
		}
	}
}
