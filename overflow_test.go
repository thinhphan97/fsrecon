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

type fakeNativeBackend struct {
	events chan internalbackend.RawEvent
	errors chan error
	once   sync.Once
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
	tracker, err := New(Config{Root: root, Recursive: true, EventBuffer: 1})
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
}

func TestReconcileReportEventLimitBoundsLargeDiff(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 20; i++ {
		path := filepath.Join(root, string(rune('a'+i)))
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	tracker, err := New(Config{
		Root: root, Recursive: true, EventBuffer: 64, ReportEventLimit: 5,
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
	if tracker.Stats().EventsDropped != 15 {
		t.Fatalf("stats = %+v", tracker.Stats())
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
	b.once.Do(func() {
		close(b.events)
		close(b.errors)
	})
	return nil
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
}
