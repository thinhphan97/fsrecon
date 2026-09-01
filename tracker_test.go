package fsrecon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type expectedProviderFunc func(context.Context, string, func(ExpectedEntry) error) error

func (fn expectedProviderFunc) WalkExpected(ctx context.Context, root string, emit func(ExpectedEntry) error) error {
	return fn(ctx, root, emit)
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
