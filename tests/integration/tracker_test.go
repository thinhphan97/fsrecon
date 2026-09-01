package integration_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thinhphan97/fsrecon"
)

func TestRecursiveNativeWatchAndAtomicReplace(t *testing.T) {
	root := t.TempDir()
	tracker, err := fsrecon.New(fsrecon.Config{
		Root: root, Recursive: true, DebounceWindow: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	dir := filepath.Join(root, "nested")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, tracker, fsrecon.EventCreated, dir)
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, tracker, fsrecon.EventCreated, file)

	temporary, err := os.CreateTemp(filepath.Dir(root), "fsrecon-replacement-")
	if err != nil {
		t.Fatal(err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.WriteString("replacement"); err != nil {
		t.Fatal(err)
	}
	if err := temporary.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporaryPath, file); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, tracker, fsrecon.EventReplaced, file)
}

func TestMoveDirectoryIntoAndOutsideRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(parent, "outside")
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(outside, "file")
	if err := os.WriteFile(file, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	tracker, err := fsrecon.New(fsrecon.Config{
		Root: root, Recursive: true, DebounceWindow: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	inside := filepath.Join(root, "moved")
	if err := os.Rename(outside, inside); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, tracker, fsrecon.EventCreated, inside)
	waitEvent(t, tracker, fsrecon.EventCreated, filepath.Join(inside, "file"))
	outsideAgain := filepath.Join(parent, "outside-again")
	if err := os.Rename(inside, outsideAgain); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, tracker, fsrecon.EventDeleted, inside)
}

func TestSymlinkAndHardlinkPolicies(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	tracker, err := fsrecon.New(fsrecon.Config{
		Root: root, Recursive: true, SymlinkPolicy: fsrecon.ReportSymlinks,
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = tracker.Close()
	if !containsEvent(report.Events, fsrecon.EventCreated, link) {
		t.Fatalf("symlink not reported: %+v", report.Events)
	}

	hardlink := filepath.Join(root, "hardlink")
	if err := os.Link(target, hardlink); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}
	reject, err := fsrecon.New(fsrecon.Config{
		Root: root, Recursive: true, HardlinkPolicy: fsrecon.RejectHardlinks,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reject.Close()
	if _, err := reject.Reconcile(context.Background()); !errors.Is(err, fsrecon.ErrHardlink) {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

func TestFilterIsAppliedToNativeReconciliation(t *testing.T) {
	root := t.TempDir()
	tracker, err := fsrecon.New(fsrecon.Config{
		Root: root, Recursive: true, DebounceWindow: 25 * time.Millisecond,
		Filter: func(path string, _ fsrecon.FileState) bool {
			return !strings.HasSuffix(path, ".tmp")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	ignored := filepath.Join(root, "ignored.tmp")
	if err := os.WriteFile(ignored, []byte("ignored"), 0o644); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(300 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case event := <-tracker.Events():
			if event.Path == ignored {
				t.Fatalf("filtered event emitted: %+v", event)
			}
		case err := <-tracker.Errors():
			t.Fatalf("tracker error: %v", err)
		case <-timer.C:
			return
		}
	}
}

func waitEvent(t *testing.T, tracker *fsrecon.Tracker, kind fsrecon.EventKind, path string) fsrecon.Event {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-tracker.Events():
			if !ok {
				t.Fatal("event channel closed")
			}
			if event.Kind == kind && event.Path == path {
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

func containsEvent(events []fsrecon.Event, kind fsrecon.EventKind, path string) bool {
	for _, event := range events {
		if event.Kind == kind && event.Path == path {
			return true
		}
	}
	return false
}
