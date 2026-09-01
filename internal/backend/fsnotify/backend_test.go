package fsnotify

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/thinhphan97/fsrecon/internal/backend"
)

func TestBackendReceivesNativeEvent(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	b := New(16)
	if err := b.Start(ctx, root); err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	path := filepath.Join(root, "created.txt")
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event := <-b.Events():
			if event.Path == path && event.Op&(backend.OpCreate|backend.OpWrite) != 0 {
				cancel()
				return
			}
		case err := <-b.Errors():
			t.Fatalf("backend error: %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for native event")
		}
	}
}

func TestBackendCancellationClosesChannels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := New(1)
	if err := b.Start(ctx, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-b.Events():
		if ok {
			t.Fatal("event channel remained open")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event channel did not close")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveAlreadyDeletedDirectoryIsIdempotent(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "watched")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	b := New(16)
	if err := b.Start(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Add(directory); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(directory); err != nil {
		t.Fatal(err)
	}
	if err := b.Remove(directory); err != nil {
		t.Fatalf("Remove() after directory deletion = %v", err)
	}
}

func TestRemovedWatchErrorPlatformSemantics(t *testing.T) {
	if got, want := isRemovedWatchError(syscall.EINVAL), runtime.GOOS == "linux"; got != want {
		t.Fatalf("isRemovedWatchError(EINVAL) = %v, want %v", got, want)
	}
	if !isRemovedWatchError(os.ErrNotExist) {
		t.Fatal("isRemovedWatchError(os.ErrNotExist) = false")
	}
	if isRemovedWatchError(errors.New("backend failure")) {
		t.Fatal("isRemovedWatchError(unrelated error) = true")
	}
}
