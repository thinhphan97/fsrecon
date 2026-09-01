package fsnotify

import (
	"context"
	"os"
	"path/filepath"
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
