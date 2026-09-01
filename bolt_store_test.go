package fsrecon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestBoltStorePersistsAndWalks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	store, err := OpenBoltStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	state := FileState{Path: filepath.Join("root", "a"), ID: newFileID("identity"), Size: 42, Mode: 0o640}
	if err := store.Put(ctx, state); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenBoltStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, ok, err := store.Get(ctx, state.Path)
	if err != nil || !ok || got.Path != state.Path || !got.ID.Equal(state.ID) || got.Size != state.Size || got.Mode != state.Mode {
		t.Fatalf("Get() = (%+v, %v, %v)", got, ok, err)
	}
	if err := store.Apply(ctx, []FileState{{Path: filepath.Join("root", "b")}}, []string{state.Path}); err != nil {
		t.Fatal(err)
	}
	var paths []string
	if err := store.Walk(ctx, "root", func(state FileState) error {
		paths = append(paths, state.Path)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != filepath.Join("root", "b") {
		t.Fatalf("paths = %v", paths)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.Put(canceled, state); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put() error = %v", err)
	}
}

func TestBoltStoreRestoresTrackerAcrossRestart(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("before"), 0o644); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(t.TempDir(), "snapshot.db")
	store, err := OpenBoltStore(database)
	if err != nil {
		t.Fatal(err)
	}
	tracker, err := New(Config{Root: root, Recursive: true, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = tracker.Close()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(file, []byte("after and longer"), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err = OpenBoltStore(database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	restarted, err := New(Config{Root: root, Recursive: true, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	report, err := restarted.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Modified != 1 || report.Created != 0 {
		t.Fatalf("restart report = %+v", report)
	}
}

func TestBoltStoreReconcilePreservesIdentityMoves(t *testing.T) {
	root := t.TempDir()
	oldPath := filepath.Join(root, "old")
	newPath := filepath.Join(root, "new")
	if err := os.WriteFile(oldPath, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := OpenBoltStore(filepath.Join(t.TempDir(), "snapshot.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tracker, err := New(Config{Root: root, Recursive: true, Store: store})
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
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Moved != 1 || len(report.Events) != 1 || report.Events[0].OldPath != oldPath || report.Events[0].Path != newPath {
		t.Fatalf("report = %+v", report)
	}
}
