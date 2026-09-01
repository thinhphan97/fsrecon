package fsrecon

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIntegrityScrubComparesExpectedFingerprint(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "object")
	expectedContent := []byte("expected")
	if err := os.WriteFile(path, []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	fingerprint := sha256.Sum256(expectedContent)
	provider := expectedProviderFunc(func(ctx context.Context, root string, emit func(ExpectedEntry) error) error {
		return emit(ExpectedEntry{Path: "object", Fingerprint: fingerprint[:]})
	})
	tracker, err := New(Config{
		Root: root, Recursive: true, Expected: provider, Integrity: SHA256Checker{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Scrub(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Scanned != 1 || report.Corrupt != 1 || len(report.Events) != 1 || report.Events[0].Source != SourceIntegrity {
		t.Fatalf("report = %+v", report)
	}
	if tracker.Stats().CorruptDetected != 1 {
		t.Fatalf("stats = %+v", tracker.Stats())
	}
}

type blockingChecker struct {
	started chan struct{}
	release chan struct{}
}

func (c blockingChecker) Check(ctx context.Context, state FileState) (IntegrityResult, error) {
	close(c.started)
	select {
	case <-c.release:
		return IntegrityResult{Valid: false}, nil
	case <-ctx.Done():
		return IntegrityResult{}, ctx.Err()
	}
}

func TestCloseWaitsForActiveIntegrityScrub(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	checker := blockingChecker{started: make(chan struct{}), release: make(chan struct{})}
	tracker, err := New(Config{Root: root, Recursive: true, Integrity: checker})
	if err != nil {
		t.Fatal(err)
	}
	scrubDone := make(chan error, 1)
	go func() {
		_, err := tracker.Scrub(context.Background())
		scrubDone <- err
	}()
	<-checker.started
	closeDone := make(chan error, 1)
	go func() { closeDone <- tracker.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before scrub completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(checker.release)
	if err := <-scrubDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestIntegrityScrubRequiresChecker(t *testing.T) {
	tracker, err := New(Config{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	if _, err := tracker.Scrub(context.Background()); !errors.Is(err, ErrNoIntegrity) {
		t.Fatalf("Scrub() error = %v", err)
	}
}

type alwaysCorruptChecker struct{}

func (alwaysCorruptChecker) Check(context.Context, FileState) (IntegrityResult, error) {
	return IntegrityResult{Valid: false}, nil
}

func TestIntegrityReportIsBounded(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 5; i++ {
		if err := os.WriteFile(filepath.Join(root, string(rune('a'+i))), []byte("bad"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	tracker, err := New(Config{
		Root: root, Recursive: true, Integrity: alwaysCorruptChecker{}, ReportEventLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Scrub(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Corrupt != 5 || len(report.Events) != 2 || report.EventsTruncated != 3 {
		t.Fatalf("report = %+v", report)
	}
	if tracker.Stats().ReportEventsTruncated != 3 {
		t.Fatalf("stats = %+v", tracker.Stats())
	}
}

func TestScrubDeliversAllCorruptEventsToChangeSink(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 5; i++ {
		if err := os.WriteFile(filepath.Join(root, string(rune('a'+i))), []byte("bad"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sink := &recordingChangeSink{}
	tracker, err := New(Config{Root: root, Integrity: alwaysCorruptChecker{}, ChangeSink: sink, ReportEventLimit: 2, ChangeBatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Scrub(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Corrupt != 5 || len(report.Events) != 2 || report.EventsTruncated != 3 {
		t.Fatalf("report=%+v", report)
	}
	if sink.eventCount() != 5 {
		t.Fatalf("sink events=%d", sink.eventCount())
	}
	for _, batch := range sink.snapshotBatches() {
		if len(batch.Events) > 2 || batch.SessionID != tracker.sessionID || batch.Generation != report.Generation {
			t.Fatalf("batch=%+v", batch)
		}
		for _, event := range batch.Events {
			if event.Kind != EventCorrupt || event.Source != SourceIntegrity {
				t.Fatalf("event=%+v", event)
			}
		}
	}
}

func TestScrubChangeSinkFailureDoesNotAdvanceGeneration(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file")
	if err := os.WriteFile(path, []byte("bad"), 0o644); err != nil {
		t.Fatal(err)
	}
	sink := &recordingChangeSink{}
	tracker, err := New(Config{Root: root, Integrity: alwaysCorruptChecker{}, ChangeSink: sink})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, ok, err := tracker.store.Get(context.Background(), path)
	if err != nil || !ok {
		t.Fatalf("snapshot=%v %v", before, err)
	}
	sink.mu.Lock()
	sink.fail = errors.New("sink unavailable")
	sink.mu.Unlock()
	if _, err := tracker.Scrub(context.Background()); err == nil {
		t.Fatal("expected sink failure")
	}
	after, ok, err := tracker.store.Get(context.Background(), path)
	if err != nil || !ok || after != before {
		t.Fatalf("snapshot changed: before=%+v after=%+v", before, after)
	}
}
