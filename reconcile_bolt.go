package fsrecon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	internalscanner "github.com/thinhphan97/fsrecon/internal/scanner"
	bolt "go.etcd.io/bbolt"
)

var (
	workPrevious   = []byte("previous")
	workActual     = []byte("actual")
	workPreviousID = []byte("previous-id")
	workActualID   = []byte("actual-id")
	workExpected   = []byte("expected")
	workMatched    = []byte("matched-previous")
)

func (t *Tracker) reconcileBoltLocked(ctx context.Context, store *BoltStore, report ReconcileReport) (ReconcileReport, error) {
	fail := func(err error) (ReconcileReport, error) {
		report.Duration = time.Since(report.StartedAt)
		t.setState(StateDirty)
		return report, err
	}
	temporary, err := os.CreateTemp("", "fsrecon-reconcile-*.db")
	if err != nil {
		return fail(fmt.Errorf("fsrecon: create reconcile index: %w", err))
	}
	indexPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(indexPath)
		return fail(fmt.Errorf("fsrecon: close reconcile index file: %w", err))
	}
	defer os.Remove(indexPath)
	index, err := bolt.Open(indexPath, 0o600, nil)
	if err != nil {
		return fail(fmt.Errorf("fsrecon: open reconcile index: %w", err))
	}
	defer index.Close()
	if err := index.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{workPrevious, workActual, workPreviousID, workActualID, workExpected, workMatched} {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fail(fmt.Errorf("fsrecon: initialize reconcile index: %w", err))
	}

	if err := index.Update(func(tx *bolt.Tx) error {
		states := tx.Bucket(workPrevious)
		identities := tx.Bucket(workPreviousID)
		return store.Walk(ctx, t.root, func(state FileState) error {
			value, err := encodeFileState(state)
			if err != nil {
				return err
			}
			if err := states.Put([]byte(state.Path), value); err != nil {
				return err
			}
			return indexIdentity(identities, state.ID.value, state.Path)
		})
	}); err != nil {
		return fail(fmt.Errorf("fsrecon: stage previous snapshot: %w", err))
	}

	observedDirectories := map[string]struct{}{t.root: {}}
	if err := index.Update(func(tx *bolt.Tx) error {
		actual := tx.Bucket(workActual)
		identities := tx.Bucket(workActualID)
		s := internalscanner.Scanner{
			Recursive: t.config.Recursive, SymlinkPolicy: scannerPolicy(t.config.SymlinkPolicy),
		}
		if t.config.Filter != nil {
			s.Filter = func(entry internalscanner.Entry) bool {
				state := stateFromEntry(entry)
				return t.config.Filter(state.Path, state)
			}
		}
		err := s.Scan(ctx, t.root, func(entry internalscanner.Entry) error {
			state := stateFromEntry(entry)
			if state.Type == FileTypeDirectory && t.config.Recursive {
				t.mu.RLock()
				tree := t.watchTree
				t.mu.RUnlock()
				if tree != nil {
					if err := tree.Add(state.Path); err != nil {
						return err
					}
				}
				observedDirectories[state.Path] = struct{}{}
			}
			if state.Type == FileTypeRegular && entry.Links > 1 {
				switch t.config.HardlinkPolicy {
				case RejectHardlinks:
					return fmt.Errorf("%w: %s", ErrHardlink, state.Path)
				case ReportHardlinks:
					after := state
					t.recordReportEvent(&report, Event{
						Kind: EventInvalid, Path: state.Path, After: &after,
						Source: SourceReconcile, Time: time.Now(),
					})
				}
			}
			value, err := encodeFileState(state)
			if err != nil {
				return err
			}
			if err := actual.Put([]byte(state.Path), value); err != nil {
				return err
			}
			if err := indexIdentity(identities, state.ID.value, state.Path); err != nil {
				return err
			}
			report.Scanned++
			return nil
		})
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}); err != nil {
		return fail(fmt.Errorf("fsrecon: stage actual filesystem: %w", err))
	}

	if t.config.Recursive {
		t.mu.RLock()
		tree := t.watchTree
		t.mu.RUnlock()
		if tree != nil {
			if err := tree.Sync([]string{t.root}, observedDirectories); err != nil {
				return fail(fmt.Errorf("fsrecon: sync recursive watches: %w", err))
			}
		}
	}

	expectedEnabled := t.config.Expected != nil
	if expectedEnabled {
		if err := index.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(workExpected)
			return t.config.Expected.WalkExpected(ctx, t.root, func(entry ExpectedEntry) error {
				path, err := t.expectedPath(entry.Path)
				if err != nil {
					return err
				}
				if path == t.root || t.config.Filter != nil && !t.config.Filter(path, FileState{Path: path}) {
					return nil
				}
				entry.Path = path
				value, err := json.Marshal(entry)
				if err != nil {
					return err
				}
				return bucket.Put([]byte(path), value)
			})
		}); err != nil {
			return fail(fmt.Errorf("fsrecon: stage expected state: %w", err))
		}
	}

	if err := index.Update(func(tx *bolt.Tx) error {
		previous := tx.Bucket(workPrevious)
		actual := tx.Bucket(workActual)
		previousIDs := tx.Bucket(workPreviousID)
		actualIDs := tx.Bucket(workActualID)
		matched := tx.Bucket(workMatched)
		cursor := actual.Cursor()
		for path, value := cursor.First(); path != nil; path, value = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var after FileState
			if err := decodeFileState(value, &after); err != nil {
				return err
			}
			if beforeValue := previous.Get(path); beforeValue != nil {
				var before FileState
				if err := decodeFileState(beforeValue, &before); err != nil {
					return err
				}
				if err := matched.Put(path, []byte{1}); err != nil {
					return err
				}
				switch {
				case !sameFileIdentity(before, after):
					t.recordReportEvent(&report, semanticEvent(EventReplaced, after.Path, "", &before, &after))
				case fileContentMetadataChanged(before, after):
					t.recordReportEvent(&report, semanticEvent(EventModified, after.Path, "", &before, &after))
				case before.Mode != after.Mode:
					t.recordReportEvent(&report, semanticEvent(EventAttributeChanged, after.Path, "", &before, &after))
				}
				continue
			}
			oldPath := previousIDs.Get([]byte(after.ID.value))
			uniqueActual := actualIDs.Get([]byte(after.ID.value))
			if !after.ID.IsZero() && len(oldPath) > 0 && string(uniqueActual) == after.Path && matched.Get(oldPath) == nil {
				beforeValue := previous.Get(oldPath)
				var before FileState
				if beforeValue != nil {
					if err := decodeFileState(beforeValue, &before); err != nil {
						return err
					}
					if err := matched.Put(oldPath, []byte{1}); err != nil {
						return err
					}
					t.recordReportEvent(&report, semanticEvent(EventMoved, after.Path, before.Path, &before, &after))
					continue
				}
			}
			t.recordReportEvent(&report, semanticEvent(EventCreated, after.Path, "", nil, &after))
		}
		cursor = previous.Cursor()
		for path, value := cursor.First(); path != nil; path, value = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if matched.Get(path) != nil {
				continue
			}
			var before FileState
			if err := decodeFileState(value, &before); err != nil {
				return err
			}
			t.recordReportEvent(&report, semanticEvent(EventDeleted, before.Path, "", &before, nil))
		}
		if expectedEnabled {
			expected := tx.Bucket(workExpected)
			cursor = expected.Cursor()
			for path, value := cursor.First(); path != nil; path, value = cursor.Next() {
				var entry ExpectedEntry
				if err := json.Unmarshal(value, &entry); err != nil {
					return err
				}
				actualValue := actual.Get(path)
				if actualValue == nil {
					t.recordReportEvent(&report, semanticEvent(EventMissing, entry.Path, "", nil, nil))
					continue
				}
				var state FileState
				if err := decodeFileState(actualValue, &state); err != nil {
					return err
				}
				if entry.Size != nil && state.Size != *entry.Size {
					t.recordReportEvent(&report, semanticEvent(EventInvalid, state.Path, "", nil, &state))
				} else {
					report.Healthy++
				}
			}
			cursor = actual.Cursor()
			for path, value := cursor.First(); path != nil; path, value = cursor.Next() {
				if expected.Get(path) != nil {
					continue
				}
				var state FileState
				if err := decodeFileState(value, &state); err != nil {
					return err
				}
				t.recordReportEvent(&report, semanticEvent(EventOrphan, state.Path, "", nil, &state))
			}
		}
		return nil
	}); err != nil {
		return fail(fmt.Errorf("fsrecon: diff persistent snapshot: %w", err))
	}
	if !expectedEnabled {
		report.Healthy = report.Scanned - report.Created - report.Modified - report.Replaced
	}
	if err := store.replaceSnapshot(ctx, index, workActual); err != nil {
		return fail(fmt.Errorf("fsrecon: replace persistent snapshot: %w", err))
	}
	report.Duration = time.Since(report.StartedAt)
	t.stats.reconciliations.Add(1)
	t.stats.filesScanned.Add(report.Scanned)
	t.stats.missingDetected.Add(report.Missing)
	t.stats.orphansDetected.Add(report.Orphan)
	t.stats.eventsDropped.Add(report.EventsTruncated)
	t.setState(StateSynced)
	for _, event := range report.Events {
		t.sendEvent(event)
	}
	return report, nil
}

func indexIdentity(bucket *bolt.Bucket, identity, path string) error {
	if identity == "" {
		return nil
	}
	key := []byte(identity)
	value := bucket.Get(key)
	if value == nil {
		return bucket.Put(key, []byte(path))
	}
	if string(value) != path {
		return bucket.Put(key, []byte{})
	}
	return nil
}

func (t *Tracker) recordReportEvent(report *ReconcileReport, event Event) {
	countEvent(report, event.Kind)
	t.addReportEvent(report, event)
}

func semanticEvent(kind EventKind, path, oldPath string, before, after *FileState) Event {
	return Event{
		Kind: kind, Path: path, OldPath: oldPath, Before: before, After: after,
		Source: SourceReconcile, Time: time.Now(),
	}
}

func sameFileIdentity(before, after FileState) bool {
	return before.ID.IsZero() || after.ID.IsZero() || before.ID.Equal(after.ID)
}

func fileContentMetadataChanged(before, after FileState) bool {
	return before.Type != after.Type || before.Size != after.Size ||
		before.ModTime.UnixNano() != after.ModTime.UnixNano()
}
