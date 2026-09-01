package fsrecon

import (
	"context"
	"encoding/binary"
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
	workChanges    = []byte("changes")
)

func (t *Tracker) reconcileBoltLocked(ctx context.Context, store *BoltStore, report ReconcileReport) (ReconcileReport, error) {
	fail := func(err error) (ReconcileReport, error) {
		report.Duration = time.Since(report.StartedAt)
		t.markDirty()
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
		for _, name := range [][]byte{workPrevious, workActual, workPreviousID, workActualID, workExpected, workMatched, workChanges} {
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
		changes := tx.Bucket(workChanges)
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
					if err := t.stageReportEvent(&report, changes, Event{
						Kind: EventInvalid, Path: state.Path, After: &after,
						Source: SourceReconcile, Time: time.Now(),
					}); err != nil {
						return err
					}
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
		rootExpectedAllowed, err := t.expectedRootCanBeExpected(ctx)
		if err != nil {
			return fail(fmt.Errorf("fsrecon: inspect expected root: %w", err))
		}
		if err := index.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(workExpected)
			return t.config.Expected.WalkExpected(ctx, t.root, func(entry ExpectedEntry) error {
				path, err := t.expectedPath(entry.Path)
				if err != nil {
					return err
				}
				if path == t.root && entry.Type == nil && !rootExpectedAllowed {
					return nil
				}
				if t.config.Filter != nil {
					filterState := FileState{Path: path}
					if entry.Type != nil {
						filterState.Type = *entry.Type
					} else if t.config.ExpectedScope == ExpectedRegularFiles {
						filterState.Type = FileTypeRegular
					}
					if !t.config.Filter(path, filterState) {
						return nil
					}
				}
				entry.Path = path
				if entry.Type == nil && t.config.ExpectedScope == ExpectedRegularFiles {
					regular := FileTypeRegular
					entry.Type = &regular
				}
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
		changes := tx.Bucket(workChanges)
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
					if err := t.stageReportEvent(&report, changes, semanticEvent(EventReplaced, after.Path, "", &before, &after)); err != nil {
						return err
					}
				case fileContentMetadataChanged(before, after):
					if err := t.stageReportEvent(&report, changes, semanticEvent(EventModified, after.Path, "", &before, &after)); err != nil {
						return err
					}
				case before.Mode != after.Mode:
					if err := t.stageReportEvent(&report, changes, semanticEvent(EventAttributeChanged, after.Path, "", &before, &after)); err != nil {
						return err
					}
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
					if err := t.stageReportEvent(&report, changes, semanticEvent(EventMoved, after.Path, before.Path, &before, &after)); err != nil {
						return err
					}
					continue
				}
			}
			if err := t.stageReportEvent(&report, changes, semanticEvent(EventCreated, after.Path, "", nil, &after)); err != nil {
				return err
			}
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
			if err := t.stageReportEvent(&report, changes, semanticEvent(EventDeleted, before.Path, "", &before, nil)); err != nil {
				return err
			}
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
					if err := t.stageReportEvent(&report, changes, semanticEvent(EventMissing, entry.Path, "", nil, nil)); err != nil {
						return err
					}
					continue
				}
				var state FileState
				if err := decodeFileState(actualValue, &state); err != nil {
					return err
				}
				if entry.Type != nil && state.Type != *entry.Type || entry.Size != nil && state.Size != *entry.Size {
					if err := t.stageReportEvent(&report, changes, semanticEvent(EventInvalid, state.Path, "", nil, &state)); err != nil {
						return err
					}
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
				if t.config.ExpectedScope == ExpectedRegularFiles && state.Type != FileTypeRegular {
					continue
				}
				if err := t.stageReportEvent(&report, changes, semanticEvent(EventOrphan, state.Path, "", nil, &state)); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return fail(fmt.Errorf("fsrecon: diff persistent snapshot: %w", err))
	}
	if !expectedEnabled {
		report.Healthy = report.Scanned - report.Created - report.Modified - report.Replaced
	}
	delivery := newChangeBatcher(ctx, t.config.ChangeSink, t.sessionID, report.Generation, t.config.ChangeBatchSize)
	if err := index.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(workChanges).Cursor()
		for _, value := cursor.First(); value != nil; _, value = cursor.Next() {
			var event Event
			if err := decodeStagedEvent(value, &event); err != nil {
				return err
			}
			if err := delivery.Add(event); err != nil {
				return err
			}
		}
		return delivery.Finish()
	}); err != nil {
		return fail(err)
	}
	if err := store.replaceSnapshot(ctx, index, workActual); err != nil {
		return fail(fmt.Errorf("fsrecon: replace persistent snapshot: %w", err))
	}
	report.Duration = time.Since(report.StartedAt)
	t.stats.reconciliations.Add(1)
	t.stats.filesScanned.Add(report.Scanned)
	t.stats.missingDetected.Add(report.Missing)
	t.stats.orphansDetected.Add(report.Orphan)
	t.stats.reportEventsTruncated.Add(report.EventsTruncated)
	t.generation.Store(report.Generation)
	t.setConsistentState()
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

func (t *Tracker) stageReportEvent(report *ReconcileReport, bucket *bolt.Bucket, event Event) error {
	countEvent(report, event.Kind)
	t.addReportEvent(report, event)
	sequence, err := bucket.NextSequence()
	if err != nil {
		return err
	}
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], sequence)
	value, err := encodeStagedEvent(event)
	if err != nil {
		return err
	}
	return bucket.Put(key[:], value)
}

type stagedEvent struct {
	Kind    EventKind        `json:"kind"`
	Path    string           `json:"path"`
	OldPath string           `json:"old_path,omitempty"`
	Before  *storedFileState `json:"before,omitempty"`
	After   *storedFileState `json:"after,omitempty"`
	Source  EventSource      `json:"source"`
	Time    time.Time        `json:"time"`
}

func encodeStagedEvent(event Event) ([]byte, error) {
	return json.Marshal(stagedEvent{
		Kind: event.Kind, Path: event.Path, OldPath: event.OldPath,
		Before: storedState(event.Before), After: storedState(event.After),
		Source: event.Source, Time: event.Time,
	})
}

func decodeStagedEvent(value []byte, event *Event) error {
	var stored stagedEvent
	if err := json.Unmarshal(value, &stored); err != nil {
		return err
	}
	*event = eventFromStaged(stored)
	return nil
}

func eventFromStaged(stored stagedEvent) Event {
	return Event{Kind: stored.Kind, Path: stored.Path, OldPath: stored.OldPath,
		Before: fileStateFromStored(stored.Before), After: fileStateFromStored(stored.After),
		Source: stored.Source, Time: stored.Time}
}

func storedState(state *FileState) *storedFileState {
	if state == nil {
		return nil
	}
	return &storedFileState{
		Path: state.Path, ID: state.ID.value, Type: state.Type, Size: state.Size,
		ModTime: state.ModTime, Mode: uint32(state.Mode), Schema: 1,
	}
}

func fileStateFromStored(stored *storedFileState) *FileState {
	if stored == nil {
		return nil
	}
	return &FileState{
		Path: stored.Path, ID: newFileID(stored.ID), Type: stored.Type, Size: stored.Size,
		ModTime: stored.ModTime, Mode: fs.FileMode(stored.Mode),
	}
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
