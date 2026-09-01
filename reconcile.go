package fsrecon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	internalreconcile "github.com/thinhphan97/fsrecon/internal/reconcile"
	internalscanner "github.com/thinhphan97/fsrecon/internal/scanner"
)

// Reconcile scans actual state, compares it with snapshot and expected state,
// updates the snapshot, and publishes semantic events.
func (t *Tracker) Reconcile(ctx context.Context) (ReconcileReport, error) {
	if ctx == nil {
		return ReconcileReport{}, errors.New("fsrecon: nil context")
	}
	t.reconcileMu.Lock()
	defer t.reconcileMu.Unlock()
	t.mu.RLock()
	closed := t.closed
	t.mu.RUnlock()
	if closed {
		return ReconcileReport{}, ErrClosed
	}
	t.setState(StateReconciling)
	report := ReconcileReport{StartedAt: time.Now()}
	fail := func(err error) (ReconcileReport, error) {
		report.Duration = time.Since(report.StartedAt)
		t.setState(StateDirty)
		return report, err
	}

	previous := make(map[string]FileState)
	if err := t.store.Walk(ctx, t.root, func(state FileState) error {
		previous[state.Path] = state
		return nil
	}); err != nil {
		return fail(fmt.Errorf("fsrecon: walk snapshot: %w", err))
	}

	actual := make(map[string]FileState)
	policyEvents := make([]Event, 0)
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
		if state.Type == FileTypeRegular && entry.Links > 1 {
			switch t.config.HardlinkPolicy {
			case RejectHardlinks:
				return fmt.Errorf("%w: %s", ErrHardlink, entry.Path)
			case ReportHardlinks:
				after := state
				policyEvents = append(policyEvents, Event{Kind: EventInvalid, Path: state.Path, After: &after, Source: SourceReconcile, Time: time.Now()})
			}
		}
		actual[state.Path] = state
		report.Scanned++
		return nil
	})
	if err != nil {
		if errors.Is(err, internalscanner.ErrSymlink) {
			err = fmt.Errorf("%w: %v", ErrSymlink, err)
		}
		return fail(fmt.Errorf("fsrecon: scan %q: %w", t.root, err))
	}

	expected, err := t.loadExpected(ctx)
	if err != nil {
		return fail(err)
	}
	changes := internalreconcile.Diff(toInternalStates(previous), toInternalStates(actual), expected)
	report.Events = make([]Event, 0, len(changes)+len(policyEvents))
	for _, change := range changes {
		event := eventFromChange(change)
		report.Events = append(report.Events, event)
		countEvent(&report, event.Kind)
	}
	for _, event := range policyEvents {
		report.Events = append(report.Events, event)
		countEvent(&report, event.Kind)
	}

	for path := range previous {
		if _, ok := actual[path]; !ok {
			if err := t.store.Delete(ctx, path); err != nil {
				return fail(fmt.Errorf("fsrecon: delete snapshot %q: %w", path, err))
			}
		}
	}
	for _, state := range actual {
		if err := t.store.Put(ctx, state); err != nil {
			return fail(fmt.Errorf("fsrecon: put snapshot %q: %w", state.Path, err))
		}
	}

	if expected != nil {
		for path, entry := range expected {
			state, ok := actual[path]
			if ok && (entry.Size == nil || state.Size == *entry.Size) {
				report.Healthy++
			}
		}
	} else {
		report.Healthy = uint64(len(actual)) - report.Created - report.Modified - report.Replaced
	}
	report.Duration = time.Since(report.StartedAt)
	t.stats.reconciliations.Add(1)
	t.stats.filesScanned.Add(report.Scanned)
	t.stats.missingDetected.Add(report.Missing)
	t.stats.orphansDetected.Add(report.Orphan)
	t.setState(StateSynced)
	for _, event := range report.Events {
		t.sendEvent(event)
	}
	return report, nil
}

func (t *Tracker) loadExpected(ctx context.Context) (map[string]internalreconcile.Expected, error) {
	if t.config.Expected == nil {
		return nil, nil
	}
	expected := make(map[string]internalreconcile.Expected)
	err := t.config.Expected.WalkExpected(ctx, t.root, func(entry ExpectedEntry) error {
		path, err := t.expectedPath(entry.Path)
		if err != nil {
			return err
		}
		if t.config.Filter != nil && !t.config.Filter(path, FileState{Path: path}) {
			return nil
		}
		expected[path] = internalreconcile.Expected{Path: path, Size: entry.Size}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fsrecon: walk expected state: %w", err)
	}
	return expected, nil
}

func (t *Tracker) expectedPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("fsrecon: expected path is empty")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(t.root, path)
	}
	path = filepath.Clean(path)
	rel, err := filepath.Rel(t.root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("fsrecon: expected path %q is outside root", path)
	}
	return path, nil
}

func toInternalStates(states map[string]FileState) map[string]internalreconcile.State {
	result := make(map[string]internalreconcile.State, len(states))
	for path, state := range states {
		result[path] = internalreconcile.State{
			Path: path, ID: state.ID.value, Type: uint8(state.Type), Size: state.Size,
			ModUnix: state.ModTime.UnixNano(), Mode: uint32(state.Mode),
		}
	}
	return result
}

func eventFromChange(change internalreconcile.Change) Event {
	var before, after *FileState
	if change.Before != nil {
		state := fromInternalState(*change.Before)
		before = &state
	}
	if change.After != nil {
		state := fromInternalState(*change.After)
		after = &state
	}
	return Event{
		Kind: EventKind(change.Kind), Path: change.Path, OldPath: change.OldPath,
		Before: before, After: after, Source: SourceReconcile, Time: time.Now(),
	}
}

func fromInternalState(state internalreconcile.State) FileState {
	return FileState{
		Path: state.Path, ID: newFileID(state.ID), Type: FileType(state.Type), Size: state.Size,
		ModTime: time.Unix(0, state.ModUnix), Mode: fs.FileMode(state.Mode),
	}
}

func countEvent(report *ReconcileReport, kind EventKind) {
	switch kind {
	case EventCreated:
		report.Created++
	case EventModified, EventAttributeChanged:
		report.Modified++
	case EventDeleted:
		report.Deleted++
	case EventMoved:
		report.Moved++
	case EventReplaced:
		report.Replaced++
	case EventMissing:
		report.Missing++
	case EventOrphan:
		report.Orphan++
	case EventInvalid:
		report.Invalid++
	case EventCorrupt:
		report.Corrupt++
	}
}
