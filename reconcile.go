package fsrecon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	internalreconcile "github.com/thinhphan97/fsrecon/internal/reconcile"
	internalscanner "github.com/thinhphan97/fsrecon/internal/scanner"
)

// Reconcile scans actual state, compares it with snapshot and expected state,
// updates the snapshot, and publishes semantic events.
func (t *Tracker) Reconcile(ctx context.Context) (ReconcileReport, error) {
	return t.reconcileScopes(ctx, []string{t.root})
}

func (t *Tracker) reconcileScopes(ctx context.Context, requested []string) (ReconcileReport, error) {
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
	scopes := collapseScopes(t.root, requested)
	if len(scopes) == 0 {
		report.Duration = time.Since(report.StartedAt)
		t.setState(StateSynced)
		return report, nil
	}
	if store, ok := t.store.(*BoltStore); ok && len(scopes) == 1 && scopes[0] == t.root {
		return t.reconcileBoltLocked(ctx, store, report)
	}

	previous := make(map[string]FileState)
	for _, scope := range scopes {
		if err := t.store.Walk(ctx, scope, func(state FileState) error {
			previous[state.Path] = state
			return nil
		}); err != nil {
			return fail(fmt.Errorf("fsrecon: walk snapshot %q: %w", scope, err))
		}
	}

	actual := make(map[string]FileState)
	policyEvents := make([]Event, 0)
	observedDirectories := map[string]struct{}{t.root: {}}
	for _, scope := range scopes {
		info, statErr := os.Lstat(scope)
		if statErr != nil {
			if errors.Is(statErr, fs.ErrNotExist) {
				continue
			}
			return fail(fmt.Errorf("fsrecon: stat scope %q: %w", scope, statErr))
		}
		if info.IsDir() {
			// Scanner intentionally emits children, not the scan root itself.
			delete(previous, scope)
			observedDirectories[scope] = struct{}{}
		}
		s := internalscanner.Scanner{
			Recursive: t.config.Recursive, SymlinkPolicy: scannerPolicy(t.config.SymlinkPolicy),
		}
		if t.config.Filter != nil {
			s.Filter = func(entry internalscanner.Entry) bool {
				state := stateFromEntry(entry)
				return t.config.Filter(state.Path, state)
			}
		}
		err := s.Scan(ctx, scope, func(entry internalscanner.Entry) error {
			state := stateFromEntry(entry)
			if state.Type == FileTypeDirectory && t.config.Recursive {
				t.mu.RLock()
				tree := t.watchTree
				t.mu.RUnlock()
				if tree != nil {
					if err := tree.Add(state.Path); err != nil {
						return fmt.Errorf("add recursive watch: %w", err)
					}
				}
				observedDirectories[state.Path] = struct{}{}
			}
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
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if errors.Is(err, internalscanner.ErrSymlink) {
				err = fmt.Errorf("%w: %v", ErrSymlink, err)
			}
			return fail(fmt.Errorf("fsrecon: scan %q: %w", scope, err))
		}
	}
	if t.config.Recursive {
		t.mu.RLock()
		tree := t.watchTree
		t.mu.RUnlock()
		if tree != nil {
			if err := tree.Sync(scopes, observedDirectories); err != nil {
				return fail(fmt.Errorf("fsrecon: sync recursive watches: %w", err))
			}
		}
	}

	expected, err := t.loadExpectedScopes(ctx, scopes)
	if err != nil {
		return fail(err)
	}
	changes := internalreconcile.Diff(toInternalStates(previous), toInternalStates(actual), expected)
	report.Events = make([]Event, 0, len(changes)+len(policyEvents))
	for _, change := range changes {
		event := eventFromChange(change)
		countEvent(&report, event.Kind)
		t.addReportEvent(&report, event)
	}
	for _, event := range policyEvents {
		countEvent(&report, event.Kind)
		t.addReportEvent(&report, event)
	}

	deletes := make([]string, 0)
	for path := range previous {
		if _, ok := actual[path]; !ok {
			deletes = append(deletes, path)
		}
	}
	puts := make([]FileState, 0, len(actual))
	for _, state := range actual {
		puts = append(puts, state)
	}
	if batch, ok := t.store.(BatchSnapshotStore); ok {
		if err := batch.Apply(ctx, puts, deletes); err != nil {
			return fail(fmt.Errorf("fsrecon: apply snapshot batch: %w", err))
		}
	} else {
		for _, path := range deletes {
			if err := t.store.Delete(ctx, path); err != nil {
				return fail(fmt.Errorf("fsrecon: delete snapshot %q: %w", path, err))
			}
		}
		for _, state := range puts {
			if err := t.store.Put(ctx, state); err != nil {
				return fail(fmt.Errorf("fsrecon: put snapshot %q: %w", state.Path, err))
			}
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
	t.stats.eventsDropped.Add(report.EventsTruncated)
	t.setState(StateSynced)
	for _, event := range report.Events {
		t.sendEvent(event)
	}
	return report, nil
}

func (t *Tracker) addReportEvent(report *ReconcileReport, event Event) {
	if len(report.Events) < t.config.ReportEventLimit {
		report.Events = append(report.Events, event)
		return
	}
	report.EventsTruncated++
}

func collapseScopes(root string, requested []string) []string {
	paths := make([]string, 0, len(requested))
	for _, path := range requested {
		path = filepath.Clean(path)
		if !pathHasPrefix(path, root) {
			path = root
		}
		covered := false
		for _, existing := range paths {
			if pathHasPrefix(path, existing) {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		kept := paths[:0]
		for _, existing := range paths {
			if !pathHasPrefix(existing, path) {
				kept = append(kept, existing)
			}
		}
		paths = append(kept, path)
	}
	return paths
}

func (t *Tracker) loadExpected(ctx context.Context) (map[string]internalreconcile.Expected, error) {
	return t.loadExpectedScopes(ctx, []string{t.root})
}

func (t *Tracker) loadExpectedScopes(ctx context.Context, scopes []string) (map[string]internalreconcile.Expected, error) {
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
		inScope := false
		for _, scope := range scopes {
			if pathHasPrefix(path, scope) && path != scope {
				inScope = true
				break
			}
		}
		if !inScope {
			return nil
		}
		expected[path] = internalreconcile.Expected{
			Path: path, Size: entry.Size, Fingerprint: append([]byte(nil), entry.Fingerprint...),
		}
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
