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
	report.Generation = t.generation.Load() + 1
	fail := func(err error) (ReconcileReport, error) {
		report.Duration = time.Since(report.StartedAt)
		t.markDirty()
		return report, err
	}
	scopes := collapseScopes(t.root, requested)
	if len(scopes) == 0 {
		report.Duration = time.Since(report.StartedAt)
		t.generation.Store(report.Generation)
		t.setConsistentState()
		return report, nil
	}
	if store, ok := t.store.(*BoltStore); ok && len(scopes) == 1 && scopes[0] == t.root {
		return t.reconcileBoltLocked(ctx, store, report)
	}

	previous := make(map[string]FileState)
	for _, scope := range scopes {
		if err := walkSnapshotScope(ctx, t.store, scope, func(state FileState) error {
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
	report.Events = make([]Event, 0, min(t.config.ReportEventLimit, len(actual)+len(previous)))
	delivery := newChangeBatcher(ctx, t.config.ChangeSink, report.Generation, t.config.ChangeBatchSize)
	err = internalreconcile.WalkDiffScoped(toInternalStates(previous), toInternalStates(actual), expected, func(state internalreconcile.State) bool {
		return t.config.ExpectedScope == ExpectedAllEntries || fileTypeFromInternal(state.Type) == FileTypeRegular
	}, func(change internalreconcile.Change) error {
		event, err := eventFromChange(change)
		if err != nil {
			return err
		}
		countEvent(&report, event.Kind)
		t.addReportEvent(&report, event)
		return delivery.Add(event)
	})
	if err != nil {
		return fail(err)
	}
	for _, event := range policyEvents {
		countEvent(&report, event.Kind)
		t.addReportEvent(&report, event)
		if err := delivery.Add(event); err != nil {
			return fail(err)
		}
	}
	if err := delivery.Finish(); err != nil {
		return fail(err)
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
			if ok && (entry.Type == nil || internalTypeFromPublic(state.Type) == *entry.Type) && (entry.Size == nil || state.Size == *entry.Size) {
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
	t.stats.reportEventsTruncated.Add(report.EventsTruncated)
	t.generation.Store(report.Generation)
	t.setConsistentState()
	for _, event := range report.Events {
		t.sendEvent(event)
	}
	return report, nil
}

func walkSnapshotScope(ctx context.Context, store SnapshotStore, scope string, fn func(FileState) error) error {
	if scoped, ok := store.(ScopedSnapshotStore); ok {
		return scoped.WalkScope(ctx, scope, fn)
	}
	return store.Walk(ctx, scope, fn)
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
	consume := func(entry ExpectedEntry) error {
		path, err := t.expectedPath(entry.Path)
		if err != nil {
			return err
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
		inScope := false
		if path != t.root {
			for _, scope := range scopes {
				if pathHasPrefix(path, scope) {
					inScope = true
					break
				}
			}
		}
		if !inScope {
			return nil
		}
		expectedType := entry.Type
		if expectedType == nil && t.config.ExpectedScope == ExpectedRegularFiles {
			regular := FileTypeRegular
			expectedType = &regular
		}
		var internalType *internalreconcile.FileType
		if expectedType != nil {
			value := internalTypeFromPublic(*expectedType)
			internalType = &value
		}
		expected[path] = internalreconcile.Expected{
			Path: path, Type: internalType, Size: entry.Size, Fingerprint: append([]byte(nil), entry.Fingerprint...),
		}
		return nil
	}
	var err error
	if scoped, ok := t.config.Expected.(ScopedExpectedProvider); ok {
		for _, scope := range scopes {
			if err = scoped.WalkExpectedScope(ctx, t.root, scope, consume); err != nil {
				break
			}
		}
	} else {
		err = t.config.Expected.WalkExpected(ctx, t.root, consume)
	}
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
			Path: path, ID: state.ID.value, Type: internalTypeFromPublic(state.Type), Size: state.Size,
			ModUnix: state.ModTime.UnixNano(), Mode: uint32(state.Mode),
		}
	}
	return result
}

func eventFromChange(change internalreconcile.Change) (Event, error) {
	var before, after *FileState
	if change.Before != nil {
		state := fromInternalState(*change.Before)
		before = &state
	}
	if change.After != nil {
		state := fromInternalState(*change.After)
		after = &state
	}
	kind, err := eventKindFromInternal(change.Kind)
	if err != nil {
		return Event{}, err
	}
	return Event{
		Kind: kind, Path: change.Path, OldPath: change.OldPath,
		Before: before, After: after, Source: SourceReconcile, Time: time.Now(),
	}, nil
}

func fromInternalState(state internalreconcile.State) FileState {
	return FileState{
		Path: state.Path, ID: newFileID(state.ID), Type: fileTypeFromInternal(state.Type), Size: state.Size,
		ModTime: time.Unix(0, state.ModUnix), Mode: fs.FileMode(state.Mode),
	}
}

func eventKindFromInternal(kind internalreconcile.Kind) (EventKind, error) {
	switch kind {
	case internalreconcile.Created:
		return EventCreated, nil
	case internalreconcile.Modified:
		return EventModified, nil
	case internalreconcile.Deleted:
		return EventDeleted, nil
	case internalreconcile.Moved:
		return EventMoved, nil
	case internalreconcile.AttributeChanged:
		return EventAttributeChanged, nil
	case internalreconcile.Missing:
		return EventMissing, nil
	case internalreconcile.Orphan:
		return EventOrphan, nil
	case internalreconcile.Replaced:
		return EventReplaced, nil
	case internalreconcile.Invalid:
		return EventInvalid, nil
	default:
		return 0, fmt.Errorf("fsrecon: unknown internal event kind %d", kind)
	}
}

func internalTypeFromPublic(fileType FileType) internalreconcile.FileType {
	switch fileType {
	case FileTypeRegular:
		return internalreconcile.TypeRegular
	case FileTypeDirectory:
		return internalreconcile.TypeDirectory
	case FileTypeSymlink:
		return internalreconcile.TypeSymlink
	case FileTypeOther:
		return internalreconcile.TypeOther
	default:
		return internalreconcile.TypeUnknown
	}
}

func fileTypeFromInternal(fileType internalreconcile.FileType) FileType {
	switch fileType {
	case internalreconcile.TypeRegular:
		return FileTypeRegular
	case internalreconcile.TypeDirectory:
		return FileTypeDirectory
	case internalreconcile.TypeSymlink:
		return FileTypeSymlink
	case internalreconcile.TypeOther:
		return FileTypeOther
	default:
		return FileTypeUnknown
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
