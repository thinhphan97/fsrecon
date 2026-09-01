package fsrecon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	internalbackend "github.com/thinhphan97/fsrecon/internal/backend"
	fsnotifybackend "github.com/thinhphan97/fsrecon/internal/backend/fsnotify"
	"github.com/thinhphan97/fsrecon/internal/debounce"
	"github.com/thinhphan97/fsrecon/internal/dirtyset"
	"github.com/thinhphan97/fsrecon/internal/normalize"
	internalscanner "github.com/thinhphan97/fsrecon/internal/scanner"
	"github.com/thinhphan97/fsrecon/internal/watchtree"
)

// TrackerState describes the current lifecycle and consistency state.
type TrackerState uint8

const (
	StateCreated TrackerState = iota
	StateStarting
	StateSyncing
	StateSynced
	StateDirty
	StateReconciling
	StateStopped
	StateDegraded
)

func (s TrackerState) String() string {
	switch s {
	case StateCreated:
		return "CREATED"
	case StateStarting:
		return "STARTING"
	case StateSyncing:
		return "SYNCING"
	case StateSynced:
		return "SYNCED"
	case StateDirty:
		return "DIRTY"
	case StateReconciling:
		return "RECONCILING"
	case StateStopped:
		return "STOPPED"
	case StateDegraded:
		return "DEGRADED"
	default:
		return "UNKNOWN"
	}
}

// Tracker combines native notifications, scanning, snapshots, and reconciliation.
type Tracker struct {
	config Config
	root   string
	store  SnapshotStore

	mu             sync.RWMutex
	state          TrackerState
	started        bool
	closed         bool
	backendHealthy bool
	cancel         context.CancelFunc

	reconcileMu  sync.Mutex
	integrityMu  sync.Mutex
	backend      internalbackend.Backend
	watchTree    *watchtree.Tree
	newBackend   func(uint) internalbackend.Backend
	events       chan Event
	errors       chan error
	closeOnce    sync.Once
	wg           sync.WaitGroup
	stats        trackerStats
	generation   atomic.Uint64
	dirtyPending atomic.Bool
}

type trackerStats struct {
	eventsReceived        atomic.Uint64
	eventsCoalesced       atomic.Uint64
	reconciliations       atomic.Uint64
	filesScanned          atomic.Uint64
	missingDetected       atomic.Uint64
	orphansDetected       atomic.Uint64
	dirtyPaths            atomic.Uint64
	integrityScanned      atomic.Uint64
	corruptDetected       atomic.Uint64
	publicEventsDropped   atomic.Uint64
	reportEventsTruncated atomic.Uint64
	nativeEventsDropped   atomic.Uint64
	backendOverflows      atomic.Uint64
}

// New validates config and constructs a tracker without touching the filesystem.
func New(config Config) (*Tracker, error) {
	if strings.TrimSpace(config.Root) == "" {
		return nil, errors.New("fsrecon: root is required")
	}
	if config.EventBuffer < 0 {
		return nil, errors.New("fsrecon: event buffer cannot be negative")
	}
	if config.ReportEventLimit < 0 {
		return nil, errors.New("fsrecon: report event limit cannot be negative")
	}
	if config.ChangeBatchSize < 0 {
		return nil, errors.New("fsrecon: change batch size cannot be negative")
	}
	if config.ReconcileInterval < 0 {
		return nil, errors.New("fsrecon: reconcile interval cannot be negative")
	}
	if config.DebounceWindow < 0 {
		return nil, errors.New("fsrecon: debounce window cannot be negative")
	}
	if config.SymlinkPolicy > RejectSymlinks {
		return nil, errors.New("fsrecon: invalid symlink policy")
	}
	if config.HardlinkPolicy > RejectHardlinks {
		return nil, errors.New("fsrecon: invalid hardlink policy")
	}
	if config.ExpectedScope > ExpectedAllEntries {
		return nil, errors.New("fsrecon: invalid expected entry scope")
	}
	root, err := filepath.Abs(config.Root)
	if err != nil {
		return nil, fmt.Errorf("fsrecon: resolve root: %w", err)
	}
	config.Root = filepath.Clean(root)
	if config.Store == nil {
		config.Store = NewMemoryStore()
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = defaultEventBuffer
	}
	if config.ReportEventLimit == 0 {
		config.ReportEventLimit = defaultReportEventLimit
	}
	if config.ChangeBatchSize == 0 {
		config.ChangeBatchSize = defaultChangeBatchSize
	}
	if config.DebounceWindow == 0 {
		config.DebounceWindow = defaultDebounceWindow
	}
	return &Tracker{
		config: config, root: config.Root, store: config.Store,
		state: StateCreated, events: make(chan Event, config.EventBuffer),
		errors:     make(chan error, 16),
		newBackend: func(buffer uint) internalbackend.Backend { return fsnotifybackend.New(buffer) },
	}, nil
}

// Start registers the native watcher before the initial reconciliation, then
// processes kernel notifications and the optional periodic safety net.
func (t *Tracker) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("fsrecon: nil context")
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}
	if t.started {
		t.mu.Unlock()
		return ErrAlreadyStarted
	}
	t.started = true
	t.state = StateStarting
	runCtx, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	t.mu.Unlock()

	native := t.newBackend(fsnotifybackend.DefaultBuffer)
	if err := native.Start(runCtx, t.root); err != nil {
		t.setState(StateDirty)
		cancel()
		return fmt.Errorf("fsrecon: start native watcher: %w", err)
	}
	t.mu.Lock()
	t.backend = native
	t.watchTree = watchtree.New(t.root, native)
	t.backendHealthy = true
	t.mu.Unlock()

	t.setState(StateSyncing)
	t.wg.Add(1)
	go t.run(runCtx)
	if _, err := t.Reconcile(runCtx); err != nil {
		t.setState(StateDirty)
		cancel()
		t.wg.Wait()
		return err
	}
	return nil
}

func (t *Tracker) run(ctx context.Context) {
	defer t.wg.Done()
	defer func() {
		t.closeBackend()
		t.stopFromContext()
	}()

	t.mu.RLock()
	native := t.backend
	t.mu.RUnlock()
	var nativeEvents <-chan internalbackend.RawEvent
	var nativeErrors <-chan error
	if native != nil {
		nativeEvents = native.Events()
		nativeErrors = native.Errors()
	}

	var ticker *time.Ticker
	var ticks <-chan time.Time
	if t.config.ReconcileInterval > 0 {
		ticker = time.NewTicker(t.config.ReconcileInterval)
		ticks = ticker.C
		defer ticker.Stop()
	}
	dirty := dirtyset.New(t.root)
	var dirtyMu sync.Mutex
	reconcileWake := make(chan struct{}, 1)
	workerDone := make(chan struct{})
	addDirty := func(path string) {
		dirtyMu.Lock()
		changed := dirty.Add(path)
		count := dirty.Len()
		dirtyMu.Unlock()
		t.dirtyPending.Store(count > 0)
		t.stats.dirtyPaths.Store(uint64(count))
		if !changed {
			t.stats.eventsCoalesced.Add(1)
		}
		t.markDirty()
	}
	wakeReconciler := func() {
		select {
		case reconcileWake <- struct{}{}:
		default:
		}
	}
	go func() {
		defer close(workerDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-reconcileWake:
				dirtyMu.Lock()
				scopes := dirty.Drain()
				t.stats.dirtyPaths.Store(0)
				t.dirtyPending.Store(false)
				dirtyMu.Unlock()
				if len(scopes) > 0 {
					t.reconcileScopesAfterHint(ctx, scopes)
				}
			}
		}
	}()
	defer func() { <-workerDone }()

	backendStoppedReported := false
	quiet := debounce.New(t.config.DebounceWindow)
	defer quiet.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case raw, ok := <-nativeEvents:
			if !ok {
				nativeEvents = nil
				if ctx.Err() == nil && !backendStoppedReported {
					backendStoppedReported = true
					t.markBackendUnhealthy()
					t.sendError(ErrBackendStopped)
					addDirty(t.root)
					wakeReconciler()
				}
				continue
			}
			t.stats.eventsReceived.Add(1)
			hint, ok := normalize.Event(raw, t.root)
			if !ok {
				continue
			}
			addDirty(hint.Scope)
			quiet.Trigger()
		case err, ok := <-nativeErrors:
			if !ok {
				nativeErrors = nil
				continue
			}
			t.handleBackendError(err)
			addDirty(t.root)
			wakeReconciler()
		case <-quiet.C():
			wakeReconciler()
		case <-ticks:
			quiet.Stop()
			addDirty(t.root)
			wakeReconciler()
		}
	}
}

func (t *Tracker) reconcileScopesAfterHint(ctx context.Context, scopes []string) {
	if _, err := t.reconcileScopes(ctx, scopes); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
		t.sendError(err)
	}
}

func (t *Tracker) handleBackendError(err error) {
	t.markDirty()
	t.sendError(fmt.Errorf("fsrecon: native watcher: %w", err))
	if errors.Is(err, internalbackend.ErrOverflow) {
		t.stats.backendOverflows.Add(1)
		now := time.Now()
		t.sendEvent(Event{Kind: EventOverflow, Path: t.root, Source: SourceWatcher, Time: now})
		t.sendEvent(Event{Kind: EventRescanRequired, Path: t.root, Source: SourceWatcher, Time: now})
	}
}

func (t *Tracker) closeBackend() {
	t.mu.RLock()
	native := t.backend
	t.mu.RUnlock()
	if native != nil {
		if err := native.Close(); err != nil {
			t.sendError(fmt.Errorf("fsrecon: close native watcher: %w", err))
		}
	}
}

func (t *Tracker) stopFromContext() {
	t.mu.Lock()
	t.closed = true
	t.state = StateStopped
	t.mu.Unlock()
	t.reconcileMu.Lock()
	t.integrityMu.Lock()
	t.closeChannels()
	t.integrityMu.Unlock()
	t.reconcileMu.Unlock()
}

// Close stops background work. It is safe to call more than once.
func (t *Tracker) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		t.wg.Wait()
		t.reconcileMu.Lock()
		t.integrityMu.Lock()
		t.closeChannels()
		t.integrityMu.Unlock()
		t.reconcileMu.Unlock()
		return nil
	}
	t.closed = true
	t.state = StateStopped
	cancel := t.cancel
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	t.wg.Wait()
	t.reconcileMu.Lock()
	t.integrityMu.Lock()
	t.closeChannels()
	t.integrityMu.Unlock()
	t.reconcileMu.Unlock()
	return nil
}

func (t *Tracker) closeChannels() {
	t.closeOnce.Do(func() {
		close(t.events)
		close(t.errors)
	})
}

// Events returns the bounded semantic-event stream.
func (t *Tracker) Events() <-chan Event { return t.events }

// Errors returns asynchronous backend and reconciliation errors.
func (t *Tracker) Errors() <-chan error { return t.errors }

// State returns the current tracker lifecycle state.
func (t *Tracker) State() TrackerState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

func (t *Tracker) setState(state TrackerState) {
	t.mu.Lock()
	if !t.closed {
		t.state = state
	}
	t.mu.Unlock()
}

func (t *Tracker) markBackendUnhealthy() {
	t.mu.Lock()
	if !t.closed {
		t.backendHealthy = false
		t.state = StateDegraded
	}
	t.mu.Unlock()
}

func (t *Tracker) markDirty() {
	t.mu.Lock()
	if !t.closed {
		if t.started && !t.backendHealthy {
			t.state = StateDegraded
		} else {
			t.state = StateDirty
		}
	}
	t.mu.Unlock()
}

func (t *Tracker) setConsistentState() {
	t.mu.Lock()
	if !t.closed {
		switch {
		case t.started && !t.backendHealthy:
			t.state = StateDegraded
		case t.dirtyPending.Load():
			t.state = StateDirty
		default:
			t.state = StateSynced
		}
	}
	t.mu.Unlock()
}

// Stats returns a lock-free snapshot of counters and queue gauges.
func (t *Tracker) Stats() Stats {
	publicDropped := t.stats.publicEventsDropped.Load()
	return Stats{
		EventsReceived: t.stats.eventsReceived.Load(), EventsCoalesced: t.stats.eventsCoalesced.Load(),
		EventsDropped: publicDropped, PublicEventsDropped: publicDropped,
		ReportEventsTruncated: t.stats.reportEventsTruncated.Load(),
		NativeEventsDropped:   t.stats.nativeEventsDropped.Load(), BackendOverflows: t.stats.backendOverflows.Load(),
		Reconciliations: t.stats.reconciliations.Load(),
		FilesScanned:    t.stats.filesScanned.Load(), MissingDetected: t.stats.missingDetected.Load(),
		OrphansDetected: t.stats.orphansDetected.Load(), DirtyPaths: t.stats.dirtyPaths.Load(),
		QueueDepth: uint64(len(t.events)), IntegrityScanned: t.stats.integrityScanned.Load(),
		CorruptDetected: t.stats.corruptDetected.Load(),
	}
}

func (t *Tracker) sendEvent(event Event) {
	select {
	case t.events <- event:
	default:
		t.stats.publicEventsDropped.Add(1)
	}
}

func (t *Tracker) sendError(err error) {
	select {
	case t.errors <- err:
	default:
	}
}

func scannerPolicy(policy SymlinkPolicy) internalscanner.SymlinkPolicy {
	switch policy {
	case IgnoreSymlinks:
		return internalscanner.IgnoreSymlinks
	case ReportSymlinks:
		return internalscanner.ReportSymlinks
	case FollowSymlinks:
		return internalscanner.FollowSymlinks
	case RejectSymlinks:
		return internalscanner.RejectSymlinks
	default:
		return internalscanner.IgnoreSymlinks
	}
}

func fileType(mode fs.FileMode) FileType {
	switch {
	case mode.IsRegular():
		return FileTypeRegular
	case mode.IsDir():
		return FileTypeDirectory
	case mode&fs.ModeSymlink != 0:
		return FileTypeSymlink
	default:
		return FileTypeOther
	}
}

func stateFromEntry(entry internalscanner.Entry) FileState {
	return FileState{
		Path: entry.Path, ID: newFileID(entry.Identity), Type: fileType(entry.Mode),
		Size: entry.Size, ModTime: entry.ModTime, Mode: entry.Mode,
	}
}
