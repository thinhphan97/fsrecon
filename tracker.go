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
	internalscanner "github.com/thinhphan97/fsrecon/internal/scanner"
)

type TrackerState uint8

const (
	StateCreated TrackerState = iota
	StateStarting
	StateSyncing
	StateSynced
	StateDirty
	StateReconciling
	StateStopped
)

func (s TrackerState) String() string {
	names := [...]string{"CREATED", "STARTING", "SYNCING", "SYNCED", "DIRTY", "RECONCILING", "STOPPED"}
	if int(s) >= len(names) {
		return "UNKNOWN"
	}
	return names[s]
}

type Tracker struct {
	config Config
	root   string
	store  SnapshotStore

	mu      sync.RWMutex
	state   TrackerState
	started bool
	closed  bool
	cancel  context.CancelFunc

	reconcileMu sync.Mutex
	backend     internalbackend.Backend
	events      chan Event
	errors      chan error
	closeOnce   sync.Once
	wg          sync.WaitGroup
	stats       trackerStats
}

type trackerStats struct {
	eventsReceived  atomic.Uint64
	eventsCoalesced atomic.Uint64
	eventsDropped   atomic.Uint64
	reconciliations atomic.Uint64
	filesScanned    atomic.Uint64
	missingDetected atomic.Uint64
	orphansDetected atomic.Uint64
	dirtyPaths      atomic.Uint64
}

// New validates config and constructs a tracker without touching the filesystem.
func New(config Config) (*Tracker, error) {
	if strings.TrimSpace(config.Root) == "" {
		return nil, errors.New("fsrecon: root is required")
	}
	if config.EventBuffer < 0 {
		return nil, errors.New("fsrecon: event buffer cannot be negative")
	}
	if config.ReconcileInterval < 0 {
		return nil, errors.New("fsrecon: reconcile interval cannot be negative")
	}
	if config.SymlinkPolicy > RejectSymlinks {
		return nil, errors.New("fsrecon: invalid symlink policy")
	}
	if config.HardlinkPolicy > RejectHardlinks {
		return nil, errors.New("fsrecon: invalid hardlink policy")
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
	return &Tracker{
		config: config, root: config.Root, store: config.Store,
		state: StateCreated, events: make(chan Event, config.EventBuffer),
		errors: make(chan error, 16),
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

	native := fsnotifybackend.New(fsnotifybackend.DefaultBuffer)
	if err := native.Start(runCtx, t.root); err != nil {
		t.setState(StateDirty)
		cancel()
		return fmt.Errorf("fsrecon: start native watcher: %w", err)
	}
	t.mu.Lock()
	t.backend = native
	t.mu.Unlock()

	t.setState(StateSyncing)
	if _, err := t.Reconcile(runCtx); err != nil {
		t.setState(StateDirty)
		cancel()
		_ = native.Close()
		return err
	}

	t.wg.Add(1)
	go t.run(runCtx)
	return nil
}

func (t *Tracker) run(ctx context.Context) {
	defer t.wg.Done()
	defer t.stopFromContext()
	defer t.closeBackend()

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
	backendStoppedReported := false
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-nativeEvents:
			if !ok {
				nativeEvents = nil
				if ctx.Err() == nil && !backendStoppedReported {
					backendStoppedReported = true
					t.setState(StateDirty)
					t.sendError(ErrBackendStopped)
					t.reconcileAfterHint(ctx)
				}
				continue
			}
			t.stats.eventsReceived.Add(1)
			t.reconcileAfterHint(ctx)
		case err, ok := <-nativeErrors:
			if !ok {
				nativeErrors = nil
				continue
			}
			t.handleBackendError(ctx, err)
		case <-ticks:
			t.reconcileAfterHint(ctx)
		}
	}
}

func (t *Tracker) reconcileAfterHint(ctx context.Context) {
	if _, err := t.Reconcile(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
		t.sendError(err)
	}
}

func (t *Tracker) handleBackendError(ctx context.Context, err error) {
	t.setState(StateDirty)
	t.sendError(fmt.Errorf("fsrecon: native watcher: %w", err))
	if errors.Is(err, internalbackend.ErrOverflow) {
		now := time.Now()
		t.sendEvent(Event{Kind: EventOverflow, Path: t.root, Source: SourceWatcher, Time: now})
		t.sendEvent(Event{Kind: EventRescanRequired, Path: t.root, Source: SourceWatcher, Time: now})
	}
	t.reconcileAfterHint(ctx)
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
	t.closeChannels()
	t.reconcileMu.Unlock()
}

// Close stops background work. It is safe to call more than once.
func (t *Tracker) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		t.wg.Wait()
		t.reconcileMu.Lock()
		t.closeChannels()
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
	t.closeChannels()
	t.reconcileMu.Unlock()
	return nil
}

func (t *Tracker) closeChannels() {
	t.closeOnce.Do(func() {
		close(t.events)
		close(t.errors)
	})
}

func (t *Tracker) Events() <-chan Event { return t.events }
func (t *Tracker) Errors() <-chan error { return t.errors }

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

func (t *Tracker) Stats() Stats {
	return Stats{
		EventsReceived: t.stats.eventsReceived.Load(), EventsCoalesced: t.stats.eventsCoalesced.Load(),
		EventsDropped: t.stats.eventsDropped.Load(), Reconciliations: t.stats.reconciliations.Load(),
		FilesScanned: t.stats.filesScanned.Load(), MissingDetected: t.stats.missingDetected.Load(),
		OrphansDetected: t.stats.orphansDetected.Load(), DirtyPaths: t.stats.dirtyPaths.Load(),
		QueueDepth: uint64(len(t.events)),
	}
}

func (t *Tracker) sendEvent(event Event) {
	select {
	case t.events <- event:
	default:
		t.stats.eventsDropped.Add(1)
	}
}

func (t *Tracker) sendError(err error) {
	select {
	case t.errors <- err:
	default:
	}
}

func scannerPolicy(policy SymlinkPolicy) internalscanner.SymlinkPolicy {
	return internalscanner.SymlinkPolicy(policy)
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
