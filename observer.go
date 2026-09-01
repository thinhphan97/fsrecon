package fsrecon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	internalbackend "github.com/thinhphan97/fsrecon/internal/backend"
	fsnotifybackend "github.com/thinhphan97/fsrecon/internal/backend/fsnotify"
	"github.com/thinhphan97/fsrecon/internal/watchtree"
)

type Observer struct {
	config      ObserverConfig
	root        string
	mu          sync.RWMutex
	state       ObserverState
	backend     internalbackend.Backend
	tree        *watchtree.Tree
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	closeOnce   sync.Once
	hints       chan Hint
	errors      chan error
	newBackend  func(uint) internalbackend.Backend
	pendingMu   sync.Mutex
	pending     map[string]Hint
	rootPending bool
	stats       observerStatsAtomic
}

func NewObserver(config ObserverConfig) (*Observer, error) {
	if config.Root == "" {
		return nil, errors.New("fsrecon: observer root is required")
	}
	if config.HintBuffer < 0 || config.MaxPendingHints < 0 || config.DebounceWindow < 0 {
		return nil, errors.New("fsrecon: invalid observer limits")
	}
	root, err := filepath.Abs(config.Root)
	if err != nil {
		return nil, err
	}
	config.Root = filepath.Clean(root)
	if config.HintBuffer == 0 {
		config.HintBuffer = 256
	}
	if config.MaxPendingHints == 0 {
		config.MaxPendingHints = 10000
	}
	if config.DebounceWindow == 0 {
		config.DebounceWindow = 100 * time.Millisecond
	}
	return &Observer{config: config, root: config.Root, state: ObserverCreated, hints: make(chan Hint, config.HintBuffer), errors: make(chan error, 16), pending: make(map[string]Hint), newBackend: func(n uint) internalbackend.Backend { return fsnotifybackend.New(n) }}, nil
}

func (o *Observer) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("fsrecon: nil context")
	}
	o.mu.Lock()
	if o.state != ObserverCreated {
		o.mu.Unlock()
		return errors.New("fsrecon: observer already started")
	}
	o.state = ObserverStarting
	runCtx, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	o.mu.Unlock()
	b := o.newBackend(fsnotifybackend.DefaultBuffer)
	if err := b.Start(runCtx, o.root); err != nil {
		cancel()
		o.setState(ObserverDegraded)
		return fmt.Errorf("fsrecon: start observer watcher: %w", err)
	}
	tree := watchtree.New(o.root, b)
	o.mu.Lock()
	o.backend, o.tree = b, tree
	o.mu.Unlock()
	if o.config.Recursive {
		if err := walkObserverDirectories(runCtx, o.root, func(path string) error { return tree.Add(path) }); err != nil {
			_ = b.Close()
			cancel()
			o.setState(ObserverDegraded)
			return err
		}
	}
	o.setState(ObserverRunning)
	o.enqueue(Hint{Path: o.root, Scope: HintSubtree, Cause: HintStartup, Time: time.Now()})
	o.wg.Add(2)
	go o.collect(runCtx)
	go o.emit(runCtx)
	return nil
}

func (o *Observer) collect(ctx context.Context) {
	defer o.wg.Done()
	o.mu.RLock()
	b := o.backend
	tree := o.tree
	o.mu.RUnlock()
	events, errs := b.Events(), b.Errors()
	for events != nil || errs != nil {
		select {
		case <-ctx.Done():
			return
		case raw, ok := <-events:
			if !ok {
				events = nil
				if ctx.Err() == nil {
					o.degrade(ErrBackendStopped)
				}
				continue
			}
			o.stats.received.Add(1)
			o.handleRaw(ctx, tree, raw)
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if errors.Is(err, internalbackend.ErrOverflow) {
				o.stats.overflows.Add(1)
				o.enqueue(Hint{Path: o.root, Scope: HintSubtree, Cause: HintOverflow, Time: time.Now()})
			}
			o.sendError(err)
		}
	}
}

func (o *Observer) handleRaw(ctx context.Context, tree *watchtree.Tree, raw internalbackend.RawEvent) {
	path := filepath.Clean(raw.Path)
	if !withinObserver(o.root, path) {
		return
	}
	if raw.Op&internalbackend.OpRemove != 0 || raw.Op&internalbackend.OpRename != 0 {
		if tree != nil {
			_ = tree.RemoveSubtree(path)
		}
	}
	if o.config.Recursive && raw.Op&internalbackend.OpCreate != 0 {
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			_ = tree.Add(path)
			_ = walkObserverDirectories(ctx, path, func(p string) error { return tree.Add(p) })
			o.enqueue(Hint{Path: path, Scope: HintSubtree, Cause: HintNativeChange, Time: time.Now()})
			return
		}
	}
	if o.config.Filter != nil && !o.config.Filter(path) {
		return
	}
	o.enqueue(Hint{Path: path, Scope: HintPath, Cause: HintNativeChange, Time: time.Now()})
}

func (o *Observer) emit(ctx context.Context) {
	defer o.wg.Done()
	ticker := time.NewTicker(o.config.DebounceWindow)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.flush()
		}
	}
}
func (o *Observer) flush() {
	o.pendingMu.Lock()
	defer o.pendingMu.Unlock()
	if o.rootPending {
		if o.trySend(Hint{Path: o.root, Scope: HintSubtree, Cause: HintNativeChange, Time: time.Now()}) {
			o.rootPending = false
		}
		return
	}
	for path, h := range o.pending {
		if !o.trySend(h) {
			break
		}
		delete(o.pending, path)
	}
}
func (o *Observer) trySend(h Hint) bool {
	select {
	case o.hints <- h:
		o.stats.emitted.Add(1)
		return true
	default:
		o.stats.dropped.Add(1)
		return false
	}
}
func (o *Observer) enqueue(h Hint) {
	o.pendingMu.Lock()
	defer o.pendingMu.Unlock()
	if h.Scope == HintSubtree && h.Path == o.root {
		o.rootPending = true
		o.pending = map[string]Hint{}
		return
	}
	if o.rootPending {
		return
	}
	if _, ok := o.pending[h.Path]; ok {
		o.stats.coalesced.Add(1)
		return
	}
	if len(o.pending) >= o.config.MaxPendingHints {
		o.pending = map[string]Hint{}
		o.rootPending = true
		return
	}
	o.pending[h.Path] = h
}
func (o *Observer) degrade(err error) {
	o.setState(ObserverDegraded)
	o.enqueue(Hint{Path: o.root, Scope: HintSubtree, Cause: HintBackendStopped, Time: time.Now()})
	o.sendError(err)
}
func (o *Observer) sendError(err error) {
	select {
	case o.errors <- err:
	default:
	}
}
func (o *Observer) setState(s ObserverState) {
	o.mu.Lock()
	if o.state != ObserverStopped {
		o.state = s
	}
	o.mu.Unlock()
}
func (o *Observer) Hints() <-chan Hint   { return o.hints }
func (o *Observer) Errors() <-chan error { return o.errors }
func (o *Observer) State() ObserverState { o.mu.RLock(); defer o.mu.RUnlock(); return o.state }
func (o *Observer) Stats() ObserverStats {
	return ObserverStats{NativeEventsReceived: o.stats.received.Load(), HintsEmitted: o.stats.emitted.Load(), HintsCoalesced: o.stats.coalesced.Load(), OverflowCount: o.stats.overflows.Load(), PendingHints: o.pendingCount(), PublicHintsDropped: o.stats.dropped.Load(), WatchedDirectories: uint64(o.treeCount())}
}
func (o *Observer) pendingCount() uint64 {
	o.pendingMu.Lock()
	defer o.pendingMu.Unlock()
	if o.rootPending {
		return 1
	}
	return uint64(len(o.pending))
}
func (o *Observer) treeCount() int {
	o.mu.RLock()
	t := o.tree
	o.mu.RUnlock()
	if t == nil {
		return 0
	}
	return t.Count()
}
func (o *Observer) Close() error {
	o.closeOnce.Do(func() {
		o.mu.Lock()
		o.state = ObserverStopped
		cancel := o.cancel
		b := o.backend
		o.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if b != nil {
			_ = b.Close()
		}
		o.wg.Wait()
		close(o.hints)
		close(o.errors)
	})
	return nil
}

func withinObserver(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !(len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator))
}
func walkObserverDirectories(ctx context.Context, root string, fn func(string) error) error {
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return nil
	}
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if path == root {
			return fn(path)
		}
		if d.IsDir() {
			return fn(path)
		}
		return nil
	})
}
