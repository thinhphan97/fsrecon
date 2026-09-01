// Package fsnotify implements the native backend with fsnotify.
package fsnotify

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	fsnotifylib "github.com/fsnotify/fsnotify"
	"github.com/thinhphan97/fsrecon/internal/backend"
)

const DefaultBuffer = 1024

type Backend struct {
	buffer uint
	events chan backend.RawEvent
	errors chan error

	mu      sync.Mutex
	watcher *fsnotifylib.Watcher
	started bool
	closed  bool

	nativeCloseOnce sync.Once
	outputCloseOnce sync.Once
	wg              sync.WaitGroup
}

func New(buffer uint) *Backend {
	if buffer == 0 {
		buffer = DefaultBuffer
	}
	return &Backend{
		buffer: buffer,
		events: make(chan backend.RawEvent, buffer),
		errors: make(chan error, 16),
	}
}

func (b *Backend) Start(ctx context.Context, root string) error {
	if ctx == nil {
		return errors.New("fsnotify backend: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errors.New("fsnotify backend: closed")
	}
	if b.started {
		return errors.New("fsnotify backend: already started")
	}

	watcher, err := fsnotifylib.NewBufferedWatcher(b.buffer)
	if err != nil {
		return fmt.Errorf("create fsnotify watcher: %w", err)
	}
	root = filepath.Clean(root)
	if err := watcher.Add(root); err != nil {
		_ = watcher.Close()
		return fmt.Errorf("watch %q: %w", root, err)
	}
	b.watcher = watcher
	b.started = true
	b.wg.Add(1)
	go b.pump(ctx, watcher)
	return nil
}

func (b *Backend) Events() <-chan backend.RawEvent { return b.events }
func (b *Backend) Errors() <-chan error            { return b.errors }

func (b *Backend) Add(path string) error {
	b.mu.Lock()
	watcher := b.watcher
	closed := b.closed
	b.mu.Unlock()
	if closed || watcher == nil {
		return errors.New("fsnotify backend: not running")
	}
	if err := watcher.Add(filepath.Clean(path)); err != nil {
		return fmt.Errorf("watch %q: %w", path, err)
	}
	return nil
}

func (b *Backend) Remove(path string) error {
	b.mu.Lock()
	watcher := b.watcher
	closed := b.closed
	b.mu.Unlock()
	if closed || watcher == nil {
		return nil
	}
	if err := watcher.Remove(filepath.Clean(path)); err != nil {
		// Windows resolves attributes while removing a watch and reports a
		// PathError when the directory has already moved or disappeared. Linux
		// invalidates the inotify descriptor implicitly and returns EINVAL.
		// Both mean the requested cleanup has already happened.
		if isRemovedWatchError(err) {
			return nil
		}
		return fmt.Errorf("remove watch %q: %w", path, err)
	}
	return nil
}

func isRemovedWatchError(err error) bool {
	return errors.Is(err, fsnotifylib.ErrNonExistentWatch) ||
		errors.Is(err, fs.ErrNotExist) ||
		os.IsNotExist(err) ||
		(runtime.GOOS == "linux" && errors.Is(err, syscall.EINVAL))
}

func (b *Backend) Close() error {
	b.mu.Lock()
	b.closed = true
	started := b.started
	b.mu.Unlock()
	var err error
	if started {
		err = b.closeNative()
		b.wg.Wait()
	}
	b.closeOutputs()
	return err
}

func (b *Backend) pump(ctx context.Context, watcher *fsnotifylib.Watcher) {
	defer b.wg.Done()
	defer b.closeOutputs()
	defer b.closeNative()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			raw := backend.RawEvent{Path: filepath.Clean(event.Name), Op: mapOp(event.Op), Time: time.Now()}
			if raw.Op == 0 {
				continue
			}
			select {
			case b.events <- raw:
			default:
				b.sendError(backend.ErrOverflow)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			if errors.Is(err, fsnotifylib.ErrEventOverflow) {
				err = fmt.Errorf("%w: %v", backend.ErrOverflow, err)
			}
			b.sendError(err)
		}
	}
}

func (b *Backend) sendError(err error) {
	select {
	case b.errors <- err:
	default:
	}
}

func (b *Backend) closeNative() error {
	var err error
	b.nativeCloseOnce.Do(func() {
		b.mu.Lock()
		watcher := b.watcher
		b.mu.Unlock()
		if watcher != nil {
			err = watcher.Close()
		}
	})
	return err
}

func (b *Backend) closeOutputs() {
	b.outputCloseOnce.Do(func() {
		close(b.events)
		close(b.errors)
	})
}

func mapOp(op fsnotifylib.Op) backend.Op {
	var result backend.Op
	if op.Has(fsnotifylib.Create) {
		result |= backend.OpCreate
	}
	if op.Has(fsnotifylib.Write) {
		result |= backend.OpWrite
	}
	if op.Has(fsnotifylib.Remove) {
		result |= backend.OpRemove
	}
	if op.Has(fsnotifylib.Rename) {
		result |= backend.OpRename
	}
	if op.Has(fsnotifylib.Chmod) {
		result |= backend.OpChmod
	}
	return result
}
