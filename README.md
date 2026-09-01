# fsrecon

[![CI](https://github.com/thinhphan97/fsrecon/actions/workflows/ci.yml/badge.svg)](https://github.com/thinhphan97/fsrecon/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/thinhphan97/fsrecon.svg)](https://pkg.go.dev/github.com/thinhphan97/fsrecon)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`fsrecon` is a cross-platform Go library for observing filesystem changes and
reconciling actual filesystem state with a previous snapshot and, optionally,
application-supplied expected state.

It is designed for storage systems, backup tools, sync engines, artifact
stores, caches, and data-integrity services that cannot treat native filesystem
notifications as a source of truth.

```text
Filesystem events (fast path) ─┐
                               ├─> Tracker ─> semantic changes
Filesystem scan (truth) ───────┤
Previous snapshot ─────────────┤
Expected state (optional) ─────┘
```

> [!IMPORTANT]
> The project is currently pre-v1. M0–M5 are implemented, including native
> kernel-backed notifications for the configured root. Recursive watch-tree
> registration is M6; nested changes still need periodic reconciliation as a
> safety net until M6 is complete.

## Features

- Linux, macOS, and Windows filesystem identities behind an opaque `FileID`.
- Native filesystem notifications through `fsnotify` (`inotify`, `kqueue`, and
  `ReadDirectoryChangesW`).
- Recursive, callback-based filesystem scanning.
- Semantic `Created`, `Modified`, `Deleted`, `Moved`, and `Replaced` events.
- Optional expected-state reconciliation for `Missing`, `Orphan`, and
  metadata-invalid paths.
- Concurrent in-memory snapshot store and a pluggable `SnapshotStore`
  interface.
- Configurable filtering, symlink policy, and hardlink policy.
- Bounded event delivery with observable drop statistics.
- Context cancellation, clean shutdown, and periodic reconciliation.
- No database, network, logging, or metrics-framework dependency in the core.

## Requirements

- Go 1.22 or newer.
- A supported target: Linux, macOS, or Windows.

## Installation

```bash
go get github.com/thinhphan97/fsrecon
```

## Quick start

The following example registers the native watcher, performs an initial scan,
and keeps a periodic full reconciliation as a safety net:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thinhphan97/fsrecon"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	tracker, err := fsrecon.New(fsrecon.Config{
		Root:              "/data",
		Recursive:         true,
		ReconcileInterval: 30 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := tracker.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer tracker.Close()

	go func() {
		for err := range tracker.Errors() {
			log.Printf("fsrecon: %v", err)
		}
	}()

	for event := range tracker.Events() {
		if event.OldPath != "" {
			fmt.Printf("%-18s %s -> %s\n", event.Kind, event.OldPath, event.Path)
			continue
		}
		fmt.Printf("%-18s %s\n", event.Kind, event.Path)
	}
}
```

`Start` registers the native watcher before scanning, so changes during startup
remain buffered and cause another reconciliation. Canceling its context or
calling `Close` stops the tracker and closes both output channels.

At M5, only `Root` itself is registered with the native backend. Direct-child
changes are kernel-driven. Recursive registration of existing and newly created
directories is M6; use `ReconcileInterval` when nested changes must be covered
in the meantime.

## Run the demo

The included demo creates a real temporary directory and performs create,
modify, rename, atomic-replace, and delete operations against it:

```bash
make demo
```

Example output:

```text
CREATED            /tmp/fsrecon-demo.XYZ/report.txt
MODIFIED           /tmp/fsrecon-demo.XYZ/report.txt
MOVED              /tmp/fsrecon-demo.XYZ/report.txt -> /tmp/fsrecon-demo.XYZ/final.txt
REPLACED            /tmp/fsrecon-demo.XYZ/final.txt
DELETED             /tmp/fsrecon-demo.XYZ/final.txt
```

The temporary directory is removed when the demo finishes. To retain it or
place it under a specific parent directory:

```bash
make demo-keep
make demo DEMO_PARENT=/path/to/directory
make demo-keep DEMO_PARENT=/path/to/directory
```

## One-shot reconciliation

Use `Reconcile` directly when background operation is not needed:

```go
tracker, err := fsrecon.New(fsrecon.Config{
	Root:      "/data",
	Recursive: true,
})
if err != nil {
	return err
}
defer tracker.Close()

report, err := tracker.Reconcile(ctx)
if err != nil {
	return err
}

fmt.Printf(
	"scanned=%d created=%d modified=%d deleted=%d moved=%d replaced=%d duration=%s\n",
	report.Scanned,
	report.Created,
	report.Modified,
	report.Deleted,
	report.Moved,
	report.Replaced,
	report.Duration,
)
```

The first reconciliation against an empty store reports existing entries as
`Created` and establishes the snapshot used by subsequent calls.

## Expected state

Implement `ExpectedProvider` to compare actual files with an application-owned
manifest. Expected paths may be relative to the configured root or absolute
paths contained by it.

```go
type Manifest []fsrecon.ExpectedEntry

func (m Manifest) WalkExpected(
	ctx context.Context,
	root string,
	emit func(fsrecon.ExpectedEntry) error,
) error {
	for _, entry := range m {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := emit(entry); err != nil {
			return err
		}
	}
	return nil
}

expectedSize := int64(4096)
manifest := Manifest{
	{Path: "objects/a.dat", Size: &expectedSize},
	{Path: "objects/b.dat"},
}

tracker, err := fsrecon.New(fsrecon.Config{
	Root:      "/data",
	Recursive: true,
	Expected:  manifest,
})
```

With an expected provider:

- Expected but absent paths produce `Missing`.
- Actual paths absent from the manifest produce `Orphan`.
- A size mismatch produces `Invalid`.

`Fingerprint` is reserved for the integrity extension and is not evaluated by
the current reconciler.

## Event semantics

| Event | Meaning |
| --- | --- |
| `CREATED` | A path exists in actual state but not in the previous snapshot. |
| `MODIFIED` | Type, size, or modification time changed at the same identity. |
| `ATTRIBUTE_CHANGED` | File mode changed without a content metadata change. |
| `DELETED` | A snapshot path no longer exists. |
| `MOVED` | A unique, non-zero filesystem identity moved to a different path. |
| `REPLACED` | A path still exists but has a different filesystem identity. |
| `MISSING` | An expected path does not exist. |
| `ORPHAN` | An actual path is absent from expected state. |
| `INVALID` | Actual metadata violates an expected constraint or policy. |

`CORRUPT`, `OVERFLOW`, and `RESCAN_REQUIRED` are part of the public model for
the integrity and watcher milestones but are not emitted by the current
implementation.

Every event includes its semantic kind, current path, optional old path,
optional before/after state, source, and observation time. Native OS event
types are never exposed.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Root` | required | File or directory to reconcile. It is normalized to an absolute path. |
| `Recursive` | `false` | Traverse descendants instead of only immediate children. |
| `Expected` | `nil` | Optional expected-state provider. |
| `Store` | `MemoryStore` | Snapshot store used across reconciliations. |
| `Filter` | `nil` | Return `true` for paths that should be tracked. Filtering a directory prunes its subtree. |
| `SymlinkPolicy` | `IgnoreSymlinks` | Ignore, report, follow, or reject symbolic links. |
| `HardlinkPolicy` | `AllowHardlinks` | Allow, report as invalid, or reject regular files with multiple links. |
| `ReconcileInterval` | disabled | Optional safety interval between full reconciliations. Native root events remain active when disabled. |
| `EventBuffer` | `256` | Capacity of the public semantic-event channel. |

The safe default is to ignore symlinks. Enable `FollowSymlinks` only when
traversal outside the lexical root is acceptable for your application.

## Snapshot stores

`MemoryStore` is suitable for development and process-lifetime tracking:

```go
store := fsrecon.NewMemoryStore()

tracker, err := fsrecon.New(fsrecon.Config{
	Root:  "/data",
	Store: store,
})
```

Applications can implement `SnapshotStore` for bbolt, Pebble, SQLite, or a
custom persistence layer:

```go
type SnapshotStore interface {
	Get(context.Context, string) (fsrecon.FileState, bool, error)
	Put(context.Context, fsrecon.FileState) error
	Delete(context.Context, string) error
	Walk(context.Context, string, func(fsrecon.FileState) error) error
}
```

`FileID` supports text marshaling so an opaque identity can be persisted without
exposing inode, device, volume, or Windows file-index fields through the API.

## Consistency model

The core rule is:

```text
watcher events are hints; reconciliation establishes truth
```

A reconciliation compares:

```text
previous snapshot vs actual filesystem -> change detection
expected state    vs actual filesystem -> state validation
```

Filesystem scanning is not an atomic snapshot. A path that disappears between
directory enumeration and metadata lookup is treated as a concurrent change;
other I/O or permission errors fail the reconciliation and move the tracker to
`DIRTY`.

Tracker lifecycle:

```text
CREATED -> STARTING -> SYNCING -> RECONCILING -> SYNCED
                                      |             |
                                      v             v
                                    DIRTY      RECONCILING

Any active state -> STOPPED
```

See [Consistency model](docs/consistency.md) and
[Architecture](docs/architecture.md) for details.

## Backpressure and statistics

The event channel is bounded. A slow consumer does not block reconciliation;
instead, excess events are dropped and recorded in `Stats`:

```go
stats := tracker.Stats()
fmt.Printf(
	"scanned=%d reconciliations=%d dropped=%d queue=%d\n",
	stats.FilesScanned,
	stats.Reconciliations,
	stats.EventsDropped,
	stats.QueueDepth,
)
```

If `EventsDropped` increases, perform or schedule reconciliation and read the
returned `ReconcileReport` for that pass. The event stream is not a durable log.

## CLI

The development CLI runs a one-shot reconciliation:

```bash
go run ./cmd/fsrecon reconcile /data
```

Example:

```text
CREATED            /data/a.dat
CREATED            /data/subdirectory
Scanned: 2  Healthy: 0  Missing: 0  Orphan: 0  Duration: 240.1µs
```

## Current implementation status

| Milestone | Status |
| --- | --- |
| M0 — Repository bootstrap | Implemented |
| M1 — Core API and contracts | Implemented |
| M2 — Streaming filesystem scanner | Implemented |
| M3 — Memory snapshot store | Implemented |
| M4 — Reconciliation engine | Implemented |
| M5 — Native watcher backend | Implemented |
| M6 — Recursive watch tree | Next |
| M7+ — Normalization, DirtySet, overflow hardening | Planned |
| Persistent stores and integrity checking | Planned |

Current scalability characteristics:

- The scanner streams entries and does not create a scanner-owned `[]FileState`.
- The current reconciliation engine builds in-memory indexes for identity matching,
  so a full pass currently uses O(N) memory.
- Queues exposed by the tracker are bounded.
- Content hashing is not performed during metadata reconciliation.

See [Scalability](docs/scalability.md) and [Watch backends](docs/backends.md).

## Development

Run the standard quality gates:

```bash
make check
make test-race
```

Equivalent commands:

```bash
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
```

CI executes formatting, vet, unit tests, and race-enabled tests on Linux,
macOS, and Windows.

## Project layout

```text
.
├── cmd/fsrecon/          development CLI
├── docs/                 architecture and consistency documentation
├── examples/             watcher, reconciliation, and expected-state examples
├── internal/backend/     native event abstraction and fsnotify adapter
├── internal/reconcile/   semantic diff engine
├── internal/scanner/     filesystem traversal and platform identity
├── scripts/demo.sh       executable filesystem demo
└── *.go                  small public package surface
```

## Design principles

1. Watcher events are hints, not truth.
2. Lost event history must be recoverable through reconciliation.
3. Unbounded queues are forbidden.
4. Filesystem scanning must support streaming.
5. Full reconciliation does not imply full content hashing.
6. Application-specific business semantics do not enter the core.
7. Expected state and observed state remain separate.
8. Uncertainty triggers reconciliation, not guessing.
9. Native OS events never leak into the public API.
10. Correctness takes priority over preserving raw event history.

## License

`fsrecon` is available under the [MIT License](LICENSE).
