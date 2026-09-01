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
> Version 1.2 implements the complete watcher-to-reconciliation path, recursive
> watch registration, dirty-subtree batching, overflow recovery, persistent
> snapshots, explicit integrity scrubbing, and cross-platform quality gates.

## Features

- Linux, macOS, and Windows filesystem identities behind an opaque `FileID`.
- Native filesystem notifications through `fsnotify` (`inotify`, `kqueue`, and
  `ReadDirectoryChangesW`).
- Recursive, callback-based filesystem scanning with batched directory reads.
- Semantic `Created`, `Modified`, `Deleted`, `Moved`, and `Replaced` events.
- Optional expected-state reconciliation for `Missing`, `Orphan`, and
  metadata-invalid paths.
- Concurrent in-memory snapshot store and a pluggable `SnapshotStore`
  interface.
- Transactional persistent snapshots with `BoltStore` and restart recovery.
- Configurable filtering, symlink policy, and hardlink policy.
- Debounced DirtySet reconciliation that collapses overlapping subtrees.
- Explicit SHA-256 integrity scrubbing outside watcher goroutines.
- Bounded best-effort event delivery plus optional authoritative `ChangeSink`
  batches committed before snapshot advancement.
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

With `Recursive` enabled, existing and newly created directories are registered
with the native backend before their children are scanned. `ReconcileInterval`
is optional and serves only as a full-scan safety net.

## Observer

Use `NewObserver` when your application already owns authoritative metadata and
only needs bounded filesystem invalidation hints. Hints are not a complete or
ordered operation log; startup, overflow, and backend loss produce broader
subtree hints. Observer does not use snapshots, expected state, checksums, or
ChangeSink generations.
Normal changes prefer exact path hints. Directory removal emits a subtree hint
even when a filter excludes the directory; uncertain rename topology escalates
to a root subtree hint. `IgnoreSymlinks` and `ReportSymlinks` never follow
targets, `RejectSymlinks` fails topology setup, and `FollowSymlinks` is
currently unsupported for Observer. `ObserverRunning` means watch coverage is
believed complete; recovery failures transition to `ObserverDegraded`.

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
- A configured type mismatch produces `Invalid`.

Expected state defaults to a regular-file manifest: unlisted parent directories
are not classified as orphans, and an entry without `Type` expects a regular
file. Set `ExpectedEntry.Type` to explicitly expect a directory, symlink, or
other entry. Set `ExpectedScope: ExpectedAllEntries` when every unlisted
filesystem entry, including directories, should be classified as an orphan.

Providers with large manifests can additionally implement
`ScopedExpectedProvider`; dirty-subtree reconciliation uses it without walking
unrelated expected entries. Existing `ExpectedProvider` implementations remain
valid as a full-walk fallback.

`Fingerprint` is evaluated by `Tracker.Scrub` when an integrity checker is
configured. Metadata reconciliation never hashes file contents.

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

`CORRUPT` is emitted by an explicit integrity scrub. `OVERFLOW` and
`RESCAN_REQUIRED` are emitted when native event history becomes unreliable,
followed by a full reconciliation.

Every event includes its semantic kind, current path, optional old path,
optional before/after state, source, and observation time. Native OS event
types are never exposed.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Root` | required | File or directory to reconcile. It is normalized to an absolute path. |
| `Recursive` | `false` | Traverse descendants instead of only immediate children. |
| `Expected` | `nil` | Optional expected-state provider. |
| `ExpectedScope` | `ExpectedRegularFiles` | Restrict orphan detection to regular files, or include every entry. |
| `ChangeSink` | `nil` | Optional authoritative, at-least-once reconciliation delivery. |
| `Integrity` | `nil` | Optional checker used only by explicit `Scrub` calls. |
| `Store` | `MemoryStore` | Snapshot store used across reconciliations. |
| `Filter` | `nil` | Return `true` for paths that should be tracked. Filtering a directory prunes its subtree. |
| `SymlinkPolicy` | `IgnoreSymlinks` | Ignore, report, follow, or reject symbolic links. |
| `HardlinkPolicy` | `AllowHardlinks` | Allow, report as invalid, or reject regular files with multiple links. |
| `DebounceWindow` | `100ms` | Quiet period used to coalesce raw notifications and dirty subtrees. |
| `ReconcileInterval` | disabled | Optional safety interval between full reconciliations. Native root events remain active when disabled. |
| `EventBuffer` | `256` | Capacity of the public semantic-event channel. |
| `ReportEventLimit` | `10000` | Maximum detailed events retained by one report; counters remain complete. |
| `ChangeBatchSize` | `256` | Maximum events passed to one `ChangeSink` call. |

The safe default is to ignore symlinks. Tracker can use `FollowSymlinks` only
when traversal outside the lexical root is acceptable. Observer intentionally
rejects `FollowSymlinks` until cycle-safe directory traversal is available.

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

Stores may implement `ScopedSnapshotStore` for an optimized subtree walk.
`MemoryStore` uses an in-memory path trie, while `BoltStore` seeks ordered keys
with a B+Tree cursor. The base `SnapshotStore.Walk` method remains the fallback.

`FileID` supports text marshaling so an opaque identity can be persisted without
exposing inode, device, volume, or Windows file-index fields through the API.

For durable state, use the included transactional store:

```go
store, err := fsrecon.OpenBoltStore("fsrecon.db")
if err != nil {
	return err
}
defer store.Close()

tracker, err := fsrecon.New(fsrecon.Config{Root: "/data", Store: store})
```

## Integrity scrubbing

Content verification is explicit and never runs on the native event-reader
goroutine:

```go
tracker, err := fsrecon.New(fsrecon.Config{
	Root:      "/data",
	Recursive: true,
	Expected:  manifest,
	Integrity: fsrecon.SHA256Checker{},
})
if err != nil {
	return err
}

report, err := tracker.Scrub(ctx)
```

Expected SHA-256 values belong in `ExpectedEntry.Fingerprint`. A mismatch emits
`CORRUPT` with `SourceIntegrity`. Integrity event detail is bounded by
`ReportEventLimit`; `IntegrityReport.EventsTruncated` exposes omitted detail
while `Corrupt` remains exact.

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

native backend stops -> DEGRADED -> reconciliation -> DEGRADED

Any active state -> STOPPED
```

See [Consistency model](docs/consistency.md) and
[Architecture](docs/architecture.md) for details. The public compatibility
policy is documented in [Versioning](docs/versioning.md).

## Backpressure and statistics

`Events()` is a bounded best-effort convenience stream. A slow consumer does
not block reconciliation; excess channel events increment
`PublicEventsDropped` (and the compatibility alias `EventsDropped`). Report
truncation is separate: `ReconcileReport.EventsTruncated` and
`Stats.ReportEventsTruncated` count detail omitted after `ReportEventLimit`,
while aggregate counters remain complete.

Applications requiring authoritative background delivery should configure a
`ChangeSink`. Batches are identified by `(SessionID, Generation, Sequence)` and
`Final` marks the last batch. `SessionID` is random and stable for one tracker
lifetime; a new tracker gets a new session, so generation is only monotonic
within a session. All batches must succeed before fsrecon advances its
snapshot. Sink failures leave the tracker dirty and the next reconciliation
retries the same semantic diff with the same batch identity. Delivery is
at-least-once, so sinks must be idempotent. Once a batch is first attempted,
its identity always refers to the same immutable payload; retries resend the
staged generation rather than recomputing it from a changed filesystem.
Reconcile and Scrub share one authoritative coordinator, so only one pending
generation can exist and a new operation resumes it before allocating another.

```go
stats := tracker.Stats()
fmt.Printf(
	"scanned=%d reconciliations=%d public_dropped=%d report_truncated=%d queue=%d\n",
	stats.FilesScanned,
	stats.Reconciliations,
	stats.PublicEventsDropped,
	stats.ReportEventsTruncated,
	stats.QueueDepth,
)
```

The event stream and retained report detail are not durable logs. `ChangeSink`
is the reliable background-consumption contract.

## CLI

The CLI supports native watching and one-shot reconciliation:

```bash
go run ./cmd/fsrecon watch /data
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
| M6 — Recursive watch tree | Implemented |
| M7 — Normalization, debounce and coalescing | Implemented |
| M8 — Dirty subtree reconciliation | Implemented |
| M9 — Overflow and backpressure recovery | Implemented |
| M10 — Persistent snapshot state | Implemented (`BoltStore`) |
| M11 — Integrity extension | Implemented (`Scrub`, SHA-256) |
| M12 — Cross-platform hardening | Implemented with semantic integration tests |
| M13 — Benchmarks and performance baseline | Implemented |
| Observer — Lightweight invalidation hints | Implemented; see [Observer guide](docs/observer.md) |

Current scalability characteristics:

- The scanner reads each directory in batches of 1024 and does not create a
  scanner-owned `[]FileState` or a cycle map unless symlinks are followed.
- Partial reconciliation uses O(K) working memory. `MemoryStore` full passes use
  O(N) comparison maps but scoped walks use an O(depth + K) path trie;
  `BoltStore` full passes spill comparison indexes to temporary
  disk and keep report detail bounded by `ReportEventLimit`.
- Queues exposed by the tracker are bounded.
- Content hashing is not performed during metadata reconciliation.

See [Scalability](docs/scalability.md), [Benchmarks](docs/benchmarks.md), and
[Watch backends](docs/backends.md).

## Development

Run the standard quality gates:

```bash
make check
make test-race
make integration
make bench
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
├── internal/dirtyset/    prefix-trie dirty subtree collapse
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
