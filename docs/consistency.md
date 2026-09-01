# Consistency model

`SnapshotStore` records the last scan that fsrecon successfully applied. A
reconciliation scans current filesystem metadata, reads optional expected state,
computes semantic changes, then applies the new snapshot.

When `ChangeSink` is configured, the ordering is:

```text
scan -> complete diff -> deliver all ChangeBatch values -> commit snapshot
```

A sink error prevents snapshot commit, leaves the tracker dirty, and allows the
same changes to be regenerated. Delivery is at-least-once: a store failure
after successful delivery can cause the same batch to be retried, so sinks must
be idempotent and use `(SessionID, Generation, Sequence)` as the identity and
`Final` as the generation boundary. Session IDs are regenerated on tracker
restart; generation is monotonic only within one session.

Watcher delivery is not a correctness boundary. Each native notification
triggers reconciliation rather than being translated directly into a public
semantic event. Lost, coalesced or overflowing notifications cause a full
reconciliation. An optional periodic reconciliation remains available as a
safety net.

Filesystem scans are not atomic. Entries may change during traversal. A path
that disappears between directory enumeration and metadata lookup is skipped
and will be resolved by comparison or a subsequent pass. Other I/O and
permission failures fail the reconciliation and leave the tracker `DIRTY`.

Stores may implement `BatchSnapshotStore` to apply one reconciliation
atomically. `MemoryStore` and `BoltStore` implement this extension. A custom
store that only implements `SnapshotStore` is updated entry by entry; an error
can leave a partial snapshot, and the next full reconciliation restores
convergence.

Raw events are drained by a dedicated collector, normalized to
parent-directory scopes, coalesced through the
debounce window, and collapsed by DirtySet before reconciliation. Moves across
two dirty subtrees are compared in one batch so identity-based `Moved` semantics
are preserved.

The public event channel is bounded and is not a durable change log. If it
fills, `Stats.PublicEventsDropped` and its compatibility alias `EventsDropped`
increase. Report truncation is independently visible through
`EventsTruncated` and `Stats.ReportEventsTruncated`; neither affects
`ChangeSink` delivery. Integrity scrub corruption events use the same
authoritative sink and bounded batching. Scrub does not modify the metadata
snapshot; sink failure returns an error and leaves it unchanged.

If the native event channel closes unexpectedly, the tracker enters
`StateDegraded`. Reconciliation can establish current filesystem truth but does
not restore continuous observation, so the state remains degraded. The current
implementation does not automatically restart a dead backend; create a new
tracker to restore observation. With `ReconcileInterval == 0`, callers must not
treat a degraded tracker as continuously monitored.

Expected state defaults to regular-file-manifest semantics. Unlisted actual
directories are not orphans, while a typeless expected entry implies a regular
file. Explicit `ExpectedEntry.Type` constraints can reconcile directories and
other entry types; `ExpectedAllEntries` enables orphan detection for all types.
