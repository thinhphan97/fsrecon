# Consistency model

`SnapshotStore` records the last scan that fsrecon successfully applied. A
reconciliation scans current filesystem metadata, reads optional expected state,
computes semantic changes, then applies the new snapshot.

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

Raw events are normalized to parent-directory scopes, coalesced through the
debounce window, and collapsed by DirtySet before reconciliation. Moves across
two dirty subtrees are compared in one batch so identity-based `Moved` semantics
are preserved.

The public event channel is bounded and is not a durable change log. If it
fills, `Stats.EventsDropped` increases. Applications that require durable
delivery must persist `ReconcileReport` results in their own domain.
