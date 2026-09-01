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

SnapshotStore has no transaction contract in v0. A store error during apply can
leave a partially updated custom store; the next full reconciliation restores
convergence. Persistent stores should add their own atomic batch facility in a
future optional interface.
