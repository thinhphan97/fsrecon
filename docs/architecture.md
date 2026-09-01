# Architecture

The public package owns configuration, lifecycle, semantic events, reports,
expected-state and snapshot-store contracts. Platform and algorithm details
remain under `internal/`.

The implemented event path is:

```text
Kernel -> fsnotify -> always-running collector -> normalize/debounce -> DirtySet
                                                                     |
                                                                     v
                                                           reconcile worker
                                                                     |
SnapshotStore -> scoped previous state -------------------------------+
Filesystem -> batched streaming scanner -> actual state -------------+-> semantic diff
ExpectedProvider/ScopedExpectedProvider ------------------------------+       |
                                                                             v
                                                            ChangeSink batches
                                                                             |
                                                                    snapshot commit
```

The scanner obtains an opaque identity from `(device, inode)` on Linux/macOS
and `(volume serial, file index)` on Windows. Reconciliation uses identity only
when both sides have a non-zero value. Ambiguous identities, including multiple
hardlinks, are not guessed as moves.

Native events trigger reconciliation of collapsed dirty subtrees. A periodic
pass or overflow triggers full-root reconciliation. Recursive watch
registration, normalization and coalescing only make the path more selective;
they do not replace reconciliation as the source of truth.

The collector and reconciliation worker are separate. A long scan cannot stop
the collector from draining native events; new scopes remain in the bounded,
hierarchy-collapsed DirtySet and schedule a subsequent pass. Only one
reconciliation mutates a snapshot at a time.

`Events()` is best-effort. When `ChangeSink` is configured, the complete diff
is delivered in bounded generation batches before snapshot commit. Memory
reconciliation streams the diff directly; Bolt reconciliation stages changes
in its temporary database so a failed scan cannot expose a partial generation.
