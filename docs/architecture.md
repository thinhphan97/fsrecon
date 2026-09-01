# Architecture

The public package owns configuration, lifecycle, semantic events, reports,
expected-state and snapshot-store contracts. Platform and algorithm details
remain under `internal/`.

The implemented event path is:

```text
Kernel -> fsnotify -> normalize -> debounce -> DirtySet --+
                                                           |
SnapshotStore ---------------------------------------------+
                                                           |
Filesystem -> streaming scanner -> scoped actual state -> semantic diff -> events
                  |                              ^
ExpectedProvider -+------------------------------+
```

The scanner obtains an opaque identity from `(device, inode)` on Linux/macOS
and `(volume serial, file index)` on Windows. Reconciliation uses identity only
when both sides have a non-zero value. Ambiguous identities, including multiple
hardlinks, are not guessed as moves.

Native events trigger reconciliation of collapsed dirty subtrees. A periodic
pass or overflow triggers full-root reconciliation. Recursive watch
registration, normalization and coalescing only make the path more selective;
they do not replace reconciliation as the source of truth.
