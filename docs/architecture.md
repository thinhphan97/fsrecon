# Architecture

The public package owns configuration, lifecycle, semantic events, reports,
expected-state and snapshot-store contracts. Platform and algorithm details
remain under `internal/`.

The implemented M5 path is:

```text
Kernel -> fsnotify backend -> raw hint -----------+
                                                    |
SnapshotStore -------------------------------------+
                                                    |
Filesystem -> streaming scanner -> actual state -> semantic diff -> events
                  |                    ^
ExpectedProvider -+--------------------+
```

The scanner obtains an opaque identity from `(device, inode)` on Linux/macOS
and `(volume serial, file index)` on Windows. Reconciliation uses identity only
when both sides have a non-zero value. Ambiguous identities, including multiple
hardlinks, are not guessed as moves.

Native events now trigger full reconciliation of the configured root. The M6+
recursive watch tree, normalization, DirtySet and partial reconciliation will
make this path more selective; they will not replace reconciliation as the
source of truth.
