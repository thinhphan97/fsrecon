# Observer mode

`Observer` is the lightweight mode for applications that already own the
authoritative file metadata. It watches paths and emits invalidation hints; it
does not maintain a snapshot, expected manifest, checksum state, or semantic
filesystem diff.

```go
observer, err := fsrecon.NewObserver(fsrecon.ObserverConfig{
	Root: "/data", Recursive: true,
		Filter: func(path string) bool { return strings.HasSuffix(path, ".chunk") },
})
if err != nil { return err }
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
if err := observer.Start(ctx); err != nil { return err }
defer observer.Close()
for hint := range observer.Hints() {
	// Revalidate hint.Path or the complete hint subtree in application state.
	log.Printf("invalidate %s (%s, %s)", hint.Path, hint.Scope, hint.Cause)
}
```

Normal file changes prefer `HintPath`. Directory creation/removal and uncertain
rename topology use `HintSubtree`; startup and overflow always invalidate the
root subtree. Hints are coalesced, may be duplicated, and are not an ordered
operation log. Filters apply only to ordinary exact-path hints and never hide
topology or uncertainty hints.

`ObserverRunning` means the configured watch topology is believed complete. A
backend stop, failed watch registration/removal, failed topology resync, or
root removal/rename transitions to `ObserverDegraded` and emits a root
subtree hint with `HintBackendStopped`.

Recursive startup and overflow recovery enumerate directories only. Observer
does not retain per-file `FileState` entries. `IgnoreSymlinks` and
`ReportSymlinks` do not follow targets, `RejectSymlinks` rejects topology
symlinks, and `FollowSymlinks` is currently unsupported for Observer.

`HintBuffer` bounds the public channel and `MaxPendingHints` bounds retained
precise invalidations. Under pressure, precise paths are replaced by a safe
root subtree invalidation. `HintDeliveryDeferred` counts nonblocking sends
that were deferred because the public channel was full; pending invalidation is
retained for later delivery.
