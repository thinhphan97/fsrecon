# Watch backends

M5 uses `github.com/fsnotify/fsnotify` behind an internal backend interface:

- Linux: inotify
- macOS: kqueue
- Windows: ReadDirectoryChangesW

Raw backend events will never be exposed publicly. Tests assert eventual
semantic state instead of platform-specific event sequences.

The tracker registers the configured root before its initial scan. Events that
occur during that scan remain buffered and trigger another reconciliation,
closing the scan-then-watch startup race.

At M5 only the configured root is registered. Watching all existing and newly
created descendant directories is M6. `Recursive` already controls scanning,
but nested changes need `ReconcileInterval` as a safety net until the recursive
watch tree is implemented.
