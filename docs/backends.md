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

With `Recursive` enabled, the watch tree registers each directory before its
children are scanned. New directories are watched before their contents are
traversed, and watches below deleted subtrees are removed after reconciliation.
The optional `ReconcileInterval` remains a safety net for kernel queue loss and
changes made while the process was stopped.
