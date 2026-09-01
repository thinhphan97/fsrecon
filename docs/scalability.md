# Scalability

The scanner reads directories in batches of 1024 and uses traversal depth plus
that fixed entry batch rather than retaining a directory-wide list or a
scanner-owned `[]FileState`. Its identity cycle map is allocated only for
`FollowSymlinks`. DirtySet uses a prefix trie and partial reconciliation retains
O(K) state for the union of dirty regions. `MemoryStore` uses a path trie, so a
scoped walk visits O(depth + K) indexed nodes instead of all N entries. With
`MemoryStore`, full reconciliation still retains O(N) comparison indexes. With
`BoltStore`, full reconciliation spills previous/actual/identity indexes into a
temporary bbolt database, bounding Go heap growth while using O(N) temporary
disk. `ReportEventLimit` bounds retained event detail independently of aggregate
report counters.

All public event channels and authoritative sink batches are bounded. If the
best-effort consumer falls behind, `Stats.PublicEventsDropped` increases. A
configured `ChangeSink` still receives the complete diff before snapshot
advancement, in bounded batches keyed by session, generation and sequence.
Native watcher queue overflow increments `BackendOverflows`, marks
the root dirty, and schedules a full reconciliation while the collector keeps
draining events.

Partial expected-state work is O(K) only when the provider implements
`ScopedExpectedProvider`; legacy providers remain correct but require a full
manifest walk followed by filtering. `BoltStore` and `MemoryStore` both
implement optimized `ScopedSnapshotStore` traversal.

Target measurements include files scanned per second, bytes of RAM per tracked
entry, startup duration, idle CPU, event latency and recovery throughput.

See [benchmarks](benchmarks.md) for commands and the published baseline.
