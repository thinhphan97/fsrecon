# Scalability

The scanner is streaming and uses traversal depth plus directory-entry buffers
rather than retaining a scanner-owned `[]FileState`. DirtySet uses a prefix trie
and partial reconciliation retains O(K) state for the union of dirty regions.
With `MemoryStore`, full reconciliation retains O(N) comparison indexes. With
`BoltStore`, full reconciliation spills previous/actual/identity indexes into a
temporary bbolt database, bounding Go heap growth while using O(N) temporary
disk. `ReportEventLimit` bounds retained event detail independently of aggregate
report counters.

All public event channels are bounded. If the consumer falls behind, semantic
events may be dropped and `Stats.EventsDropped` increases; state can be rebuilt
with `Reconcile`. Native watcher queue overflow will likewise mark state dirty
and schedule a full reconciliation.

Target measurements include files scanned per second, bytes of RAM per tracked
entry, startup duration, idle CPU, event latency and recovery throughput.

See [benchmarks](benchmarks.md) for commands and the published baseline.
