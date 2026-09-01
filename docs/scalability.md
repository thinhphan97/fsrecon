# Scalability

The scanner itself is streaming and uses traversal depth plus directory-entry
buffers rather than retaining every `FileState`. The v0 reconciler and
`MemoryStore` currently retain O(N) state to perform identity-based move
matching. A persistent store and external indexes are planned before claiming
bounded-memory operation at multi-million-entry scale.

All public event channels are bounded. If the consumer falls behind, semantic
events may be dropped and `Stats.EventsDropped` increases; state can be rebuilt
with `Reconcile`. Native watcher queue overflow will likewise mark state dirty
and schedule a full reconciliation.

Target measurements include files scanned per second, bytes of RAM per tracked
entry, startup duration, idle CPU, event latency and recovery throughput.
