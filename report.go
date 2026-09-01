package fsrecon

import "time"

// ReconcileReport summarizes one completed reconciliation.
type ReconcileReport struct {
	StartedAt time.Time
	Duration  time.Duration
	Scanned   uint64
	Healthy   uint64
	Created   uint64
	Modified  uint64
	Deleted   uint64
	Moved     uint64
	Replaced  uint64
	Missing   uint64
	Orphan    uint64
	Invalid   uint64
	Corrupt   uint64
	Events    []Event
}

// Stats contains monotonic counters and current queue gauges.
type Stats struct {
	EventsReceived  uint64
	EventsCoalesced uint64
	EventsDropped   uint64
	Reconciliations uint64
	FilesScanned    uint64
	MissingDetected uint64
	OrphansDetected uint64
	DirtyPaths      uint64
	QueueDepth      uint64
}
