package fsrecon

import "time"

// ReconcileReport summarizes one completed reconciliation.
type ReconcileReport struct {
	Generation uint64
	StartedAt  time.Time
	Duration   time.Duration
	Scanned    uint64
	Healthy    uint64
	Created    uint64
	Modified   uint64
	Deleted    uint64
	Moved      uint64
	Replaced   uint64
	Missing    uint64
	Orphan     uint64
	Invalid    uint64
	Corrupt    uint64
	Events     []Event
	// EventsTruncated counts semantic events omitted from Events after the
	// configured ReportEventLimit. Aggregate counters still include them.
	EventsTruncated uint64
}

// IntegrityReport summarizes one explicit content scrub.
type IntegrityReport struct {
	Generation uint64
	StartedAt  time.Time
	Duration   time.Duration
	Scanned    uint64
	Healthy    uint64
	Corrupt    uint64
	Events     []Event
	// EventsTruncated counts corruption events omitted from Events after the
	// configured ReportEventLimit. Aggregate counters still include them.
	EventsTruncated uint64
}

// Stats contains monotonic counters and current queue gauges.
type Stats struct {
	EventsReceived  uint64
	EventsCoalesced uint64
	// EventsDropped is retained for compatibility and has the same value as
	// PublicEventsDropped. It no longer includes report truncation.
	EventsDropped         uint64
	PublicEventsDropped   uint64
	ReportEventsTruncated uint64
	BackendOverflows      uint64
	Reconciliations       uint64
	FilesScanned          uint64
	MissingDetected       uint64
	OrphansDetected       uint64
	DirtyPaths            uint64
	QueueDepth            uint64
	IntegrityScanned      uint64
	CorruptDetected       uint64
}
