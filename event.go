package fsrecon

import (
	"context"
	"time"
)

// EventKind describes a semantic filesystem change.
type EventKind uint8

const (
	EventCreated EventKind = iota
	EventModified
	EventDeleted
	EventMoved
	EventAttributeChanged
	EventMissing
	EventOrphan
	EventReplaced
	EventInvalid
	EventCorrupt
	EventOverflow
	EventRescanRequired
)

// String returns the stable uppercase event name.
func (k EventKind) String() string {
	names := [...]string{
		"CREATED", "MODIFIED", "DELETED", "MOVED", "ATTRIBUTE_CHANGED",
		"MISSING", "ORPHAN", "REPLACED", "INVALID", "CORRUPT", "OVERFLOW", "RESCAN_REQUIRED",
	}
	if int(k) >= len(names) {
		return "UNKNOWN"
	}
	return names[k]
}

// EventSource identifies the subsystem that established an event.
type EventSource uint8

const (
	SourceWatcher EventSource = iota
	SourceReconcile
	SourceIntegrity
)

// String returns the stable uppercase source name.
func (s EventSource) String() string {
	names := [...]string{"WATCHER", "RECONCILE", "INTEGRITY"}
	if int(s) >= len(names) {
		return "UNKNOWN"
	}
	return names[s]
}

// Event is a normalized, application-agnostic filesystem change.
type Event struct {
	Kind    EventKind
	Path    string
	OldPath string
	Before  *FileState
	After   *FileState
	Source  EventSource
	Time    time.Time
}

// ChangeBatch is one bounded part of an authoritative reconciliation result.
// All batches in an attempt share a Generation and have increasing Sequence
// values starting at zero. Final marks the last batch in that generation.
// A failed reconciliation may retry the same generation and sequences, so a
// sink must apply them idempotently and only finalize a generation on Final.
type ChangeBatch struct {
	SessionID  string
	Generation uint64
	Sequence   uint64
	Final      bool
	Events     []Event
}

// ChangeSink receives authoritative reconciliation and integrity changes.
// ApplyChanges is called outside the native event collector. Returning an
// error prevents metadata snapshot advancement for reconciliation changes.
type ChangeSink interface {
	ApplyChanges(ctx context.Context, batch ChangeBatch) error
}
