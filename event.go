package fsrecon

import "time"

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
