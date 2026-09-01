package fsrecon

import (
	"sync/atomic"
	"time"
)

type PathFilter func(path string) bool
type ObserverConfig struct {
	Root            string
	Recursive       bool
	DebounceWindow  time.Duration
	HintBuffer      int
	MaxPendingHints int
	Filter          PathFilter
	SymlinkPolicy   SymlinkPolicy
}
type HintScope uint8

const (
	HintPath HintScope = iota
	HintSubtree
)

func (s HintScope) String() string {
	switch s {
	case HintPath:
		return "PATH"
	case HintSubtree:
		return "SUBTREE"
	}
	return "UNKNOWN"
}

type HintCause uint8

const (
	HintNativeChange HintCause = iota
	HintStartup
	HintOverflow
	HintBackendStopped
)

func (c HintCause) String() string {
	switch c {
	case HintNativeChange:
		return "NATIVE_CHANGE"
	case HintStartup:
		return "STARTUP"
	case HintOverflow:
		return "OVERFLOW"
	case HintBackendStopped:
		return "BACKEND_STOPPED"
	}
	return "UNKNOWN"
}

type Hint struct {
	Path  string
	Scope HintScope
	Cause HintCause
	Time  time.Time
}
type ObserverState uint8

const (
	ObserverCreated ObserverState = iota
	ObserverStarting
	ObserverRunning
	ObserverDegraded
	ObserverStopped
)

func (s ObserverState) String() string {
	switch s {
	case ObserverCreated:
		return "CREATED"
	case ObserverStarting:
		return "STARTING"
	case ObserverRunning:
		return "RUNNING"
	case ObserverDegraded:
		return "DEGRADED"
	case ObserverStopped:
		return "STOPPED"
	}
	return "UNKNOWN"
}

type ObserverStats struct {
	NativeEventsReceived uint64
	HintsEmitted         uint64
	HintsCoalesced       uint64
	OverflowCount        uint64
	PendingHints         uint64
	HintDeliveryDeferred uint64
	WatchedDirectories   uint64
}
type observerStatsAtomic struct{ received, emitted, coalesced, overflows, dropped atomic.Uint64 }
