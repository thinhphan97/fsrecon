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
	if s == HintSubtree {
		return "SUBTREE"
	}
	return "PATH"
}

type HintCause uint8

const (
	HintNativeChange HintCause = iota
	HintStartup
	HintOverflow
	HintBackendStopped
)

func (c HintCause) String() string {
	return [...]string{"NATIVE_CHANGE", "STARTUP", "OVERFLOW", "BACKEND_STOPPED"}[minInt(int(c), 3)]
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

type ObserverStats struct {
	NativeEventsReceived uint64
	HintsEmitted         uint64
	HintsCoalesced       uint64
	OverflowCount        uint64
	PendingHints         uint64
	PublicHintsDropped   uint64
	WatchedDirectories   uint64
}
type observerStatsAtomic struct{ received, emitted, coalesced, overflows, dropped atomic.Uint64 }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
