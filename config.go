package fsrecon

import "time"

// FilterFunc returns true when an entry should be tracked.
type FilterFunc func(path string, info FileState) bool

// SymlinkPolicy controls how scanning treats symbolic links.
type SymlinkPolicy uint8

const (
	IgnoreSymlinks SymlinkPolicy = iota
	ReportSymlinks
	FollowSymlinks
	RejectSymlinks
)

// HardlinkPolicy controls how regular files with multiple links are handled.
type HardlinkPolicy uint8

const (
	AllowHardlinks HardlinkPolicy = iota
	ReportHardlinks
	RejectHardlinks
)

// Config controls a Tracker. Root is the only required field.
type Config struct {
	Root              string
	Recursive         bool
	Expected          ExpectedProvider
	Integrity         IntegrityChecker
	Store             SnapshotStore
	Filter            FilterFunc
	SymlinkPolicy     SymlinkPolicy
	HardlinkPolicy    HardlinkPolicy
	DebounceWindow    time.Duration
	ReconcileInterval time.Duration
	EventBuffer       int
	ReportEventLimit  int
}

const defaultEventBuffer = 256
const defaultDebounceWindow = 100 * time.Millisecond
const defaultReportEventLimit = 10_000
