package fsrecon

import "time"

// FilterFunc returns true when an entry should be tracked.
type FilterFunc func(path string, info FileState) bool

type SymlinkPolicy uint8

const (
	IgnoreSymlinks SymlinkPolicy = iota
	ReportSymlinks
	FollowSymlinks
	RejectSymlinks
)

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
	Store             SnapshotStore
	Filter            FilterFunc
	SymlinkPolicy     SymlinkPolicy
	HardlinkPolicy    HardlinkPolicy
	ReconcileInterval time.Duration
	EventBuffer       int
}

const defaultEventBuffer = 256
