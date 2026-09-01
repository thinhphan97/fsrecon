package fsrecon

import "context"

// ExpectedEntryScope controls which unlisted actual entries are classified as
// orphans. The zero value models the common regular-file manifest use case.
type ExpectedEntryScope uint8

const (
	ExpectedRegularFiles ExpectedEntryScope = iota
	ExpectedAllEntries
)

// ExpectedProvider streams the application-supplied desired filesystem state.
type ExpectedProvider interface {
	WalkExpected(ctx context.Context, root string, fn func(ExpectedEntry) error) error
}

// ScopedExpectedProvider optionally streams only entries within scope. Scope
// is an absolute cleaned path below root. Tracker falls back to
// ExpectedProvider.WalkExpected when this interface is not implemented.
type ScopedExpectedProvider interface {
	ExpectedProvider
	WalkExpectedScope(ctx context.Context, root, scope string, fn func(ExpectedEntry) error) error
}

// ExpectedEntry describes optional constraints for an expected path.
// Paths may be absolute under Config.Root or relative to it.
type ExpectedEntry struct {
	Path        string
	Type        *FileType
	Size        *int64
	Fingerprint []byte
}

// IntegrityChecker optionally verifies file contents outside watcher threads.
type IntegrityChecker interface {
	Check(ctx context.Context, state FileState) (IntegrityResult, error)
}

// IntegrityResult is the result of an integrity check.
type IntegrityResult struct {
	Valid       bool
	Fingerprint []byte
	Reason      string
}
