package fsrecon

import "context"

// ExpectedProvider streams the application-supplied desired filesystem state.
type ExpectedProvider interface {
	WalkExpected(ctx context.Context, root string, fn func(ExpectedEntry) error) error
}

// ExpectedEntry describes optional constraints for an expected path.
// Paths may be absolute under Config.Root or relative to it.
type ExpectedEntry struct {
	Path        string
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
