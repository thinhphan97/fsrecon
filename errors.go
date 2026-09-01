package fsrecon

import "errors"

var (
	ErrAlreadyStarted = errors.New("fsrecon: tracker already started")
	ErrClosed         = errors.New("fsrecon: tracker is closed")
	ErrSymlink        = errors.New("fsrecon: symlink rejected by policy")
	ErrHardlink       = errors.New("fsrecon: hardlink rejected by policy")
	ErrBackendStopped = errors.New("fsrecon: native watch backend stopped")
)
