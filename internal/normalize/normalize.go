// Package normalize converts platform-neutral raw operations into reconcile hints.
package normalize

import (
	"path/filepath"

	"github.com/thinhphan97/fsrecon/internal/backend"
)

type Hint struct {
	Scope     string
	Uncertain bool
}

// Event maps every raw path to its parent directory. Reconciliation needs the
// parent to establish deletion and rename semantics when the path no longer
// exists.
func Event(event backend.RawEvent, root string) (Hint, bool) {
	root = filepath.Clean(root)
	path := filepath.Clean(event.Path)
	if !within(root, path) {
		return Hint{}, false
	}
	scope := filepath.Dir(path)
	if path == root || !within(root, scope) {
		scope = root
	}
	return Hint{
		Scope:     scope,
		Uncertain: event.Op&(backend.OpRemove|backend.OpRename) != 0,
	}, true
}

func within(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." &&
		!(len(relative) > 3 && relative[:3] == ".."+string(filepath.Separator))
}
