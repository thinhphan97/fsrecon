package normalize

import (
	"path/filepath"
	"testing"

	"github.com/thinhphan97/fsrecon/internal/backend"
)

func TestEventUsesParentScopeAndRejectsOutside(t *testing.T) {
	root := filepath.Clean(filepath.Join("root", "data"))
	path := filepath.Join(root, "a", "file")
	hint, ok := Event(backend.RawEvent{Path: path, Op: backend.OpRename}, root)
	if !ok || hint.Scope != filepath.Dir(path) || !hint.Uncertain {
		t.Fatalf("hint = %+v, ok = %v", hint, ok)
	}
	if _, ok := Event(backend.RawEvent{Path: filepath.Join("root", "database")}, root); ok {
		t.Fatal("outside path accepted")
	}
}
