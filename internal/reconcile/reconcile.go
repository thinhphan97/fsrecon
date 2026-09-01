// Package reconcile computes semantic differences between filesystem states.
package reconcile

import "sort"

type State struct {
	Path    string
	ID      string
	Type    FileType
	Size    int64
	ModUnix int64
	Mode    uint32
}

type Expected struct {
	Path        string
	Type        *FileType
	Size        *int64
	Fingerprint []byte
}

type FileType uint8

const (
	TypeUnknown FileType = iota
	TypeRegular
	TypeDirectory
	TypeSymlink
	TypeOther
)

type Kind uint8

const (
	Created Kind = iota
	Modified
	Deleted
	Moved
	AttributeChanged
	Missing
	Orphan
	Replaced
	Invalid
)

type Change struct {
	Kind    Kind
	Path    string
	OldPath string
	Before  *State
	After   *State
}

// Diff returns changes in stable path order. Expected must be nil when expected
// state reconciliation is disabled; an empty non-nil map means nothing is
// expected and therefore every actual entry is an orphan.
func Diff(previous, actual map[string]State, expected map[string]Expected) []Change {
	changes := make([]Change, 0)
	WalkDiff(previous, actual, expected, func(change Change) error {
		changes = append(changes, change)
		return nil
	})
	return changes
}

// WalkDiff streams changes in stable path order without retaining the complete
// result. The input maps are not modified.
func WalkDiff(previous, actual map[string]State, expected map[string]Expected, emit func(Change) error) error {
	return WalkDiffScoped(previous, actual, expected, nil, emit)
}

// WalkDiffScoped is WalkDiff with an optional predicate limiting which
// unlisted actual entries participate in expected-state orphan detection.
func WalkDiffScoped(previous, actual map[string]State, expected map[string]Expected, orphanEligible func(State) bool, emit func(Change) error) error {
	remainingPrevious := clone(previous)
	remainingActual := clone(actual)

	for _, path := range sortedKeys(actual) {
		before, existed := previous[path]
		if !existed {
			continue
		}
		after := actual[path]
		delete(remainingPrevious, path)
		delete(remainingActual, path)
		switch {
		case !sameIdentity(before, after):
			if err := emit(change(Replaced, path, "", &before, &after)); err != nil {
				return err
			}
		case contentChanged(before, after):
			if err := emit(change(Modified, path, "", &before, &after)); err != nil {
				return err
			}
		case before.Mode != after.Mode:
			if err := emit(change(AttributeChanged, path, "", &before, &after)); err != nil {
				return err
			}
		}
	}

	prevIDs := indexUniqueIDs(remainingPrevious)
	actualIDs := indexUniqueIDs(remainingActual)
	ids := sortedKeys(actualIDs)
	for _, id := range ids {
		oldPath, oldOK := prevIDs[id]
		newPath := actualIDs[id]
		if !oldOK || id == "" {
			continue
		}
		before := remainingPrevious[oldPath]
		after := remainingActual[newPath]
		if err := emit(change(Moved, newPath, oldPath, &before, &after)); err != nil {
			return err
		}
		delete(remainingPrevious, oldPath)
		delete(remainingActual, newPath)
	}
	for _, path := range sortedKeys(remainingActual) {
		after := remainingActual[path]
		if err := emit(change(Created, path, "", nil, &after)); err != nil {
			return err
		}
	}
	for _, path := range sortedKeys(remainingPrevious) {
		before := remainingPrevious[path]
		if err := emit(change(Deleted, path, "", &before, nil)); err != nil {
			return err
		}
	}

	if expected != nil {
		for _, path := range sortedKeys(expected) {
			exp := expected[path]
			state, ok := actual[path]
			if !ok {
				if err := emit(Change{Kind: Missing, Path: path}); err != nil {
					return err
				}
				continue
			}
			if exp.Type != nil && state.Type != *exp.Type || exp.Size != nil && state.Size != *exp.Size {
				if err := emit(change(Invalid, path, "", nil, &state)); err != nil {
					return err
				}
			}
		}
		for _, path := range sortedKeys(actual) {
			if orphanEligible != nil && !orphanEligible(actual[path]) {
				continue
			}
			if _, ok := expected[path]; !ok {
				state := actual[path]
				if err := emit(change(Orphan, path, "", nil, &state)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func change(kind Kind, path, oldPath string, before, after *State) Change {
	return Change{Kind: kind, Path: path, OldPath: oldPath, Before: before, After: after}
}

func sameIdentity(a, b State) bool {
	if a.ID == "" || b.ID == "" {
		return true
	}
	return a.ID == b.ID
}

func contentChanged(a, b State) bool {
	return a.Type != b.Type || a.Size != b.Size || a.ModUnix != b.ModUnix
}

func clone(input map[string]State) map[string]State {
	out := make(map[string]State, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func indexUniqueIDs(states map[string]State) map[string]string {
	result := make(map[string]string)
	duplicates := make(map[string]struct{})
	for path, state := range states {
		if state.ID == "" {
			continue
		}
		if _, exists := result[state.ID]; exists {
			duplicates[state.ID] = struct{}{}
		} else {
			result[state.ID] = path
		}
	}
	for id := range duplicates {
		delete(result, id)
	}
	return result
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
