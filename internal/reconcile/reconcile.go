// Package reconcile computes semantic differences between filesystem states.
package reconcile

import "sort"

type State struct {
	Path    string
	ID      string
	Type    uint8
	Size    int64
	ModUnix int64
	Mode    uint32
}

type Expected struct {
	Path        string
	Size        *int64
	Fingerprint []byte
}

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
			changes = append(changes, change(Replaced, path, "", &before, &after))
		case contentChanged(before, after):
			changes = append(changes, change(Modified, path, "", &before, &after))
		case before.Mode != after.Mode:
			changes = append(changes, change(AttributeChanged, path, "", &before, &after))
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
		changes = append(changes, change(Moved, newPath, oldPath, &before, &after))
		delete(remainingPrevious, oldPath)
		delete(remainingActual, newPath)
	}
	for _, path := range sortedKeys(remainingActual) {
		after := remainingActual[path]
		changes = append(changes, change(Created, path, "", nil, &after))
	}
	for _, path := range sortedKeys(remainingPrevious) {
		before := remainingPrevious[path]
		changes = append(changes, change(Deleted, path, "", &before, nil))
	}

	if expected != nil {
		for _, path := range sortedKeys(expected) {
			exp := expected[path]
			state, ok := actual[path]
			if !ok {
				changes = append(changes, Change{Kind: Missing, Path: path})
				continue
			}
			if exp.Size != nil && state.Size != *exp.Size {
				changes = append(changes, change(Invalid, path, "", nil, &state))
			}
		}
		for _, path := range sortedKeys(actual) {
			if _, ok := expected[path]; !ok {
				state := actual[path]
				changes = append(changes, change(Orphan, path, "", nil, &state))
			}
		}
	}
	return changes
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
