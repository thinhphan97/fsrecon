package reconcile

import "testing"

func TestDiffSemanticChanges(t *testing.T) {
	previous := map[string]State{
		"/old":       {Path: "/old", ID: "move", Size: 1},
		"/replace":   {Path: "/replace", ID: "old", Size: 1},
		"/modify":    {Path: "/modify", ID: "same", Size: 1},
		"/deleted":   {Path: "/deleted", ID: "gone"},
		"/unchanged": {Path: "/unchanged", ID: "steady"},
	}
	actual := map[string]State{
		"/new":       {Path: "/new", ID: "move", Size: 1},
		"/replace":   {Path: "/replace", ID: "new", Size: 1},
		"/modify":    {Path: "/modify", ID: "same", Size: 2},
		"/created":   {Path: "/created", ID: "created"},
		"/unchanged": {Path: "/unchanged", ID: "steady"},
	}
	changes := Diff(previous, actual, nil)
	want := map[Kind]int{Created: 1, Modified: 1, Deleted: 1, Moved: 1, Replaced: 1}
	for _, change := range changes {
		want[change.Kind]--
		if change.Kind == Moved && (change.OldPath != "/old" || change.Path != "/new") {
			t.Fatalf("bad move: %+v", change)
		}
	}
	for kind, remaining := range want {
		if remaining != 0 {
			t.Fatalf("kind %d remaining = %d; changes = %+v", kind, remaining, changes)
		}
	}
}

func TestDiffExpected(t *testing.T) {
	size := int64(2)
	actual := map[string]State{
		"/valid":   {Path: "/valid", Size: 1},
		"/invalid": {Path: "/invalid", Size: 1},
		"/orphan":  {Path: "/orphan"},
	}
	expected := map[string]Expected{
		"/valid":   {Path: "/valid"},
		"/invalid": {Path: "/invalid", Size: &size},
		"/missing": {Path: "/missing"},
	}
	changes := Diff(actual, actual, expected)
	want := map[Kind]int{Missing: 1, Orphan: 1, Invalid: 1}
	for _, change := range changes {
		want[change.Kind]--
	}
	for kind, remaining := range want {
		if remaining != 0 {
			t.Fatalf("kind %d remaining = %d; changes = %+v", kind, remaining, changes)
		}
	}
}
