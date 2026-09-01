package fsrecon

// pendingGeneration is retained while authoritative delivery or snapshot
// commit is incomplete. It freezes the semantic payload and snapshot
// mutation so retries never recompute a generation from a changed filesystem.
type pendingGeneration struct {
	kind       pendingKind
	sessionID  string
	generation uint64
	report     ReconcileReport
	events     []Event
	puts       []FileState
	deletes    []string
	integrity  *IntegrityReport
}

type pendingKind uint8

const (
	pendingReconcile pendingKind = iota
	pendingIntegrity
)
