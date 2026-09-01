package fsrecon

// pendingGeneration is retained while authoritative delivery or snapshot
// commit is incomplete. It freezes the semantic payload and snapshot
// mutation so retries never recompute a generation from a changed filesystem.
type pendingGeneration struct {
	sessionID  string
	generation uint64
	report     ReconcileReport
	events     []Event
	puts       []FileState
	deletes    []string
}

type pendingIntegrityGeneration struct {
	sessionID  string
	generation uint64
	report     IntegrityReport
	events     []Event
}
