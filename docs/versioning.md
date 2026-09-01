# Versioning and compatibility

fsrecon follows Semantic Versioning. Beginning with v1.0, exported identifiers,
method signatures, enum meanings, snapshot encoding, and documented default
behaviour are compatibility commitments.

Minor releases may add fields, event kinds, optional interfaces, store
implementations, and configuration knobs. Existing event meanings will not be
silently redefined. Breaking API or behavioural changes require a new major
module version.

`FileID` remains opaque. Its text representation may only be used to persist
and restore values produced by the same fsrecon major version; applications
must not parse it into operating-system fields.

Native kernels can produce different raw event sequences. Compatibility is
defined in terms of eventual semantic state and events, not exact raw event
counts or ordering.

`ChangeBatch` delivery is at-least-once. Its idempotency key is
`SessionID + Generation + Sequence`; `SessionID` changes when a tracker is
recreated, while generation and sequence may restart from their initial values.
