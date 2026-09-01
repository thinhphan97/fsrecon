package fsrecon

import (
	"context"
	"fmt"
)

type changeBatcher struct {
	ctx        context.Context
	sink       ChangeSink
	generation uint64
	sequence   uint64
	limit      int
	pending    []Event
}

func newChangeBatcher(ctx context.Context, sink ChangeSink, generation uint64, limit int) *changeBatcher {
	return &changeBatcher{ctx: ctx, sink: sink, generation: generation, limit: limit}
}

func (b *changeBatcher) Add(event Event) error {
	if b.sink == nil {
		return nil
	}
	if len(b.pending) == b.limit {
		if err := b.flush(false); err != nil {
			return err
		}
	}
	b.pending = append(b.pending, event)
	return nil
}

func (b *changeBatcher) Finish() error {
	if b.sink == nil || len(b.pending) == 0 {
		return nil
	}
	return b.flush(true)
}

func (b *changeBatcher) flush(final bool) error {
	events := append([]Event(nil), b.pending...)
	batch := ChangeBatch{
		Generation: b.generation,
		Sequence:   b.sequence,
		Final:      final,
		Events:     events,
	}
	if err := b.sink.ApplyChanges(b.ctx, batch); err != nil {
		return fmt.Errorf("fsrecon: deliver generation %d batch %d: %w", b.generation, b.sequence, err)
	}
	b.sequence++
	b.pending = b.pending[:0]
	return nil
}
