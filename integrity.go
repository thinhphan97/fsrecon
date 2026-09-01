package fsrecon

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	internalscanner "github.com/thinhphan97/fsrecon/internal/scanner"
)

// SHA256Checker computes a SHA-256 fingerprint. ExpectedProvider fingerprints,
// when present, are compared by Tracker.Scrub.
type SHA256Checker struct{}

func (SHA256Checker) Check(ctx context.Context, state FileState) (IntegrityResult, error) {
	file, err := os.Open(state.Path)
	if err != nil {
		return IntegrityResult{}, err
	}
	defer file.Close()
	hash := sha256.New()
	buffer := make([]byte, 128*1024)
	for {
		if err := ctx.Err(); err != nil {
			return IntegrityResult{}, err
		}
		n, readErr := file.Read(buffer)
		if n > 0 {
			if _, err := hash.Write(buffer[:n]); err != nil {
				return IntegrityResult{}, err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return IntegrityResult{}, readErr
		}
	}
	return IntegrityResult{Valid: true, Fingerprint: hash.Sum(nil)}, nil
}

// Scrub explicitly verifies regular-file contents. It never runs on the native
// event-reader goroutine and does not mutate the metadata snapshot.
func (t *Tracker) Scrub(ctx context.Context) (IntegrityReport, error) {
	if ctx == nil {
		return IntegrityReport{}, fmt.Errorf("fsrecon: nil context")
	}
	if t.config.Integrity == nil {
		return IntegrityReport{}, ErrNoIntegrity
	}
	t.reconcileMu.Lock()
	defer t.reconcileMu.Unlock()
	t.integrityMu.Lock()
	defer t.integrityMu.Unlock()
	t.mu.RLock()
	closed := t.closed
	t.mu.RUnlock()
	if closed {
		return IntegrityReport{}, ErrClosed
	}
	if t.pending != nil {
		if t.pending.kind == pendingReconcile {
			if _, err := t.resumePending(ctx); err != nil {
				return IntegrityReport{}, err
			}
		} else {
			return t.resumePendingIntegrity(ctx)
		}
	}

	report := IntegrityReport{StartedAt: time.Now(), Generation: t.generation.Load() + 1}
	allEvents := make([]Event, 0)
	var stage *os.File
	var encoder *json.Encoder
	var err error
	if t.config.ChangeSink != nil {
		stage, err = os.CreateTemp("", "fsrecon-integrity-*.jsonl")
		if err != nil {
			return report, fmt.Errorf("fsrecon: create integrity change log: %w", err)
		}
		defer func() {
			_ = stage.Close()
			_ = os.Remove(stage.Name())
		}()
		encoder = json.NewEncoder(stage)
	}
	expected, err := t.loadExpectedScopes(ctx, []string{t.root})
	if err != nil {
		return report, err
	}
	s := internalscanner.Scanner{
		Recursive: t.config.Recursive, SymlinkPolicy: scannerPolicy(t.config.SymlinkPolicy),
	}
	if t.config.Filter != nil {
		s.Filter = func(entry internalscanner.Entry) bool {
			state := stateFromEntry(entry)
			return t.config.Filter(state.Path, state)
		}
	}
	err = s.Scan(ctx, t.root, func(entry internalscanner.Entry) error {
		state := stateFromEntry(entry)
		if state.Type != FileTypeRegular {
			return nil
		}
		result, err := t.config.Integrity.Check(ctx, state)
		if err != nil {
			return fmt.Errorf("check %q: %w", state.Path, err)
		}
		report.Scanned++
		valid := result.Valid
		if entry, ok := expected[state.Path]; ok && len(entry.Fingerprint) > 0 {
			valid = valid && bytes.Equal(result.Fingerprint, entry.Fingerprint)
		}
		if valid {
			report.Healthy++
			return nil
		}
		after := state
		event := Event{
			Kind: EventCorrupt, Path: state.Path, After: &after,
			Source: SourceIntegrity, Time: time.Now(),
		}
		report.Corrupt++
		if len(report.Events) < t.config.ReportEventLimit {
			report.Events = append(report.Events, event)
		} else {
			report.EventsTruncated++
		}
		if encoder != nil {
			if err := encoder.Encode(stagedEvent{
				Kind: event.Kind, Path: event.Path, OldPath: event.OldPath,
				Before: storedState(event.Before), After: storedState(event.After),
				Source: event.Source, Time: event.Time,
			}); err != nil {
				return err
			}
		}
		allEvents = append(allEvents, event)
		return nil
	})
	report.Duration = time.Since(report.StartedAt)
	if err != nil {
		return report, fmt.Errorf("fsrecon: integrity scrub: %w", err)
	}
	if stage != nil {
		if err := stage.Close(); err != nil {
			return report, fmt.Errorf("fsrecon: close integrity change log: %w", err)
		}
		input, err := os.Open(stage.Name())
		if err != nil {
			return report, fmt.Errorf("fsrecon: open integrity change log: %w", err)
		}
		decoder := json.NewDecoder(bufio.NewReader(input))
		delivery := newChangeBatcher(ctx, t.config.ChangeSink, t.sessionID, report.Generation, t.config.ChangeBatchSize)
		for {
			var staged stagedEvent
			if err := decoder.Decode(&staged); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				_ = input.Close()
				return report, fmt.Errorf("fsrecon: read integrity change log: %w", err)
			}
			if err := delivery.Add(eventFromStaged(staged)); err != nil {
				_ = input.Close()
				t.pending = &pendingGeneration{kind: pendingIntegrity, sessionID: t.sessionID, generation: report.Generation, integrity: &report, events: cloneEvents(allEvents)}
				return report, err
			}
		}
		if err := input.Close(); err != nil {
			return report, fmt.Errorf("fsrecon: close integrity change log: %w", err)
		}
		if err := delivery.Finish(); err != nil {
			t.pending = &pendingGeneration{kind: pendingIntegrity, sessionID: t.sessionID, generation: report.Generation, integrity: &report, events: cloneEvents(allEvents)}
			return report, err
		}
	}
	t.stats.integrityScanned.Add(report.Scanned)
	t.stats.corruptDetected.Add(report.Corrupt)
	t.stats.reportEventsTruncated.Add(report.EventsTruncated)
	t.generation.Store(report.Generation)
	for _, event := range report.Events {
		t.sendEvent(event)
	}
	return report, nil
}

func (t *Tracker) resumePendingIntegrity(ctx context.Context) (IntegrityReport, error) {
	p := t.pending
	if p == nil || p.integrity == nil {
		return IntegrityReport{}, fmt.Errorf("fsrecon: no pending integrity generation")
	}
	delivery := newChangeBatcher(ctx, t.config.ChangeSink, p.sessionID, p.generation, t.config.ChangeBatchSize)
	for _, event := range p.events {
		if err := delivery.Add(event); err != nil {
			return *p.integrity, err
		}
	}
	if err := delivery.Finish(); err != nil {
		return *p.integrity, err
	}
	report := *p.integrity
	t.pending = nil
	t.generation.Store(p.generation)
	t.stats.integrityScanned.Add(report.Scanned)
	t.stats.corruptDetected.Add(report.Corrupt)
	t.stats.reportEventsTruncated.Add(report.EventsTruncated)
	for _, event := range report.Events {
		t.sendEvent(event)
	}
	return report, nil
}

var _ IntegrityChecker = SHA256Checker{}
