package fsrecon

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	t.integrityMu.Lock()
	defer t.integrityMu.Unlock()
	t.mu.RLock()
	closed := t.closed
	t.mu.RUnlock()
	if closed {
		return IntegrityReport{}, ErrClosed
	}

	report := IntegrityReport{StartedAt: time.Now()}
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
		return nil
	})
	report.Duration = time.Since(report.StartedAt)
	if err != nil {
		return report, fmt.Errorf("fsrecon: integrity scrub: %w", err)
	}
	t.stats.integrityScanned.Add(report.Scanned)
	t.stats.corruptDetected.Add(report.Corrupt)
	t.stats.reportEventsTruncated.Add(report.EventsTruncated)
	for _, event := range report.Events {
		t.sendEvent(event)
	}
	return report, nil
}

var _ IntegrityChecker = SHA256Checker{}
