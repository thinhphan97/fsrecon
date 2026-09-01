// Package debounce provides a resettable quiet-period timer.
package debounce

import "time"

type Timer struct {
	window time.Duration
	timer  *time.Timer
	ch     <-chan time.Time
}

func New(window time.Duration) *Timer { return &Timer{window: window} }

func (t *Timer) Trigger() {
	if t.timer == nil {
		t.timer = time.NewTimer(t.window)
		t.ch = t.timer.C
		return
	}
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	t.timer.Reset(t.window)
	t.ch = t.timer.C
}

func (t *Timer) C() <-chan time.Time { return t.ch }

func (t *Timer) Stop() {
	if t.timer != nil {
		t.timer.Stop()
	}
	t.ch = nil
}
