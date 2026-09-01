package debounce

import (
	"testing"
	"time"
)

func TestTimerResetsQuietWindow(t *testing.T) {
	timer := New(30 * time.Millisecond)
	defer timer.Stop()
	timer.Trigger()
	time.Sleep(20 * time.Millisecond)
	timer.Trigger()
	select {
	case <-timer.C():
		t.Fatal("timer fired before reset window")
	case <-time.After(15 * time.Millisecond):
	}
	select {
	case <-timer.C():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timer did not fire")
	}
}
