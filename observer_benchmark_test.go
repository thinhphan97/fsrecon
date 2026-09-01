package fsrecon

import (
	"context"
	internalbackend "github.com/thinhphan97/fsrecon/internal/backend"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func benchmarkObserver() *Observer {
	o, _ := NewObserver(ObserverConfig{Root: "/bench", HintBuffer: 1024, MaxPendingHints: 10000, DebounceWindow: time.Millisecond})
	return o
}

func BenchmarkObserverExactPathEvents(b *testing.B) {
	o := benchmarkObserver()
	fake := newObserverFakeBackend()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o.handleRaw(context.Background(), nil, internalbackend.RawEvent{Path: filepath.Join("/bench", strconv.Itoa(i%10000)), Op: internalbackend.OpWrite})
	}
	b.ReportMetric(float64(o.pendingCount()), "pending")
	_ = fake
}
func BenchmarkObserverSamePathCoalescing(b *testing.B) {
	o := benchmarkObserver()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o.enqueue(Hint{Path: "/bench/a", Scope: HintPath, Cause: HintNativeChange})
	}
}
func BenchmarkObserverCapacityEscalation(b *testing.B) {
	o := benchmarkObserver()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o.enqueue(Hint{Path: filepath.Join("/bench", strconv.Itoa(i)), Scope: HintPath, Cause: HintNativeChange})
	}
}
func BenchmarkObserverBackpressure(b *testing.B) {
	o := benchmarkObserver()
	o.hints = make(chan Hint, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o.enqueue(Hint{Path: filepath.Join("/bench", strconv.Itoa(i)), Scope: HintPath, Cause: HintNativeChange})
		o.flush()
	}
}
func BenchmarkObserverStats(b *testing.B) {
	o := benchmarkObserver()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = o.Stats()
	}
}
func BenchmarkObserverWatchTreeResync(b *testing.B) {
	b.Skip("requires filesystem topology fixture")
}
func BenchmarkObserverRecursiveStartup(b *testing.B) { b.Skip("requires filesystem topology fixture") }
func BenchmarkObserverDirectoryRemoveHints(b *testing.B) {
	o := benchmarkObserver()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o.enqueue(Hint{Path: "/bench/dir", Scope: HintSubtree, Cause: HintNativeChange})
	}
}
func BenchmarkObserverRenameResync(b *testing.B) { b.Skip("requires filesystem topology fixture") }
