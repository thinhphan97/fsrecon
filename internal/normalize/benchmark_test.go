package normalize

import (
	"path/filepath"
	"testing"

	"github.com/thinhphan97/fsrecon/internal/backend"
)

func BenchmarkEventProcessing(b *testing.B) {
	root := filepath.Join(string(filepath.Separator), "data")
	raw := backend.RawEvent{Path: filepath.Join(root, "ab", "cd", "object"), Op: backend.OpWrite}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := Event(raw, root); !ok {
			b.Fatal("event was not normalized")
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/s")
}
