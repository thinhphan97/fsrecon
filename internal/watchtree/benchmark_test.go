package watchtree

import (
	"fmt"
	"path/filepath"
	"testing"
)

type benchmarkBackend struct{ registrations int }

func (b *benchmarkBackend) Add(string) error {
	b.registrations++
	return nil
}
func (*benchmarkBackend) Remove(string) error { return nil }

func BenchmarkWatchRegistration10K(b *testing.B) {
	root := filepath.Join(string(filepath.Separator), "data")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		backend := &benchmarkBackend{}
		tree := New(root, backend)
		for n := 0; n < 10_000; n++ {
			if err := tree.Add(filepath.Join(root, fmt.Sprintf("%08d", n))); err != nil {
				b.Fatal(err)
			}
		}
		if backend.registrations != 10_000 {
			b.Fatalf("registered %d watches", backend.registrations)
		}
	}
	b.ReportMetric(10_000*float64(b.N)/b.Elapsed().Seconds(), "directories/s")
}
