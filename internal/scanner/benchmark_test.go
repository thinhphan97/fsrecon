package scanner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkLargeFlatDirectory(b *testing.B) {
	root := b.TempDir()
	for i := 0; i < 10_000; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("%08d", i)), nil, 0o644); err != nil {
			b.Fatal(err)
		}
	}
	benchmarkScan(b, root, 10_000)
}

func BenchmarkDeepDirectoryTree(b *testing.B) {
	root := b.TempDir()
	current := root
	for i := 0; i < 100; i++ {
		current = filepath.Join(current, fmt.Sprintf("d%03d", i))
		if err := os.Mkdir(current, 0o755); err != nil {
			b.Fatal(err)
		}
	}
	benchmarkScan(b, root, 100)
}

func benchmarkScan(b *testing.B, root string, want int) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := (Scanner{Recursive: true}).Scan(context.Background(), root, func(Entry) error {
			count++
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		if count != want {
			b.Fatalf("scanned %d entries, want %d", count, want)
		}
	}
	b.ReportMetric(float64(want)*float64(b.N)/b.Elapsed().Seconds(), "entries/s")
}
