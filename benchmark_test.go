package fsrecon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func BenchmarkMemoryStoreScale(b *testing.B) {
	for _, size := range []int{100_000, 1_000_000, 10_000_000} {
		b.Run(scaleName(size), func(b *testing.B) {
			if size == 10_000_000 && os.Getenv("FSRECON_BENCH_10M") == "" {
				b.Skip("set FSRECON_BENCH_10M=1 to run the 10M-entry benchmark")
			}
			ctx := context.Background()
			store := NewMemoryStore()
			runtime.GC()
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			states := make([]FileState, size)
			for i := range states {
				states[i] = FileState{Path: fmt.Sprintf("/data/%08d", i), Size: int64(i)}
			}
			if err := store.Apply(ctx, states, nil); err != nil {
				b.Fatal(err)
			}
			states = nil
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			peakBytesPerEntry := float64(0)
			if after.Alloc > before.Alloc {
				peakBytesPerEntry = float64(after.Alloc-before.Alloc) / float64(size)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				if err := store.Walk(ctx, "/data", func(FileState) error {
					count++
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				if count != size {
					b.Fatalf("walked %d entries", count)
				}
			}
			b.ReportMetric(float64(size)*float64(b.N)/b.Elapsed().Seconds(), "entries/s")
			if peakBytesPerEntry > 0 {
				b.ReportMetric(peakBytesPerEntry, "peak-bytes/entry")
			}
			runtime.KeepAlive(store)
		})
	}
}

func BenchmarkBoltStorePointLookup(b *testing.B) {
	database := filepath.Join(b.TempDir(), "snapshot.db")
	store, err := OpenBoltStore(database)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	states := make([]FileState, 10_000)
	for i := range states {
		states[i] = FileState{Path: fmt.Sprintf("/data/%08d", i), Size: int64(i)}
	}
	if err := store.Apply(ctx, states, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := states[i%len(states)].Path
		if _, ok, err := store.Get(ctx, path); err != nil || !ok {
			b.Fatalf("Get(%q) = ok:%v err:%v", path, ok, err)
		}
	}
	if info, err := os.Stat(database); err == nil {
		b.ReportMetric(float64(info.Size())/float64(len(states)), "disk-bytes/entry")
	}
}

func BenchmarkBoltStoreWalk10K(b *testing.B) {
	store, err := OpenBoltStore(filepath.Join(b.TempDir(), "snapshot.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	states := make([]FileState, 10_000)
	for i := range states {
		states[i] = FileState{Path: fmt.Sprintf("/data/%08d", i), Size: int64(i)}
	}
	if err := store.Apply(ctx, states, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := store.Walk(ctx, "/data", func(FileState) error {
			count++
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		if count != len(states) {
			b.Fatalf("walked %d entries", count)
		}
	}
	b.ReportMetric(float64(len(states))*float64(b.N)/b.Elapsed().Seconds(), "entries/s")
}

func BenchmarkBoltStoreBatch1K(b *testing.B) {
	store, err := OpenBoltStore(filepath.Join(b.TempDir(), "snapshot.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	states := make([]FileState, 1_000)
	for i := range states {
		states[i] = FileState{Path: fmt.Sprintf("/data/%08d", i)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for n := range states {
			states[n].Size = int64(i)
		}
		if err := store.Apply(ctx, states, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(1_000*float64(b.N)/b.Elapsed().Seconds(), "entries/s")
}

func BenchmarkFullReconcile10K(b *testing.B) {
	root := b.TempDir()
	for i := 0; i < 10_000; i++ {
		path := filepath.Join(root, fmt.Sprintf("%08d", i))
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			b.Fatal(err)
		}
	}
	tracker, err := New(Config{Root: root, Recursive: true, EventBuffer: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer tracker.Close()
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		report, err := tracker.Reconcile(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if report.Scanned != 10_000 {
			b.Fatalf("scanned %d files", report.Scanned)
		}
	}
	b.ReportMetric(10_000*float64(b.N)/b.Elapsed().Seconds(), "files/s")
}

func scaleName(size int) string {
	switch size {
	case 100_000:
		return "100K"
	case 1_000_000:
		return "1M"
	case 10_000_000:
		return "10M"
	default:
		return fmt.Sprintf("%d", size)
	}
}
