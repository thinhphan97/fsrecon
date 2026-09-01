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

func BenchmarkDirtySubtreeReconciliation(b *testing.B) {
	root := b.TempDir()
	for directory := 0; directory < 100; directory++ {
		dir := filepath.Join(root, fmt.Sprintf("d%03d", directory))
		if err := os.Mkdir(dir, 0o755); err != nil {
			b.Fatal(err)
		}
		for file := 0; file < 100; file++ {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%03d", file)), nil, 0o644); err != nil {
				b.Fatal(err)
			}
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
	cases := []struct {
		name   string
		scopes []string
		files  float64
	}{
		{name: "full", scopes: []string{root}, files: 10_100},
		{name: "1-percent", scopes: []string{filepath.Join(root, "d000")}, files: 100},
		{name: "0.01-percent", scopes: []string{filepath.Join(root, "d000", "f000")}, files: 1},
	}
	for _, test := range cases {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := tracker.reconcileScopes(context.Background(), test.scopes); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(test.files*float64(b.N)/b.Elapsed().Seconds(), "entries/s")
		})
	}
}

func BenchmarkStartupToSynced(b *testing.B) {
	for _, size := range []int{100_000, 1_000_000} {
		b.Run(scaleName(size), func(b *testing.B) {
			if runtime.GOOS == "darwin" && size >= 100_000 && os.Getenv("FSRECON_BENCH_NATIVE_LARGE") == "" {
				b.Skip("macOS kqueue may allocate one descriptor per existing file; set FSRECON_BENCH_NATIVE_LARGE=1 only with a raised ulimit")
			}
			if size == 1_000_000 && os.Getenv("FSRECON_BENCH_1M") == "" {
				b.Skip("set FSRECON_BENCH_1M=1 to run the 1M-file startup benchmark")
			}
			root := b.TempDir()
			for i := 0; i < size; i++ {
				if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("%08d", i)), nil, 0o644); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tracker, err := New(Config{Root: root, Recursive: true, EventBuffer: 1})
				if err != nil {
					b.Fatal(err)
				}
				if err := tracker.Start(context.Background()); err != nil {
					b.Fatal(err)
				}
				if state := tracker.State(); state != StateSynced {
					b.Fatalf("startup state = %v", state)
				}
				if err := tracker.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size)*float64(b.N)/b.Elapsed().Seconds(), "files/s")
		})
	}
}

func BenchmarkPersistentRestartVerification10K(b *testing.B) {
	root := b.TempDir()
	for i := 0; i < 10_000; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("%08d", i)), nil, 0o644); err != nil {
			b.Fatal(err)
		}
	}
	database := filepath.Join(b.TempDir(), "snapshot.db")
	store, err := OpenBoltStore(database)
	if err != nil {
		b.Fatal(err)
	}
	tracker, err := New(Config{Root: root, Recursive: true, Store: store, EventBuffer: 1})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := tracker.Reconcile(context.Background()); err != nil {
		b.Fatal(err)
	}
	_ = tracker.Close()
	if err := store.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store, err := OpenBoltStore(database)
		if err != nil {
			b.Fatal(err)
		}
		tracker, err := New(Config{Root: root, Recursive: true, Store: store, EventBuffer: 1})
		if err != nil {
			b.Fatal(err)
		}
		report, err := tracker.Reconcile(context.Background())
		if err != nil || report.Scanned != 10_000 {
			b.Fatalf("restart reconciliation: report=%+v err=%v", report, err)
		}
		_ = tracker.Close()
		if err := store.Close(); err != nil {
			b.Fatal(err)
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
