# Benchmarks

The benchmark suite measures the two primary resource metrics: retained bytes
per tracked entry and entries/files processed per second.

```bash
go test -run='^$' -bench=. -benchmem ./...
```

Focused commands:

```bash
go test -run='^$' -bench=BenchmarkMemoryStoreScale -benchmem .
go test -run='^$' -bench=BenchmarkBoltStorePointLookup -benchmem .
go test -run='^$' -bench='BenchmarkBoltStore(Walk|Batch)' -benchmem .
go test -run='^$' -bench=BenchmarkFullReconcile10K -benchmem .
go test -run='^$' -bench=BenchmarkCollapse100K -benchmem ./internal/dirtyset
```

The 10-million-entry memory case is opt-in because it can consume significant
RAM:

```bash
FSRECON_BENCH_10M=1 go test -run='^$' -bench=BenchmarkMemoryStoreScale/10M -benchtime=1x .
```

Benchmark results depend heavily on filesystem, storage, operating system, and
Go version. Record those details with every published baseline. CI validates
that benchmarks compile but does not run the large-scale cases on every push.

## Baseline

Baseline recorded on 2026-09-01 with Go 1.23.4, macOS arm64, Apple M4, and the
local APFS volume. Each case used `-benchtime=1x`, except point lookup which used
100 iterations.

| Benchmark | Result |
| --- | ---: |
| MemoryStore, 100K entries | 5.08M entries/s; 326 peak bytes/entry |
| DirtySet collapse, 100K paths | 2.31M paths/s |
| Full reconciliation, 10K files | 350K files/s |
| BoltStore point lookup | 2.35 µs/op |

These numbers are a regression baseline, not a performance guarantee. Run the
suite on the target filesystem before capacity planning.
