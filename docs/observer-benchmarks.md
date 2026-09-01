# Observer benchmarks

Observer benchmarks measure hint ingestion, coalescing, bounded backpressure,
capacity escalation, and statistics overhead without maintaining per-file
metadata. Run them with:

```bash
go test -run '^$' -bench 'Observer' -benchmem ./...
```

Results depend strongly on filesystem, kernel watcher limits, CPU, and Go
version. For storage pilots, measure separate layouts such as 100K, 1M, and
10M files across 256, 4,096, and 65,536 directories. Observer does not retain
file metadata, but recursive startup and topology resync still enumerate
directories. Keep large fixture generation opt-in and outside normal CI.

Interpret event throughput, hint latency, pending-hint bounds, and watched
directory count together; synthetic numbers are not production limits.
