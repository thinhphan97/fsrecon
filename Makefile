.PHONY: test test-race integration vet fmt-check check bench bench-large demo demo-keep

test:
	go test ./...

test-race:
	go test -race ./...

integration:
	go test -count=3 ./tests/integration

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

check: fmt-check vet test

bench:
	go test -run='^$$' -bench=. -benchmem ./...

bench-large:
	FSRECON_BENCH_10M=1 go test -run='^$$' -bench=BenchmarkMemoryStoreScale/10M -benchtime=1x .

demo:
	./scripts/demo.sh $(if $(DEMO_PARENT),--parent "$(DEMO_PARENT)")

demo-keep:
	./scripts/demo.sh --keep $(if $(DEMO_PARENT),--parent "$(DEMO_PARENT)")
