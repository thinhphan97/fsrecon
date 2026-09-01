.PHONY: test test-race vet fmt-check check demo demo-keep

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

check: fmt-check vet test

demo:
	./scripts/demo.sh $(if $(DEMO_PARENT),--parent "$(DEMO_PARENT)")

demo-keep:
	./scripts/demo.sh --keep $(if $(DEMO_PARENT),--parent "$(DEMO_PARENT)")
