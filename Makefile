.PHONY: build fmt fmt-check vet lint test test-race fuzz verify package

VERSION ?= dev
REVISION ?= unknown
GO_LDFLAGS := -s -w \
	-X github.com/yaop-labs/coral/internal/buildinfo.version=$(VERSION) \
	-X github.com/yaop-labs/coral/internal/buildinfo.revision=$(REVISION)

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "$(GO_LDFLAGS)" -o bin/coral ./cmd/coral

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test:
	go test ./... -count=1

test-race:
	go test ./... -race -count=1

fuzz:
	go test ./internal/config -run '^$$' -fuzz '^FuzzConfigParse$$' -fuzztime 10s

verify: fmt-check vet lint test-race

package:
	./scripts/package.sh "$(shell go env GOOS)" "$(shell go env GOARCH)" "$(VERSION)" "$(REVISION)" dist
