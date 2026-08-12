.PHONY: all build test race bench vet cover server cli clean

all: vet test

build:
	go build ./...

# Full test suite.
test:
	go test ./...

# Test suite under the race detector (what CI runs).
race:
	go test -race ./...

# Micro-benchmarks plus the standalone throughput/latency harness.
bench:
	go test -run '^$$' -bench=. -benchmem ./...
	go run ./cmd/godb-bench -n 200000

vet:
	go vet ./...

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

# Run the TCP server on :6380 with data in ./data.
server:
	go run ./cmd/godb-server -addr :6380 -dir ./data

# Run the REPL client against a local server.
cli:
	go run ./cmd/godb-cli -addr 127.0.0.1:6380

clean:
	rm -rf ./data coverage.out
	go clean ./...
