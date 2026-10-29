.PHONY: build test check

build:
	go build -buildvcs=false -trimpath -o bin/gtfs-rt-archiver ./cmd/gtfs-rt-archiver

test:
	env GOMAXPROCS=2 GOGC=50 go test -p 1 ./...

check:
	env GOMAXPROCS=2 GOGC=50 go test -p 1 -race ./...
	env GOMAXPROCS=2 GOGC=50 go vet ./...
