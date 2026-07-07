VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)"

.PHONY: all build clean test vet fmt deps

all: deps vet build

deps:
	go mod tidy

build:
	go build $(LDFLAGS) -o bin/frenzy ./cmd

build-linux:
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o bin/frenzy-linux-amd64 ./cmd
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o bin/frenzy-linux-arm64 ./cmd

clean:
	rm -rf bin/

test:
	go test -v -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

run: build
	./bin/frenzy --listen :15432 \
		--primary "$(PRIMARY)" \
		--mirror "$(MIRROR)" \
		--mirror-all-queries

install: build
	cp bin/frenzy /usr/local/bin/frenzy
