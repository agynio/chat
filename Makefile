SHELL := /bin/bash

PROTO_PATHS := agynio/api/chat/v1 agynio/api/threads/v1

.PHONY: all proto build test lint fmt clean e2e

all: build

proto:
	buf generate buf.build/agynio/api $(foreach p,$(PROTO_PATHS),--path $(p))

build:
	GOFLAGS=-mod=mod go build ./...

test:
	GOFLAGS=-mod=mod go test ./...

lint:
	GOFLAGS=-mod=mod go vet ./...

fmt:
	gofmt -w $(shell find . -type f -name '*.go')

clean:
	rm -rf gen

e2e: proto
	GOFLAGS=-mod=mod go test -v -count=1 ./test/e2e/...
