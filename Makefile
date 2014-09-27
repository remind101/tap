.PHONY: cmd run

default: test

test:
	godep go test ./...
