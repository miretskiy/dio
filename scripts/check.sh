#!/bin/sh

set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository"

if ! command -v staticcheck >/dev/null 2>&1; then
	echo "staticcheck is required"
	exit 1
fi

unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
	echo "gofmt is required for:"
	echo "$unformatted"
	exit 1
fi

go test ./...
go vet ./...
go test -race ./iosched

host_os=$(go env GOOS)
if [ "$host_os" = linux ]; then
	staticcheck ./...
else
	host_arch=$(go env GOARCH)
	printf 'host: GOOS=%s GOARCH=%s\nlinux: GOOS=linux GOARCH=%s CGO_ENABLED=0\n' \
		"$host_os" "$host_arch" "$host_arch" |
		staticcheck -matrix ./...

	GOOS=linux CGO_ENABLED=0 go test -exec=true ./...
	GOOS=linux CGO_ENABLED=0 go vet ./...
fi
