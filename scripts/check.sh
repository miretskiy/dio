#!/bin/sh

set -eu

repository=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
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

	# The liburing conformance tests are gated on cgo alone, so the go test above
	# already ran them when a C toolchain is present. Say so when it is not, since
	# nothing else validates the committed UAPI against the bundled headers.
	if [ "$(go env CGO_ENABLED)" != 1 ]; then
		echo "warning: liburing conformance skipped, CGO_ENABLED=0"
	fi
else
	host_arch=$(go env GOARCH)
	printf 'host: GOOS=%s GOARCH=%s\nlinux: GOOS=linux GOARCH=%s CGO_ENABLED=0\n' \
		"$host_os" "$host_arch" "$host_arch" |
		staticcheck -matrix ./...

	# Go disables cgo when cross-compiling, so the liburing conformance tests and
	# their oracle drop out here. Only Linux runs them.
	GOOS=linux go test -exec=true ./...
	GOOS=linux go vet ./...
fi
