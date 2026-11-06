package buildutil

import (
	"flag"
	"os"
	"strings"
)

// TestMode enables invariant assertions. It defaults to true in Go test
// binaries, including benchmarks, and false otherwise. Set it before starting
// concurrent work; it must not change while assertions may be checked.
var TestMode = isTestBinary()

// EnableTestMode sets TestMode and returns a function that restores its previous
// value. Neither call may race concurrent work that checks assertions.
func EnableTestMode(enabled bool) func() {
	previous := TestMode
	TestMode = enabled
	return func() { TestMode = previous }
}

func isTestBinary() bool {
	// flag-based check: testing.init registers "test.v". This is the
	// canonical signal, but testing.init is a sibling of buildutil.init —
	// init ordering between them is unspecified.
	if flag.Lookup("test.v") != nil {
		return true
	}
	// argv fallback: standard "go test" binaries end in ".test" (also
	// covers `go test -c -o foo.test`), while rules_go test binaries end in
	// "_test". Catches the case where buildutil.init runs before testing.init.
	return strings.HasSuffix(os.Args[0], ".test") ||
		strings.HasSuffix(os.Args[0], "_test")
}
