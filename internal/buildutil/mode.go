package buildutil

import (
	"flag"
	"os"
	"strings"
	"sync/atomic"
)

// testMode is set at package init by detecting whether the current process is
// a Go test binary. Once set it is never cleared, so [TestMode] is a single
// atomic load on the hot path.
var testMode atomic.Bool

func init() {
	testMode.Store(isTestBinary())
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

// TestMode reports whether the current process is a Go test binary. Determined
// once at package init; subsequent calls are a single atomic load.
func TestMode() bool { return testMode.Load() }

// EnableTestMode forces test mode on. Use from test infrastructure whose
// binary doesn't fit the standard "go test" convention (e.g. a bespoke test
// runner whose argv0 doesn't end in ".test").
func EnableTestMode() { testMode.Store(true) }
