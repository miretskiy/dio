package iosched_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/iosched"
)

// schedulerFactory names a scheduler backend and constructs a fresh instance
// with cleanup registered on t. newSched may Skip the test when the platform
// can't provide the backend; since it runs inside a subtest, a skip drops only
// that backend, not the whole test.
type schedulerFactory struct {
	name     string
	newSched func(t *testing.T) iosched.Scheduler
}

// forEachScheduler runs fn as a subtest against every available backend, each
// with its own fresh scheduler. The backend list is platform-specific:
// availableSchedulers is defined per GOOS (POSIX everywhere, io_uring on Linux).
func forEachScheduler(t *testing.T, fn func(t *testing.T, s iosched.Scheduler)) {
	for _, f := range availableSchedulers() {
		t.Run(f.name, func(t *testing.T) { fn(t, f.newSched(t)) })
	}
}

func newPOSIX(t *testing.T) iosched.Scheduler {
	s := iosched.NewPOSIXScheduler()
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// runOp submits op, waits for it, and returns a copy of its Result. Release is
// deferred immediately after Submit — before any require — so a failed assertion
// still cleans up, and it is nil-safe if Submit itself errored. This is the
// standard submit/await/check lifecycle for any op, not just vectored ones.
func runOp(t *testing.T, s iosched.Scheduler, op iosched.Op) iosched.Result {
	t.Helper()
	ticket, err := s.Submit(op)
	defer ticket.Release()
	require.NoError(t, err)
	ticket.Wait()
	require.NoError(t, ticket.Error())
	return ticket.Op.Result
}
