package iosched_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

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

// runOp submits op, waits for it, and returns its count.
func runOp(t *testing.T, s iosched.Scheduler, op iosched.Op) int {
	t.Helper()
	ticket, err := s.Submit(op)
	require.NoError(t, err)
	ticket.Wait()
	require.NoError(t, ticket.Error())
	return ticket.N()
}

func TestVirtualOpenReplacesIdleSlot(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		dir := t.TempDir()
		firstPath := filepath.Join(dir, "first.dat")
		secondPath := filepath.Join(dir, "second.dat")
		const vfd = uint32(0)

		runOp(t, s, iosched.VOpenatOp(unix.AT_FDCWD, firstPath,
			unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd))
		runOp(t, s, iosched.VWriteOp(vfd, []byte("first"), 0))

		runOp(t, s, iosched.VOpenatOp(unix.AT_FDCWD, secondPath,
			unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd))
		runOp(t, s, iosched.VWriteOp(vfd, []byte("second"), 0))
		runOp(t, s, iosched.VCloseOp(vfd))

		first, err := os.ReadFile(firstPath)
		require.NoError(t, err)
		require.Equal(t, []byte("first"), first)
		second, err := os.ReadFile(secondPath)
		require.NoError(t, err)
		require.Equal(t, []byte("second"), second)
	})
}

// TestSchedulerCloseCompletesPending is the shutdown contract: no ticket is left
// unfinished. It submits a burst, closes without waiting, then waits every
// ticket under a timeout — each must return (completed, or failed with the close
// error), never hang.
func TestSchedulerCloseCompletesPending(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		f := newEmptyFile(t)
		const n = 64
		buf := make([]byte, 512)
		tickets := make([]iosched.Ticket, n)
		for i := range n {
			tk, err := s.Submit(iosched.WriteOp(f, buf, int64(i*len(buf))))
			require.NoError(t, err)
			tickets[i] = tk
		}

		require.NoError(t, s.Close()) // close without waiting for the burst

		done := make(chan struct{})
		go func() {
			for _, tk := range tickets {
				tk.Wait()
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("tickets did not all finish after Close")
		}

		for _, tk := range tickets {
			if err := tk.Error(); err != nil {
				require.ErrorContains(t, err, "closed") // completed, or the close error
			}
		}
	})
}
