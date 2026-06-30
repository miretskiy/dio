//go:build linux

package iosched_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/iosched"
)

// TestURing_VOpenFallocateWriteChain verifies the create-and-first-write fusion
// the slab write path relies on: a single submission opens a file into a virtual
// slot, preallocates it, and writes the first record. A second, independent
// write submitted right after parks behind the open barrier and still lands.
func TestURing_VOpenFallocateWriteChain(t *testing.T) {
	s := newVURingSched(t, iosched.URingConfig{RingDepth: 16, VFiles: 4})
	const vfd = uint32(1)
	const size = int64(1 << 16)

	path := filepath.Join(t.TempDir(), "chain.dat")
	first := []byte("first-record")
	second := []byte("second-record")

	chain := iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, vfd).
		Link(iosched.VFallocateOp(vfd, size)).
		Link(iosched.VWriteOp(vfd, first, 0))
	chainTkt, err := s.Submit(chain)
	require.NoError(t, err)

	// Submitted while the open may still be in flight: this parks behind the
	// open barrier and runs once the open completes.
	secondTkt, err := s.Submit(iosched.VWriteOp(vfd, second, int64(len(first))))
	require.NoError(t, err)

	chainTkt.Wait()
	secondTkt.Wait()
	require.NoError(t, chainTkt.Error())
	require.NoError(t, secondTkt.Error())
	chainTkt.Release()
	secondTkt.Release()

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, info.Size(), size)

	got := make([]byte, len(first)+len(second))
	rd := submitOne(t, s, iosched.VReadOp(vfd, got, 0))
	require.NoError(t, rd.Error())
	rd.Release()
	require.Equal(t, append(append([]byte{}, first...), second...), got)

	cl := submitOne(t, s, iosched.VCloseOp(vfd))
	require.NoError(t, cl.Error())
	cl.Release()
}

// TestURing_VSlotRecycleAfterCloseWait verifies the slot-recycle discipline the
// write ring and read cache use: a slot is reused only after its prior close has
// completed. Reopening a slot whose close has been *waited* succeeds — whereas a
// reopen submitted before the close completes parks behind the close barrier and
// is failed EBADF (see TestCoordinatorVCloseFailsParkedOps). Waiting the close
// is what makes recycle safe without changing the barrier model.
func TestURing_VSlotRecycleAfterCloseWait(t *testing.T) {
	s := newVURingSched(t, iosched.URingConfig{RingDepth: 16, VFiles: 2})
	const vfd = uint32(0)
	dir := t.TempDir()

	for i, name := range []string{"gen0.dat", "gen1.dat"} {
		path := filepath.Join(dir, name)
		want := []byte{byte('A' + i), byte('A' + i), byte('A' + i), byte('A' + i)}

		op := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path,
			unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd))
		require.NoError(t, op.Error())
		op.Release()

		w := submitOne(t, s, iosched.VWriteOp(vfd, want, 0))
		require.NoError(t, w.Error())
		w.Release()

		got := make([]byte, len(want))
		rd := submitOne(t, s, iosched.VReadOp(vfd, got, 0))
		require.NoError(t, rd.Error())
		rd.Release()
		require.Equal(t, want, got)

		// Wait the close before the next iteration reopens the same slot.
		cl := submitOne(t, s, iosched.VCloseOp(vfd))
		require.NoError(t, cl.Error())
		cl.Release()
	}
}
