//go:build linux

package iosched_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/iosched"
)

// TestURing_VOpenFallocateWriteChain verifies the create-and-first-write fusion
// the slab write path relies on: a single submission opens a file into a virtual
// slot, preallocates it, and writes the first record. Once that chain has
// completed, an independent second write resolves the now-live slot and lands.
// Ordering the second write after the open is the caller's responsibility (here
// via Wait); the scheduler imposes no slot ordering of its own.
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
	chainTkt.Wait()
	require.NoError(t, chainTkt.Error())
	chainTkt.Release()

	// The open has completed, so the slot is live: an independent write now
	// resolves it and lands. The caller orders the write after the open (via the
	// Wait above); the scheduler does not.
	secondTkt, err := s.Submit(iosched.VWriteOp(vfd, second, int64(len(first))))
	require.NoError(t, err)
	secondTkt.Wait()
	require.NoError(t, secondTkt.Error())
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
// completed. The scheduler does not order a reopen against an outstanding close,
// so the caller sequences them — here by waiting the close before the next
// iteration reopens the same slot. (Linking the close ahead of the reopen would
// serve equally; either is the caller's choice, see Scheduler.Submit.)
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

// TestURing_VCloseDrainsInflightWrites verifies the close-drain: a VClose
// submitted right behind in-flight writes to the same slot is held until they
// complete, so every write lands (the slot is not cleared out from under a
// queued write) and its data is durable. It is deterministic — the coordinator
// holds the close until the slot's in-flight ops drain — so it holds under
// -count, unlike relying on the kernel not to race the close.
func TestURing_VCloseDrainsInflightWrites(t *testing.T) {
	s := newVURingSched(t, iosched.URingConfig{RingDepth: 256, VFiles: 2})
	const vfd = uint32(0)
	const n = 64
	const recLen = 64 // contiguous records → coalesced into one writev

	path := filepath.Join(t.TempDir(), "drain.dat")
	rec := func(i int) []byte {
		b := make([]byte, recLen)
		for j := range b {
			b[j] = byte('A' + (i+j)%26)
		}
		return b
	}

	open := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path,
		unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd).
		Link(iosched.VFallocateOp(vfd, int64(n*recLen))))
	require.NoError(t, open.Error())
	open.Release()

	// Fire N durable writes, then a close, without waiting between — the close
	// races the writes' completion, and the drain must hold it behind them.
	writes := make([]*iosched.Ticket, n)
	for i := range writes {
		var err error
		writes[i], err = s.Submit(iosched.VWriteOp(vfd, rec(i), int64(i)*recLen).Durable())
		require.NoError(t, err)
	}
	closeTkt, err := s.Submit(iosched.VCloseOp(vfd))
	require.NoError(t, err)

	for i, w := range writes {
		w.Wait()
		require.NoErrorf(t, w.Error(), "write %d", i)
		require.Equal(t, recLen, w.Result().N)
		w.Release()
	}
	closeTkt.Wait()
	require.NoError(t, closeTkt.Error())
	closeTkt.Release()

	// Every record landed at its offset (read via a fresh fd, not the closed slot).
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	for i := range writes {
		got := make([]byte, recLen)
		_, err := f.ReadAt(got, int64(i)*recLen)
		require.NoError(t, err)
		require.Equalf(t, rec(i), got, "record %d", i)
	}
}

// drainKeepAlive holds files handed to CloseOp so their *os.File finalizer can't
// double-close the descriptor dio already closed. A real caller keeps the file
// referenced for the same reason; letting it finalize after handing off the fd is
// the use-after-close dio does not guard against.
var drainKeepAlive []*os.File

// TestURing_CloseDrainsInflightWrites_Regular is the drain over a regular file
// (no vfiles): durable writes fired with a CloseOp right behind them, no waits.
// The close is held until the writes drain, so all land and are durable, and
// dio's close leaves the file readable via a fresh fd. This is the same drain
// code as the virtual case — only the key differs (VFiles+fd vs slot index).
func TestURing_CloseDrainsInflightWrites_Regular(t *testing.T) {
	s := newURingSched(t)
	const n = 64
	const recLen = 64

	f, err := os.CreateTemp(t.TempDir(), "drain-*.dat")
	require.NoError(t, err)
	path := f.Name()
	drainKeepAlive = append(drainKeepAlive, f) // dio owns the close; keep f from finalizing

	rec := func(i int) []byte {
		b := make([]byte, recLen)
		for j := range b {
			b[j] = byte('a' + (i+j)%26)
		}
		return b
	}

	writes := make([]*iosched.Ticket, n)
	for i := range writes {
		writes[i], err = s.Submit(iosched.WriteOp(f, rec(i), int64(i)*recLen).Durable())
		require.NoError(t, err)
	}
	closeTkt, err := s.Submit(iosched.CloseOp(f))
	require.NoError(t, err)

	for i, w := range writes {
		w.Wait()
		require.NoErrorf(t, w.Error(), "write %d", i)
		require.Equal(t, recLen, w.Result().N)
		w.Release()
	}
	closeTkt.Wait()
	require.NoError(t, closeTkt.Error())
	closeTkt.Release()

	// Read back via a fresh fd; dio has closed the original.
	rf, err := os.Open(path)
	require.NoError(t, err)
	defer rf.Close()
	for i := range writes {
		got := make([]byte, recLen)
		_, err := rf.ReadAt(got, int64(i)*recLen)
		require.NoError(t, err)
		require.Equalf(t, rec(i), got, "record %d", i)
	}
}

// TestURing_VSlot1_Recycle stresses a single virtual slot: open → write → read →
// close, repeated hundreds of times, so slot 0 is reopened every iteration. Each
// cycle fires the durable write and the close with no wait between them, so the
// close is held until the write drains; the next open then recycles the slot. It
// checks data survives the write, the drain, and the reopen across every reuse.
func TestURing_VSlot1_Recycle(t *testing.T) {
	s := newVURingSched(t, iosched.URingConfig{RingDepth: 16, VFiles: 1})
	const vfd = uint32(0)
	const iters = 300
	dir := t.TempDir()

	for i := range iters {
		path := filepath.Join(dir, fmt.Sprintf("gen-%d.dat", i))
		want := fmt.Appendf(nil, "payload-for-generation-%d", i)

		open := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path,
			unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd))
		require.NoErrorf(t, open.Error(), "iter %d: open", i)
		open.Release()

		// Durable write and close fired with no wait between: the close is held
		// until the write drains.
		w, err := s.Submit(iosched.VWriteOp(vfd, want, 0).Durable())
		require.NoError(t, err)
		cl, err := s.Submit(iosched.VCloseOp(vfd))
		require.NoError(t, err)
		w.Wait()
		require.NoErrorf(t, w.Error(), "iter %d: write", i)
		w.Release()
		cl.Wait()
		require.NoErrorf(t, cl.Error(), "iter %d: close", i)
		cl.Release()

		// Recycle the slot: reopen and read the data back.
		reopen := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_RDWR, 0o600, vfd))
		require.NoErrorf(t, reopen.Error(), "iter %d: reopen", i)
		reopen.Release()

		got := make([]byte, len(want))
		rd := submitOne(t, s, iosched.VReadOp(vfd, got, 0))
		require.NoErrorf(t, rd.Error(), "iter %d: read", i)
		rd.Release()
		require.Equalf(t, want, got, "iter %d: data", i)

		cl2 := submitOne(t, s, iosched.VCloseOp(vfd))
		require.NoErrorf(t, cl2.Error(), "iter %d: close2", i)
		cl2.Release()
	}
}
