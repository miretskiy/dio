//go:build linux

package iosched_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
)

// TestURing_VOpenFallocateWriteChain verifies the create-and-first-write fusion
// the slab write path relies on. The independent second write is submitted
// without waiting; the open barrier keeps it out of the ring until the whole
// open-fallocate-write chain has completed.
func TestURing_VOpenFallocateWriteChain(t *testing.T) {
	s := newVURingSched(t, iosched.WithRingDepth(16), iosched.WithVFiles(4))
	const vfd = uint32(1)
	const size = int64(1 << 16)

	path := filepath.Join(t.TempDir(), "chain.dat")
	first := []byte("first-record")
	second := []byte("second-record")

	chain := iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, vfd).
		Link(iosched.VFallocateOp(vfd, size), iosched.VWriteOp(vfd, first, 0))
	chainTkt, err := s.Submit(chain)
	require.NoError(t, err)

	// No artificial open wait: this independent submission is ordered by the
	// slot's open barrier.
	secondTkt, err := s.Submit(iosched.VWriteOp(vfd, second, int64(len(first))))
	require.NoError(t, err)
	_, err = chainTkt.Wait()
	require.NoError(t, err)
	_, err = secondTkt.Wait()
	require.NoError(t, err)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, info.Size(), size)

	got := make([]byte, len(first)+len(second))
	_, err = submitOne(t, s, iosched.VReadOp(vfd, got, 0))
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, first...), second...), got)

	_, err = submitOne(t, s, iosched.VCloseOp(vfd))
	require.NoError(t, err)
}

// TestURing_VOpenBlocksLaterReads is the open-barrier contract directly: reads
// submitted after an open need no caller-side Wait, yet they cannot resolve the
// virtual slot until the open CQE has installed the file.
func TestURing_VOpenBlocksLaterReads(t *testing.T) {
	s := newVURingSched(t, iosched.WithRingDepth(64), iosched.WithVFiles(2))
	const vfd = uint32(0)
	want := []byte("read-after-open-without-an-artificial-wait")
	path := filepath.Join(t.TempDir(), "open-barrier.dat")
	require.NoError(t, os.WriteFile(path, want, 0o600))

	open, err := s.Submit(iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_RDONLY, 0, vfd))
	require.NoError(t, err)
	const readers = 32
	reads := make([]iosched.Ticket, readers)
	bufs := make([][]byte, readers)
	for i := range reads {
		bufs[i] = make([]byte, len(want))
		reads[i], err = s.Submit(iosched.VReadOp(vfd, bufs[i], 0))
		require.NoError(t, err)
	}

	_, err = open.Wait()
	require.NoError(t, err)
	for i, read := range reads {
		_, err := read.Wait()
		require.NoErrorf(t, err, "read %d", i)
		require.Equal(t, want, bufs[i])
	}

	_, err = submitOne(t, s, iosched.VCloseOp(vfd))
	require.NoError(t, err)
}

// TestURing_VSlotRecycleAfterCloseWait covers the supported slot-reuse contract:
// the close completes before the next open is submitted for that slot.
func TestURing_VSlotRecycleAfterCloseWait(t *testing.T) {
	s := newVURingSched(t, iosched.WithRingDepth(16), iosched.WithVFiles(2))
	const vfd = uint32(0)
	dir := t.TempDir()

	for i, name := range []string{"gen0.dat", "gen1.dat"} {
		path := filepath.Join(dir, name)
		want := []byte{byte('A' + i), byte('A' + i), byte('A' + i), byte('A' + i)}

		_, err := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path,
			unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd))
		require.NoError(t, err)

		_, err = submitOne(t, s, iosched.VWriteOp(vfd, want, 0))
		require.NoError(t, err)

		got := make([]byte, len(want))
		_, err = submitOne(t, s, iosched.VReadOp(vfd, got, 0))
		require.NoError(t, err)
		require.Equal(t, want, got)

		// Wait the close before the next iteration reopens the same slot.
		_, err = submitOne(t, s, iosched.VCloseOp(vfd))
		require.NoError(t, err)
	}
}

// TestURing_VCloseDrainsInflightWrites verifies the close-drain: a VClose
// submitted right behind in-flight writes to the same slot is held until they
// complete, so every write lands (the slot is not cleared out from under a
// queued write) and its data is durable. It is deterministic — the coordinator
// holds the close until the slot's in-flight ops drain — so it holds under
// -count, unlike relying on the kernel not to race the close.
func TestURing_VCloseDrainsInflightWrites(t *testing.T) {
	s := newVURingSched(t, iosched.WithRingDepth(256), iosched.WithVFiles(2))
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

	_, err := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path,
		unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd).
		Link(iosched.VFallocateOp(vfd, int64(n*recLen))))
	require.NoError(t, err)

	// Fire N durable writes, then a close, without waiting between — the close
	// races the writes' completion, and the drain must hold it behind them.
	writes := make([]iosched.Ticket, n)
	for i := range writes {
		var err error
		writes[i], err = s.Submit(iosched.VWriteOp(vfd, rec(i), int64(i)*recLen).Durable())
		require.NoError(t, err)
	}
	closeTkt, err := s.Submit(iosched.VCloseOp(vfd))
	require.NoError(t, err)

	for i, w := range writes {
		n, err := w.Wait()
		require.NoErrorf(t, err, "write %d", i)
		require.Equal(t, recLen, n)
	}
	_, err = closeTkt.Wait()
	require.NoError(t, err)

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

// TestURing_DrainWaitsForInflightWrites_Regular submits durable writes followed
// immediately by DrainOp. Drain waits through their synthesized fdatasyncs but
// leaves the os.File open and caller-owned.
func TestURing_DrainWaitsForInflightWrites_Regular(t *testing.T) {
	s := newURingSched(t)
	const n = 64
	const recLen = 64

	f, err := os.CreateTemp(t.TempDir(), "drain-*.dat")
	require.NoError(t, err)
	path := f.Name()

	rec := func(i int) []byte {
		b := make([]byte, recLen)
		for j := range b {
			b[j] = byte('a' + (i+j)%26)
		}
		return b
	}

	writes := make([]iosched.Ticket, n)
	for i := range writes {
		writes[i], err = s.Submit(iosched.WriteOp(f, rec(i), int64(i)*recLen).Durable())
		require.NoError(t, err)
	}
	drainTkt, err := s.Submit(iosched.DrainOp(f))
	require.NoError(t, err)

	for i, w := range writes {
		n, err := w.Wait()
		require.NoErrorf(t, err, "write %d", i)
		require.Equal(t, recLen, n)
	}
	_, err = drainTkt.Wait()
	require.NoError(t, err)

	// Drain does not close f. The caller can still inspect it and then performs
	// the one real close through os.File, clearing its finalizer safely.
	_, err = f.Stat()
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Read back via a fresh fd after the caller-owned close.
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
	s := newVURingSched(t, iosched.WithRingDepth(16), iosched.WithVFiles(1))
	const vfd = uint32(0)
	const iters = 300
	dir := t.TempDir()

	for i := range iters {
		path := filepath.Join(dir, fmt.Sprintf("gen-%d.dat", i))
		want := fmt.Appendf(nil, "payload-for-generation-%d", i)

		_, err := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path,
			unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd))
		require.NoErrorf(t, err, "iter %d: open", i)

		// Durable write and close fired with no wait between: the close is held
		// until the write drains.
		w, err := s.Submit(iosched.VWriteOp(vfd, want, 0).Durable())
		require.NoError(t, err)
		cl, err := s.Submit(iosched.VCloseOp(vfd))
		require.NoError(t, err)
		_, err = w.Wait()
		require.NoErrorf(t, err, "iter %d: write", i)
		_, err = cl.Wait()
		require.NoErrorf(t, err, "iter %d: close", i)

		// Recycle the slot: reopen and read the data back.
		_, err = submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_RDWR, 0o600, vfd))
		require.NoErrorf(t, err, "iter %d: reopen", i)

		got := make([]byte, len(want))
		_, err = submitOne(t, s, iosched.VReadOp(vfd, got, 0))
		require.NoErrorf(t, err, "iter %d: read", i)
		require.Equalf(t, want, got, "iter %d: data", i)

		_, err = submitOne(t, s, iosched.VCloseOp(vfd))
		require.NoErrorf(t, err, "iter %d: close2", i)
	}
}

// TestURing_BarrierChainFollowsCompletionFunctions checks the lifecycle
// contract that a chain holding a close is placed only after the completion
// functions of the operations it waited for have returned: a completion
// function changes the buffer the held chain writes, and the change lands.
func TestURing_BarrierChainFollowsCompletionFunctions(t *testing.T) {
	s := newVURingSched(t, iosched.WithRingDepth(64), iosched.WithVFiles(1))
	const vfd = uint32(0)
	const bulk = 64 << 20 // long enough to be in flight when the chain is submitted
	path := filepath.Join(t.TempDir(), "barrier.dat")
	_, err := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR|unix.O_DIRECT, 0o600, vfd).
		Link(iosched.VFallocateOp(vfd, bulk+4096)))
	require.NoError(t, err)

	data := align.AllocAligned(bulk)
	defer align.FreeAligned(data)
	tail := align.AllocAligned(4096)
	defer align.FreeAligned(tail)
	copy(tail, "original")

	write, err := iosched.SubmitNotify(s, iosched.VWriteOp(vfd, data, 0), func(_ int, err error) {
		if err == nil {
			copy(tail, "modified")
		}
	})
	require.NoError(t, err)
	_, err = submitOne(t, s, iosched.VWriteOp(vfd, tail, bulk).Link(iosched.VCloseOp(vfd)))
	require.NoError(t, err)
	_, err = write.Wait()
	require.NoError(t, err)

	got := make([]byte, 8)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.ReadAt(got, bulk)
	require.NoError(t, err)
	require.Equal(t, "modified", string(got))
}

// TestURing_ReplaceSlotWhileBusy covers a chain that closes a slot and opens
// another file in it while a read of the first file is still in flight: it is
// accepted and waits for that read, and a read of the new file submitted while
// it is pending waits for it rather than being refused. A close submitted
// meanwhile is still refused. Refusals happen at acceptance, so they are
// ticket errors.
func TestURing_ReplaceSlotWhileBusy(t *testing.T) {
	s := newVURingSched(t, iosched.WithRingDepth(64), iosched.WithVFiles(1))
	const vfd = uint32(0)
	const bulk = 64 << 20
	dir := t.TempDir()
	first, second := filepath.Join(dir, "first"), filepath.Join(dir, "second")
	require.NoError(t, os.WriteFile(first, make([]byte, bulk), 0o600))
	require.NoError(t, os.WriteFile(second, []byte("second file"), 0o600))

	_, err := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, first, unix.O_RDONLY|unix.O_DIRECT, 0, vfd))
	require.NoError(t, err)
	big := align.AllocAligned(bulk)
	defer align.FreeAligned(big)
	inFlight, err := s.Submit(iosched.VReadOp(vfd, big, 0)) // holds the replacement pending
	require.NoError(t, err)

	replace, err := s.Submit(iosched.VCloseOp(vfd).Link(iosched.VOpenatOp(unix.AT_FDCWD, second, unix.O_RDONLY, 0, vfd)))
	require.NoError(t, err)
	got := make([]byte, len("second file"))
	read, err := s.Submit(iosched.VReadOp(vfd, got, 0))
	require.NoError(t, err)
	refused, err := s.Submit(iosched.VCloseOp(vfd))
	require.NoError(t, err)

	n, err := inFlight.Wait()
	require.NoError(t, err)
	require.Equal(t, bulk, n, "the read in flight finished against the first file")
	_, err = replace.Wait()
	require.NoError(t, err, "a replacement is accepted while the slot is busy")
	_, err = read.Wait()
	require.NoError(t, err, "a read submitted during the replacement waits for it")
	require.Equal(t, "second file", string(got))
	_, err = refused.Wait()
	require.Error(t, err, "a close during a pending replacement is still refused")

	_, err = submitOne(t, s, iosched.VCloseOp(vfd))
	require.NoError(t, err)
}
