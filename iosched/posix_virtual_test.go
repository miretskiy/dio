package iosched_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/iosched"
)

// The POSIXScheduler emulates the virtual-file ops with a userspace descriptor
// table so virtual-file consumers run on non-io_uring hosts (and in tests on
// any platform). These tests pin NewPOSIXScheduler directly rather than the
// default scheduler so they exercise the emulation even on Linux.

func TestPOSIXVirtualOpenFallocateWriteChain(t *testing.T) {
	s := iosched.NewPOSIXScheduler()
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	path := filepath.Join(t.TempDir(), "slab.dat")
	const vfd = uint32(3)
	const size = int64(1 << 16)
	payload := []byte("openat + fallocate + write, fused")

	// One submission: open the slab into vfd, preallocate it, write the first
	// record. This is the create-and-first-write fusion the slab write path uses.
	chain := iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, vfd).
		Link(iosched.VFallocateOp(vfd, size), iosched.VWriteOp(vfd, payload, 0))
	tkt := submitAndWait(t, s, chain)
	require.NoError(t, tkt.Error())

	// Fallocate grew the file to at least size; the write landed at offset 0.
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, info.Size(), size)

	got := make([]byte, len(payload))
	rd := submitAndWait(t, s, iosched.VReadOp(vfd, got, 0))
	require.NoError(t, rd.Error())
	require.Equal(t, len(payload), rd.N())
	require.Equal(t, payload, got)

	// Close the slot; ops on it then fail (the table entry is gone).
	cl := submitAndWait(t, s, iosched.VCloseOp(vfd))
	require.NoError(t, cl.Error())

	after := submitAndWait(t, s, iosched.VWriteOp(vfd, payload, 0))
	require.Error(t, after.Error())
}

// TestPOSIXVirtualSlotRecycle documents that on the POSIX backend a slot can be
// closed and immediately reopened because Submit completes synchronously. On the
// io_uring backend callers wait for the close ticket before reusing the slot.
func TestPOSIXVirtualSlotRecycle(t *testing.T) {
	s := iosched.NewPOSIXScheduler()
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	dir := t.TempDir()
	const vfd = uint32(0)

	for i, name := range []string{"a.dat", "b.dat"} {
		path := filepath.Join(dir, name)
		want := []byte{byte('A' + i)}

		op := submitAndWait(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, vfd))
		require.NoError(t, op.Error())

		w := submitAndWait(t, s, iosched.VWriteOp(vfd, want, 0))
		require.NoError(t, w.Error())

		c := submitAndWait(t, s, iosched.VCloseOp(vfd))
		require.NoError(t, c.Error())
	}
}

func TestFallocateOpGrowsFile(t *testing.T) {
	s := iosched.NewPOSIXScheduler()
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	path := filepath.Join(t.TempDir(), "grow.dat")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	const size = int64(1 << 20)
	tkt := submitAndWait(t, s, iosched.FallocateOp(f, size))
	require.NoError(t, tkt.Error())

	info, err := f.Stat()
	require.NoError(t, err)
	require.GreaterOrEqual(t, info.Size(), size)
}
