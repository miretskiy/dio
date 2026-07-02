//go:build linux

package iosched

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestOpenatSyncMode(t *testing.T) {
	require.Equal(t, syncNone, openatSyncMode(os.O_RDWR))
	require.Equal(t, syncData, openatSyncMode(os.O_RDWR|syscall.O_DSYNC))
	require.Equal(t, syncFull, openatSyncMode(os.O_RDWR|syscall.O_SYNC))
	// O_SYNC implies O_DSYNC on Linux; it must classify as full, not data.
	require.Equal(t, syncFull, openatSyncMode(os.O_RDWR|syscall.O_SYNC|syscall.O_DSYNC))

	require.Equal(t, os.O_RDWR, stripSyncFlags(os.O_RDWR|syscall.O_SYNC))
	require.Equal(t, os.O_RDWR, stripSyncFlags(os.O_RDWR|syscall.O_DSYNC))
	require.Equal(t, syncNone, openatSyncMode(stripSyncFlags(os.O_RDWR|syscall.O_SYNC)))
}

// TestLinkDurableSyncs checks the link is set up correctly: a durable-slot write
// gets exactly one linked sync (fdatasync for O_DSYNC, fsync for O_SYNC) with
// pending grown by one; a coalesced run's leader gets a single sync, not one per
// follower; plain slots and regular files are left untouched.
func TestLinkDurableSyncs(t *testing.T) {
	c := coordinator{syncMode: make([]syncMode, 4)}
	c.noteSyncMode(0, syncData) // O_DSYNC
	c.noteSyncMode(1, syncFull) // O_SYNC
	// slots 2, 3 stay syncNone

	mk := func(op Op) *Ticket {
		tk := &Ticket{Op: op}
		tk.pending.Store(1)
		return tk
	}
	dsync := mk(VWriteOp(0, make([]byte, 8), 0))
	fsync := mk(VWriteOp(1, make([]byte, 8), 0))
	plain := mk(VWriteOp(2, make([]byte, 8), 0))
	reg := mk(WriteOp(os.NewFile(100, "r"), make([]byte, 8), 0))
	// A coalesced writev leader on a durable slot, with one follower.
	leader := mk(VWriteOp(0, make([]byte, 8), 0))
	leader.Op.opcode = OpWritev | opVirtual
	leader.group = mk(VWriteOp(0, make([]byte, 8), 8))

	c.linkDurableSyncs([]*Ticket{dsync, fsync, plain, reg, leader})

	require.NotNil(t, dsync.Op.linked)
	require.Equal(t, OpFdatasync, dsync.Op.linked.base())
	require.True(t, dsync.Op.isLinked())
	require.Equal(t, int32(2), dsync.pending.Load())

	require.NotNil(t, fsync.Op.linked)
	require.Equal(t, OpFsync, fsync.Op.linked.base())
	require.Equal(t, int32(2), fsync.pending.Load())

	require.Nil(t, plain.Op.linked)
	require.Equal(t, int32(1), plain.pending.Load())
	require.Nil(t, reg.Op.linked) // regular (non-virtual) file: not tracked
	require.Equal(t, int32(1), reg.pending.Load())

	// Amortization: the whole coalesced run gets one sync, not one per member.
	require.NotNil(t, leader.Op.linked)
	require.Equal(t, OpFdatasync, leader.Op.linked.base())
	require.Equal(t, int32(2), leader.pending.Load())
	require.Nil(t, leader.group.Op.linked)
}

func newDurabilitySched(t *testing.T) *URingScheduler {
	t.Helper()
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	s, err := NewURingScheduler(URingConfig{RingDepth: 16, VFiles: 2})
	if err != nil {
		t.Skipf("io_uring virtual files not available on this kernel: %v", err)
	}
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// TestURingDurableSyncOrder observes the completion order end-to-end: a write to
// an O_DSYNC slot succeeds, and the linked fdatasync's CQE is reaped after the
// write's (write.seq < fdatasync.seq). That ordering only holds if the sync is
// chained behind the write, which is exactly the durability guarantee.
func TestURingDurableSyncOrder(t *testing.T) {
	s := newDurabilitySched(t)
	path := filepath.Join(t.TempDir(), "durable.dat")

	open, err := s.Submit(VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR|unix.O_DSYNC, 0o600, 0))
	require.NoError(t, err)
	open.Wait()
	require.NoError(t, open.Error())
	open.Release()

	w, err := s.Submit(VWriteOp(0, []byte("durable payload"), 0))
	require.NoError(t, err)
	w.Wait()
	require.NoError(t, w.Error())

	require.NotNil(t, w.Op.linked)
	require.Equal(t, OpFdatasync, w.Op.linked.base())
	require.NotZero(t, w.Op.Result.seq)
	require.NotZero(t, w.Op.linked.Result.seq)
	require.Less(t, w.Op.Result.seq, w.Op.linked.Result.seq, "write must complete before its linked fdatasync")
	w.Release()

	cl, err := s.Submit(VCloseOp(0))
	require.NoError(t, err)
	cl.Wait()
	cl.Release()
}
