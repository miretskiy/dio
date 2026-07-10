//go:build linux

package iosched

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestLinkDurableSyncs checks the link is set up from each write's durability
// flag: a Durable write gets one linked fdatasync with pending grown by one; a
// plain write is untouched; a coalesced writev leader carrying the durable flag
// gets a single sync, not one per follower. It covers both virtual and regular
// targets — durability is uniform.
func TestLinkDurableSyncs(t *testing.T) {
	mk := func(op Op) *Ticket {
		tk := &Ticket{Op: op}
		tk.pending.Store(1)
		return tk
	}
	vdata := mk(VWriteOp(0, make([]byte, 8), 0).Durable())
	vplain := mk(VWriteOp(2, make([]byte, 8), 0))
	regData := mk(WriteOp(os.NewFile(100, "r"), make([]byte, 8), 0).Durable())
	// A coalesced writev leader carrying the durable flag, with one follower.
	leader := mk(VWriteOp(0, make([]byte, 8), 0).Durable())
	leader.Op.opcode = OpWritev | opVirtual
	leader.group = mk(VWriteOp(0, make([]byte, 8), 8))

	linkDurableSyncs([]*Ticket{vdata, vplain, regData, leader})

	require.NotNil(t, vdata.Op.linked)
	require.Equal(t, OpFdatasync, vdata.Op.linked.kind())
	require.True(t, vdata.Op.isLinked())
	require.Equal(t, int32(2), vdata.pending.Load())

	require.Nil(t, vplain.Op.linked)
	require.Equal(t, int32(1), vplain.pending.Load())

	// Regular files get durability too now (uniform, per-op — not vfile-only).
	require.NotNil(t, regData.Op.linked)
	require.Equal(t, OpFdatasync, regData.Op.linked.kind())
	require.Equal(t, int32(2), regData.pending.Load())

	// Amortization: the whole coalesced run gets one sync, not one per member.
	require.NotNil(t, leader.Op.linked)
	require.Equal(t, OpFdatasync, leader.Op.linked.kind())
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

// TestURingDurableSyncOrder observes the completion order end-to-end: a Durable
// write succeeds, and the linked fdatasync's CQE is reaped after the write's
// (write.seq < fdatasync.seq). That ordering only holds if the sync is chained
// behind the write, which is exactly the durability guarantee.
func TestURingDurableSyncOrder(t *testing.T) {
	s := newDurabilitySched(t)
	path := filepath.Join(t.TempDir(), "durable.dat")

	open, err := s.Submit(VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, 0))
	require.NoError(t, err)
	open.Wait()
	require.NoError(t, open.Error())
	open.Release()

	w, err := s.Submit(VWriteOp(0, []byte("durable payload"), 0).Durable())
	require.NoError(t, err)
	w.Wait()
	require.NoError(t, w.Error())

	require.NotNil(t, w.Op.linked)
	require.Equal(t, OpFdatasync, w.Op.linked.kind())
	require.NotZero(t, w.Op.result.seq)
	require.NotZero(t, w.Op.linked.result.seq)
	require.Less(t, w.Op.result.seq, w.Op.linked.result.seq, "write must complete before its linked fdatasync")
	w.Release()

	cl, err := s.Submit(VCloseOp(0))
	require.NoError(t, err)
	cl.Wait()
	cl.Release()
}
