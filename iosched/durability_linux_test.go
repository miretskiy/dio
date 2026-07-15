//go:build linux

package iosched_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/iosched"
)

// TestURing_DurableVWrite_IssuesLinkedSync verifies durability is issued where
// the write asks for it and nowhere else: a Durable write places two SQEs (write
// + linked fdatasync), a plain write to the same kind of slot places one, and the
// data still lands. The SQE counts come from Stats.OpsPlaced, so this observes
// behavior without reaching into the ring.
func TestURing_DurableVWrite_IssuesLinkedSync(t *testing.T) {
	s := newVURingSched(t, iosched.URingConfig{RingDepth: 16, VFiles: 4})
	dir := t.TempDir()

	openSlot := func(vfd uint32, name string) {
		tk := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, filepath.Join(dir, name),
			unix.O_CREAT|unix.O_RDWR, 0o600, vfd))
		require.NoError(t, tk.Error())
	}
	openSlot(0, "durable.dat")
	openSlot(1, "plain.dat")

	payload := []byte("durable-payload")
	placed := func(op iosched.Op) uint64 {
		before := s.Stats().OpsPlaced
		tk := submitOne(t, s, op)
		require.NoError(t, tk.Error())
		require.Equal(t, len(payload), tk.N())
		return s.Stats().OpsPlaced - before
	}
	require.Equal(t, uint64(2), placed(iosched.VWriteOp(0, payload, 0).Durable())) // write + fdatasync
	require.Equal(t, uint64(1), placed(iosched.VWriteOp(1, payload, 0)))           // write only

	got := make([]byte, len(payload))
	rd := submitOne(t, s, iosched.VReadOp(0, got, 0))
	require.NoError(t, rd.Error())
	require.Equal(t, payload, got)

	for _, vfd := range []uint32{0, 1} {
		cl := submitOne(t, s, iosched.VCloseOp(vfd))
		require.NoError(t, cl.Error())
	}
}
