//go:build linux

package iosched

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/miretskiy/dio/giouring"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestDurableWrite(t *testing.T) {
	regular := os.NewFile(100, "regular")
	durable := func(ops ...Op) bool {
		c := newTestCoordinator(8, 1, &fakeRingQueue{})
		_, handles := acceptOps(&c, ops...)
		got := c.durableWrite(handles)
		c.failRemaining(nil, errors.New("test cleanup"))
		return got
	}
	require.True(t, durable(VWriteOp(0, make([]byte, 8), 0).Durable()))
	require.False(t, durable(VWriteOp(0, make([]byte, 8), 0)))
	require.True(t, durable(WriteOp(regular, make([]byte, 8), 0).Durable()))
	require.True(t, durable(
		VWriteOp(0, make([]byte, 8), 0),
		VWriteOp(0, make([]byte, 8), 8).Durable(),
	))
	require.False(t, durable(VReadOp(0, make([]byte, 8), 0)))

	virtual := VWriteOp(3, nil, 0).syncOp()
	require.Equal(t, OpFdatasync, virtual.kind())
	require.True(t, virtual.isVirtual())
	require.Equal(t, uint32(3), virtual.vfd)

	regSync := WriteOp(regular, nil, 0).syncOp()
	require.Equal(t, OpFdatasync, regSync.kind())
	require.Same(t, regular, regSync.f)
}

func TestPlaceDurableWriteWithLinkedSync(t *testing.T) {
	ring := &fakeRingQueue{}
	c := newTestCoordinator(2, 1, ring)
	tickets, _ := acceptOps(&c, VWriteOp(0, make([]byte, 8), 0).Durable())

	c.placeReady(true)
	require.Len(t, ring.sqes, 2)
	require.Equal(t, uint8(giouring.OpWrite), ring.sqes[0].OpCode)
	require.NotZero(t, ring.sqes[0].Flags&giouring.SqeIOLink)
	require.Equal(t, uint8(giouring.OpFsync), ring.sqes[1].OpCode)
	require.Equal(t, giouring.FsyncDatasync, ring.sqes[1].OpcodeFlags)

	c.failRemaining(nil, errors.New("test cleanup"))
	c.releaseAllSlots()
	tickets[0].Wait()
}

func TestPlacePlainWriteWithoutSync(t *testing.T) {
	ring := &fakeRingQueue{}
	c := newTestCoordinator(2, 1, ring)
	tickets, _ := acceptOps(&c, VWriteOp(0, make([]byte, 8), 0))

	c.placeReady(true)
	require.Len(t, ring.sqes, 1)
	require.Equal(t, uint8(giouring.OpWrite), ring.sqes[0].OpCode)
	require.Zero(t, ring.sqes[0].Flags&giouring.SqeIOLink)

	c.failRemaining(nil, errors.New("test cleanup"))
	c.releaseAllSlots()
	tickets[0].Wait()
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

func TestURingDurableWrite(t *testing.T) {
	s := newDurabilitySched(t)
	path := filepath.Join(t.TempDir(), "durable.dat")

	open, err := s.Submit(VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, 0))
	require.NoError(t, err)
	open.Wait()
	require.NoError(t, open.Error())

	w, err := s.Submit(VWriteOp(0, []byte("durable payload"), 0).Durable())
	require.NoError(t, err)
	w.Wait()
	require.NoError(t, w.Error())

	cl, err := s.Submit(VCloseOp(0))
	require.NoError(t, err)
	cl.Wait()
}

func TestURingLinkedOpenChain(t *testing.T) {
	s := newDurabilitySched(t)
	path := filepath.Join(t.TempDir(), "linked-open.dat")
	op := VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, 1).
		Link(VFallocateOp(1, 4096), VWriteOp(1, []byte("payload"), 0))
	ticket, err := s.Submit(op)
	require.NoError(t, err)
	ticket.Wait()
	require.NoErrorf(t, ticket.Error(), "root count %d", ticket.N())

	closeTicket, err := s.Submit(VCloseOp(1))
	require.NoError(t, err)
	closeTicket.Wait()
	require.NoError(t, closeTicket.Error())
}
