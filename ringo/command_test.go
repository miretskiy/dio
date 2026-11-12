//go:build linux

package ringo

import (
	"encoding/binary"
	"runtime"
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestCommandExtendedQueues(t *testing.T) {
	ring, transport := newFakeRing(2)
	raw := newHeapRingFlags(2, 4, rawSetupSQE128|rawSetupCQE32)
	raw.hooks = ring.backend.hooks
	ring.backend = raw
	transport.raw = raw
	defer ring.Close()
	var alloc OpAlloc
	buffer := make([]byte, 4096)
	for cycle := 0; cycle < 9; cycle++ {
		payload := make([]byte, 72)
		payload[0] = 0x02
		payload[71] = 0xab
		op := UringCmd(BorrowedFD(17), 0xc0484e80, payload, []CommandBuffer{{Offset: 24, Buffer: buffer}}, WithAlloc(&alloc))
		payload[71] = 0 // Constructor owns a copy.
		h, err := ring.Push(op)
		require.NoError(t, err)
		nop, err := ring.Push(Nop())
		require.NoError(t, err)
		command := raw.sqeAt(uint32(cycle * 2))
		require.Equal(t, uint8(rawOpUringCmd), command.Opcode)
		require.Equal(t, int32(17), command.Fd)
		bytes := unsafe.Slice((*byte)(unsafe.Pointer(command)), 128)
		require.Equal(t, uint32(0xc0484e80), binary.NativeEndian.Uint32(bytes[8:12]))
		require.Equal(t, byte(0xab), bytes[48+71])
		require.Equal(t, uint64(slicePtr(buffer)), binary.NativeEndian.Uint64(bytes[48+24:]))
		require.Equal(t, uintptr(128), uintptr(unsafe.Pointer(raw.sqeAt(uint32(cycle*2+1))))-uintptr(unsafe.Pointer(command)))
		_, err = ring.Submit()
		require.NoError(t, err)
		tail := *raw.cq.tail
		transport.complete(h, 0x123, 0)
		extra := (*[2]uint64)(unsafe.Add(unsafe.Pointer(raw.cqeAt(tail)), 16))
		*extra = [2]uint64{0x0123456789abcdef, 0xfedcba9876543210}
		transport.complete(nop, 0, 0)
		var completions []Completion
		for c := range ring.Reap() {
			completions = append(completions, c)
		}
		require.Len(t, completions, 2)
		require.Equal(t, 0x123, completions[0].Result) // Positive driver status is preserved.
		require.NoError(t, completions[0].Err)
		require.Equal(t, *extra, completions[0].Extra)
		require.Equal(t, nop, completions[1].Handle)
	}
	// Reuse the command slot for a normal operation: extension must be cleared.
	h, err := ring.Push(Nop())
	require.NoError(t, err)
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(raw.sqeAt(18))), 128)
	require.Equal(t, make([]byte, 64), bytes[64:])
	transport.complete(h, -int32(syscall.EIO), 0)
	for c := range ring.Reap() {
		require.ErrorIs(t, c.Err, syscall.EIO)
	}
	require.Len(t, alloc.commands.free, 1)
	require.Nil(t, alloc.commands.free[0].buffers)
}

func TestCommandValidation(t *testing.T) {
	ring, _ := newFakeRing(8)
	defer ring.Close()
	_, err := ring.Push(UringCmd(BorrowedFD(1), 0, make([]byte, 72), nil))
	require.ErrorContains(t, err, "WithUringCmd")
	ring.backend.flags |= rawSetupSQE128 | rawSetupCQE32 // Validation only; no SQE is prepared below.
	for _, op := range []Op{
		UringCmd(BorrowedFD(1), 0, make([]byte, 81), nil),
		UringCmd(BorrowedFD(1), 0, make([]byte, 81), []CommandBuffer{{Offset: 73}}),
		UringCmd(BorrowedFD(1), 0, make([]byte, 72), []CommandBuffer{{Offset: 65}}),
		UringCmd(BorrowedFD(1), 0, make([]byte, 72), []CommandBuffer{{Offset: 16}, {Offset: 20}}),
	} {
		_, err := ring.Push(op)
		require.Error(t, err)
	}
	require.Zero(t, *ring.backend.sq.tail)
	require.Zero(t, ring.backend.sq.sqeTail)
}

func TestCommandKernelExtendedEntries(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(4), WithUringCmd())
	for batch := 0; batch < 12; batch++ {
		for i := 0; i < 4; i++ {
			_, err := ring.Push(Nop())
			require.NoError(t, err)
		}
		completions := awaitCompletions(t, ring, 4)
		for _, c := range completions {
			require.Zero(t, c.Result)
			require.Equal(t, [2]uint64{}, c.Extra)
		}
	}
}

func TestCommandBufferEscapes(t *testing.T) {
	ring, transport := newFakeRing(1)
	raw := newHeapRingFlags(1, 2, rawSetupSQE128|rawSetupCQE32)
	raw.hooks = ring.backend.hooks
	ring.backend = raw
	transport.raw = raw
	defer ring.Close()
	var buffer [4096]byte
	descriptors := []CommandBuffer{{Offset: 24, Buffer: buffer[:]}}
	h, err := ring.Push(UringCmd(BorrowedFD(1), 1, make([]byte, 72), descriptors))
	require.NoError(t, err)
	clear(descriptors)
	before := binary.NativeEndian.Uint64(unsafe.Slice((*byte)(unsafe.Pointer(raw.sqeAt(0))), 128)[72:])
	growEscapeTestStack(64)
	runtime.GC()
	owned := ring.pending.Value(h.slot).op.(*uringCmdOp)
	require.Equal(t, before, uint64(slicePtr(owned.buffers[0].Buffer)))
	require.Equal(t, before, uint64(slicePtr(buffer[:])))
	transport.complete(h, 0, 0)
	for range ring.Reap() {
	}
}
