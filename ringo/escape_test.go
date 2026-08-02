//go:build linux

package ringo

import (
	"runtime"
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func growEscapeTestStack(depth int) byte {
	var padding [1024]byte
	padding[0] = byte(depth)
	if depth != 0 {
		padding[0] ^= growEscapeTestStack(depth - 1)
	}
	runtime.KeepAlive(&padding)
	return padding[0]
}

func sliceLiteralAddressIsStable() bool {
	ring, fake := newFakeRing(1)
	handle, err := ring.Push(Read(BorrowedFD(1), []byte{1, 2, 3}, 0))
	if err != nil {
		panic(err)
	}
	before := fake.sqe(0).Addr
	growEscapeTestStack(64)
	op := ring.pending.Value(handle.slot).op.(*readOp)
	after := uint64(slicePtr(op.buffer))
	runtime.KeepAlive(ring)
	return before == after
}

func arraySliceAddressIsStable() bool {
	var buffer [5]byte
	ring, fake := newFakeRing(1)
	handle, err := ring.Push(Read(BorrowedFD(1), buffer[:], 0))
	if err != nil {
		panic(err)
	}
	before := fake.sqe(0).Addr
	growEscapeTestStack(64)
	op := ring.pending.Value(handle.slot).op.(*readOp)
	after := uint64(slicePtr(op.buffer))
	runtime.KeepAlive(ring)
	return before == after
}

func copiedStructAddressIsStable() bool {
	ring, fake := newFakeRing(1)
	handle, err := ring.Push(Timeout(syscall.Timespec{Sec: 1}, 1, 0))
	if err != nil {
		panic(err)
	}
	before := fake.sqe(0).Addr
	growEscapeTestStack(64)
	op := ring.pending.Value(handle.slot).op.(*timeoutOp)
	after := uint64(uintptr(unsafe.Pointer(&op.spec)))
	runtime.KeepAlive(ring)
	return before == after
}

func retainedOutputAddressIsStable() bool {
	var result unix.Statx_t
	ring, fake := newFakeRing(1)
	handle, err := ring.Push(StatxAt(
		BorrowedFD(1),
		"",
		unix.AT_EMPTY_PATH,
		unix.STATX_SIZE,
		&result,
	))
	if err != nil {
		panic(err)
	}
	before := fake.sqe(0).Off
	growEscapeTestStack(64)
	op := ring.pending.Value(handle.slot).op.(*statxOp)
	after := uint64(uintptr(unsafe.Pointer(op.result)))
	runtime.KeepAlive(ring)
	return before == after
}

func TestManagedMemoryRemainsStableAcrossStackGrowth(t *testing.T) {
	const attempts = 10_000
	for attempt := 0; attempt < attempts; attempt++ {
		result := make(chan [4]bool)
		go func() {
			result <- [4]bool{
				sliceLiteralAddressIsStable(),
				arraySliceAddressIsStable(),
				copiedStructAddressIsStable(),
				retainedOutputAddressIsStable(),
			}
		}()
		stable := <-result
		require.Truef(t,
			stable[0] && stable[1] && stable[2] && stable[3],
			"kernel-visible address moved: literal=%t array=%t copied=%t output=%t",
			stable[0], stable[1], stable[2], stable[3],
		)
	}
}
