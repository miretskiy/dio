//go:build linux

package ringo

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestGeneratedKernelABILayout(t *testing.T) {
	require.Equal(t, uintptr(64), unsafe.Sizeof(rawSQE{}))
	require.Equal(t, uintptr(16), unsafe.Sizeof(rawCQE{}))
	require.Equal(t, uintptr(120), unsafe.Sizeof(rawParams{}))
	require.Equal(t, uintptr(40), unsafe.Sizeof(rawSQOffsets{}))
	require.Equal(t, uintptr(40), unsafe.Sizeof(rawCQOffsets{}))
	require.Equal(t, uintptr(16), unsafe.Sizeof(rawFilesUpdate{}))
	require.Equal(t, uintptr(32), unsafe.Sizeof(rawRsrcRegister{}))
	require.Equal(t, uintptr(64), unsafe.Sizeof(rawSyncCancelReg{}))
	require.Equal(t, uintptr(8), unsafe.Sizeof(rawProbeOp{}))
	require.Equal(t, uintptr(16), unsafe.Sizeof(rawTimespec{}))

	// io_probe copies back sizeof(io_uring_probe) + nr_ops*sizeof(probe_op)
	// bytes, so a header size or padding change would misplace every entry.
	require.Equal(t, uintptr(16), unsafe.Sizeof(rawProbeHeader{}))
	require.Equal(t,
		unsafe.Sizeof(rawProbeHeader{})+
			uintptr(rawOpLast)*unsafe.Sizeof(rawProbeOp{}),
		unsafe.Sizeof(rawProbe{}),
	)
}
