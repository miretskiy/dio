//go:build linux

// MIT License
//
// Copyright (c) 2023 Paweł Gaczyński
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
// TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
// SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package giouring

import (
	"errors"
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestIOUringMlockSizeParams(t *testing.T) {
	p := &Params{}
	size, err := MlockSizeParams(1024, p)
	require.NoError(t, err)

	newerKernel, err := CheckKernelVersion(5, 12, 0)
	require.NoError(t, err)
	if newerKernel {
		require.Zero(t, size)
	}
}

func TestRoundupPow2(t *testing.T) {
	for _, tc := range []struct {
		depth uint32
		want  uint32
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{1024, 1024},
		{1025, 2048},
	} {
		require.Equal(t, tc.want, roundupPow2(tc.depth))
	}
}

func TestAllocHugeUsesKernelEntrySizes(t *testing.T) {
	const entries = 1024
	for _, tc := range []struct {
		name  string
		flags uint32
		want  uint
	}{
		{name: "standard", want: 106496},
		{name: "without SQ array", flags: SetupNoSQArray, want: 102400},
		{name: "large entries", flags: SetupSQE128 | SetupCQE32, want: 204800},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, 256*1024)
			p := &Params{flags: tc.flags}
			ring := NewRing()

			got, err := allocHuge(entries, p, ring.sqRing, ring.cqRing,
				unsafe.Pointer(unsafe.SliceData(buf)), uint64(len(buf)))

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Zero(t, ring.sqRing.sqesSize, "caller-owned storage must not be marked for unmapping")
		})
	}
}

func TestAllocHugeTracksOwnedMappings(t *testing.T) {
	p := &Params{}
	ring := NewRing()

	_, err := allocHuge(8, p, ring.sqRing, ring.cqRing, nil, 0)
	require.NoError(t, err)
	require.NotZero(t, ring.sqRing.sqesSize)
	t.Cleanup(func() {
		require.NoError(t, sysMunmap(unsafe.Pointer(ring.sqRing.sqes), uintptr(ring.sqRing.sqesSize)))
		UnmapRings(ring.sqRing, ring.cqRing)
	})
}

func TestQueueInitMemLeavesCallerMappingOwnedByCaller(t *testing.T) {
	const mappingSize = uintptr(2 * 1024 * 1024)
	ptr, err := sysMmap(mappingSize, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS, -1, 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sysMunmap(ptr, mappingSize))
	})

	ring := NewRing()
	p := &Params{}
	err = ring.QueueInitMem(8, p, ptr, uint64(mappingSize))
	if errors.Is(err, syscall.EINVAL) {
		t.Skip("kernel does not support IORING_SETUP_NO_MMAP")
	}
	require.NoError(t, err)

	ring.QueueExit()
	require.NoError(t, sysMadvise(uintptr(ptr), mappingSize, syscall.MADV_NORMAL),
		"QueueExit must not unmap caller-provided ring memory")
}

func TestSetupReturnsNilErrorOnSuccess(t *testing.T) {
	fd, err := Setup(8, &Params{})
	require.NoError(t, err)
	require.NoError(t, syscall.Close(int(fd)))
}
