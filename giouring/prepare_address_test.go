//go:build linux

package giouring

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestPrepareSliceBackedAddresses(t *testing.T) {
	name := []byte("user.key\x00")
	value := make([]byte, 32)
	entry := &SubmissionQueueEntry{}

	entry.PrepareFgetxattr(10, name, value)

	require.Equal(t, uint64(uintptr(unsafe.Pointer(unsafe.SliceData(value)))), entry.Off)
	require.Equal(t, uint64(uintptr(unsafe.Pointer(unsafe.SliceData(name)))), entry.Addr)
}
