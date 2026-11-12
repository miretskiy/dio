//go:build linux && cgo

package ringo

import (
	"testing"
	"unsafe"

	"github.com/miretskiy/dio/v2/ringo/internal/testdata/liburingoracle"
	"github.com/stretchr/testify/require"
)

func TestCommandMatchesLiburing(t *testing.T) {
	var payload [80]byte
	for i := range payload {
		payload[i] = byte(i + 1)
	}
	op := UringCmd(BorrowedFD(7), 0xc0484e80, payload[:], nil)
	var sqe [16]uint64 // 128 bytes, aligned like a real SQE.
	op.prepare((*rawSQE)(unsafe.Pointer(&sqe)))
	want := liburingoracle.Command(7, 0xc0484e80, payload)
	require.Equal(t, want, *(*[128]byte)(unsafe.Pointer(&sqe)))
}
