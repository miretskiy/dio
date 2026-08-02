//go:build linux && cgo

package ringo

import (
	"testing"
	"unsafe"

	"github.com/miretskiy/dio/ringo/internal/testdata/liburingoracle"
	"github.com/stretchr/testify/require"
)

func TestGeneratedABILayoutMatchesLiburing(t *testing.T) {
	want := liburingoracle.Sizes()
	require.Equal(t, want.SQE, unsafe.Sizeof(rawSQE{}))
	require.Equal(t, want.CQE, unsafe.Sizeof(rawCQE{}))
	require.Equal(t, want.Params, unsafe.Sizeof(rawParams{}))
	require.Equal(t, want.SQOffsets, unsafe.Sizeof(rawSQOffsets{}))
	require.Equal(t, want.CQOffsets, unsafe.Sizeof(rawCQOffsets{}))
	require.Equal(t, want.FilesUpdate, unsafe.Sizeof(rawFilesUpdate{}))
	require.Equal(t, want.ResourceRegister, unsafe.Sizeof(rawRsrcRegister{}))
	require.Equal(t, want.SyncCancel, unsafe.Sizeof(rawSyncCancelReg{}))
	require.Equal(t, want.ProbeOperation, unsafe.Sizeof(rawProbeOp{}))
}

func TestRawConstantsMatchLiburing(t *testing.T) {
	want := liburingoracle.Constants()
	require.Equal(t, want.SQEFlags[:], rawValues(
		rawSqeFixedFile,
		rawSqeIODrain,
		rawSqeIOLink,
		rawSqeIOHardlink,
	))
	require.Equal(t, want.SetupFlags[:], rawValues(
		rawSetupIOPoll,
		rawSetupSQPoll,
		rawSetupSQAff,
		rawSetupCQSize,
		rawSetupClamp,
		rawSetupSubmitAll,
		rawSetupCoopTaskrun,
		rawSetupTaskrunFlag,
		rawSetupNoSQArray,
		rawSetupHybridIOPoll,
	))
	require.Equal(t, want.CQEFlags[:], rawValues(
		rawCQEMore,
	))
	require.Equal(t, want.Features[:], rawValues(
		rawFeatSingleMMap,
		rawFeatNoDrop,
		rawFeatLinkedFile,
	))
	require.Equal(t, want.SQFlags[:], rawValues(
		rawSQNeedWakeup,
		rawSQCQOverflow,
		rawSQTaskrun,
	))
	require.Equal(t, want.EnterFlags[:], rawValues(
		rawEnterGetEvents,
		rawEnterSQWakeup,
	))
	require.Equal(t, want.RegisterOps[:], rawValues(
		rawRegisterBuffers,
		rawRegisterFiles,
		rawRegisterFilesUpdate,
		rawRegisterEventFD,
		rawUnregisterEventFD,
		rawRegisterEventFDAsync,
		rawRegisterProbe,
		rawRegisterFiles2,
		rawRegisterSyncCancel,
	))
	require.Equal(t, want.RsrcSparse, rawRsrcRegisterSparse)
	require.Equal(t, want.CancelFlags[:], rawValues(
		rawAsyncCancelAll,
		rawAsyncCancelFD,
		rawAsyncCancelAny,
		rawAsyncCancelFDFixed,
	))
	require.Equal(t, want.MmapOffsets, [3]uint64{
		rawSQRingOffset,
		rawCQRingOffset,
		rawSQEsOffset,
	})
}

func rawValues[T ~uint8 | ~uint16 | ~uint32](values ...T) []uint32 {
	result := make([]uint32, len(values))
	for index, value := range values {
		result[index] = uint32(value)
	}
	return result
}

func rawSQEBytes(sqe rawSQE) liburingoracle.SQE {
	return *(*liburingoracle.SQE)(unsafe.Pointer(&sqe))
}

func withSQEFlags(sqe liburingoracle.SQE, flags rawSQEFlags) liburingoracle.SQE {
	(*rawSQE)(unsafe.Pointer(&sqe)).Flags |= uint8(flags)
	return sqe
}

func materializeOperation(op Op) rawSQE {
	pending := pendingSlot{
		op:     op,
		handle: Handle{slot: 0x7fff},
	}
	var sqe rawSQE
	bytes := (*[64]byte)(unsafe.Pointer(&sqe))
	for index := range bytes {
		bytes[index] = 0xa5
	}
	new(Ring).prepareSQE(&sqe, &pending)
	sqe.User_data = 0
	return sqe
}
