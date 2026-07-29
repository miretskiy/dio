//go:build ignore

package ringo

/*
#include <liburing/io_uring.h>
*/
import "C"

// This file is input to cgo -godefs. It is not part of the ringo package
// build. Run go generate on Linux after updating the bundled UAPI header.

type rawOpcode uint8
type rawSQEFlags uint8
type rawCQEFlags uint32
type rawSetupFlags uint32
type rawCancelFlags uint32
type rawSQFlags uint32
type rawEnterFlags uint32
type rawFeatureFlags uint32
type rawRegisterOpcode uint32

type rawSQE C.struct_io_uring_sqe
type rawCQE C.struct_io_uring_cqe
type rawParams C.struct_io_uring_params
type rawSQOffsets C.struct_io_sqring_offsets
type rawCQOffsets C.struct_io_cqring_offsets
type rawFilesUpdate C.struct_io_uring_files_update
type rawRsrcRegister C.struct_io_uring_rsrc_register
type rawTimespec C.struct___kernel_timespec
type rawSyncCancelReg C.struct_io_uring_sync_cancel_reg
type rawProbeHeader C.struct_io_uring_probe
type rawProbeOp C.struct_io_uring_probe_op

const (
	rawSqeFixedFile  rawSQEFlags = C.IOSQE_FIXED_FILE
	rawSqeIODrain    rawSQEFlags = C.IOSQE_IO_DRAIN
	rawSqeIOLink     rawSQEFlags = C.IOSQE_IO_LINK
	rawSqeIOHardlink rawSQEFlags = C.IOSQE_IO_HARDLINK

	rawCQEMore rawCQEFlags = C.IORING_CQE_F_MORE

	rawSetupIOPoll       rawSetupFlags   = C.IORING_SETUP_IOPOLL
	rawSetupSQPoll       rawSetupFlags   = C.IORING_SETUP_SQPOLL
	rawSetupSQAff        rawSetupFlags   = C.IORING_SETUP_SQ_AFF
	rawSetupCQSize       rawSetupFlags   = C.IORING_SETUP_CQSIZE
	rawSetupClamp        rawSetupFlags   = C.IORING_SETUP_CLAMP
	rawSetupSubmitAll    rawSetupFlags   = C.IORING_SETUP_SUBMIT_ALL
	rawSetupCoopTaskrun  rawSetupFlags   = C.IORING_SETUP_COOP_TASKRUN
	rawSetupTaskrunFlag  rawSetupFlags   = C.IORING_SETUP_TASKRUN_FLAG
	rawSetupNoSQArray    rawSetupFlags   = C.IORING_SETUP_NO_SQARRAY
	rawSetupHybridIOPoll rawSetupFlags   = C.IORING_SETUP_HYBRID_IOPOLL
	rawFeatSingleMMap    rawFeatureFlags = C.IORING_FEAT_SINGLE_MMAP
	rawFeatNoDrop        rawFeatureFlags = C.IORING_FEAT_NODROP
	rawFeatLinkedFile    rawFeatureFlags = C.IORING_FEAT_LINKED_FILE
	rawSQNeedWakeup      rawSQFlags      = C.IORING_SQ_NEED_WAKEUP
	rawSQCQOverflow      rawSQFlags      = C.IORING_SQ_CQ_OVERFLOW
	rawSQTaskrun         rawSQFlags      = C.IORING_SQ_TASKRUN
	rawEnterGetEvents    rawEnterFlags   = C.IORING_ENTER_GETEVENTS
	rawEnterSQWakeup     rawEnterFlags   = C.IORING_ENTER_SQ_WAKEUP

	rawOpNop           rawOpcode = C.IORING_OP_NOP
	rawOpReadv         rawOpcode = C.IORING_OP_READV
	rawOpWritev        rawOpcode = C.IORING_OP_WRITEV
	rawOpFsync         rawOpcode = C.IORING_OP_FSYNC
	rawOpReadFixed     rawOpcode = C.IORING_OP_READ_FIXED
	rawOpWriteFixed    rawOpcode = C.IORING_OP_WRITE_FIXED
	rawOpPollAdd       rawOpcode = C.IORING_OP_POLL_ADD
	rawOpPollRemove    rawOpcode = C.IORING_OP_POLL_REMOVE
	rawOpTimeout       rawOpcode = C.IORING_OP_TIMEOUT
	rawOpTimeoutRemove rawOpcode = C.IORING_OP_TIMEOUT_REMOVE
	rawOpAsyncCancel   rawOpcode = C.IORING_OP_ASYNC_CANCEL
	rawOpLinkTimeout   rawOpcode = C.IORING_OP_LINK_TIMEOUT
	rawOpFallocate     rawOpcode = C.IORING_OP_FALLOCATE
	rawOpOpenat        rawOpcode = C.IORING_OP_OPENAT
	rawOpClose         rawOpcode = C.IORING_OP_CLOSE
	rawOpStatx         rawOpcode = C.IORING_OP_STATX
	rawOpRead          rawOpcode = C.IORING_OP_READ
	rawOpWrite         rawOpcode = C.IORING_OP_WRITE
	rawOpOpenat2       rawOpcode = C.IORING_OP_OPENAT2
	rawOpFtruncate     rawOpcode = C.IORING_OP_FTRUNCATE
	rawOpReadvFixed    rawOpcode = C.IORING_OP_READV_FIXED
	rawOpWritevFixed   rawOpcode = C.IORING_OP_WRITEV_FIXED
	rawOpLast          rawOpcode = C.IORING_OP_LAST

	rawFsyncDatasync uint32 = C.IORING_FSYNC_DATASYNC
	rawPollMultishot uint32 = C.IORING_POLL_ADD_MULTI

	rawAsyncCancelAll     rawCancelFlags = C.IORING_ASYNC_CANCEL_ALL
	rawAsyncCancelFD      rawCancelFlags = C.IORING_ASYNC_CANCEL_FD
	rawAsyncCancelAny     rawCancelFlags = C.IORING_ASYNC_CANCEL_ANY
	rawAsyncCancelFDFixed rawCancelFlags = C.IORING_ASYNC_CANCEL_FD_FIXED

	rawRegisterBuffers      rawRegisterOpcode = C.IORING_REGISTER_BUFFERS
	rawRegisterFiles        rawRegisterOpcode = C.IORING_REGISTER_FILES
	rawRegisterFilesUpdate  rawRegisterOpcode = C.IORING_REGISTER_FILES_UPDATE
	rawRegisterEventFD      rawRegisterOpcode = C.IORING_REGISTER_EVENTFD
	rawUnregisterEventFD    rawRegisterOpcode = C.IORING_UNREGISTER_EVENTFD
	rawRegisterEventFDAsync rawRegisterOpcode = C.IORING_REGISTER_EVENTFD_ASYNC
	rawRegisterProbe        rawRegisterOpcode = C.IORING_REGISTER_PROBE
	rawRegisterFiles2       rawRegisterOpcode = C.IORING_REGISTER_FILES2
	rawRegisterSyncCancel   rawRegisterOpcode = C.IORING_REGISTER_SYNC_CANCEL
	rawRsrcRegisterSparse   uint32            = C.IORING_RSRC_REGISTER_SPARSE
	rawOpSupported          uint16            = C.IO_URING_OP_SUPPORTED

	rawSQRingOffset uint64 = C.IORING_OFF_SQ_RING
	rawCQRingOffset uint64 = C.IORING_OFF_CQ_RING
	rawSQEsOffset   uint64 = C.IORING_OFF_SQES
)
