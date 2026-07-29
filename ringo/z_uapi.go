//go:build linux

// Code generated from the bundled Linux io_uring UAPI by cgo -godefs; DO NOT EDIT.

package ringo

type rawOpcode uint8
type rawSQEFlags uint8
type rawCQEFlags uint32
type rawSetupFlags uint32
type rawCancelFlags uint32
type rawSQFlags uint32
type rawEnterFlags uint32
type rawFeatureFlags uint32
type rawRegisterOpcode uint32

type rawSQE struct {
	Opcode       uint8
	Flags        uint8
	Ioprio       uint16
	Fd           int32
	Off          uint64
	Addr         uint64
	Len          uint32
	Rw_flags     int32
	User_data    uint64
	Buf_index    uint16
	Personality  uint16
	Splice_fd_in int32
	Anon0        [16]byte
}
type rawCQE struct {
	Data  uint64
	Res   int32
	Flags uint32
}
type rawParams struct {
	Sq_entries     uint32
	Cq_entries     uint32
	Flags          uint32
	Sq_thread_cpu  uint32
	Sq_thread_idle uint32
	Features       uint32
	Wq_fd          uint32
	Resv           [3]uint32
	Sq_off         rawSQOffsets
	Cq_off         rawCQOffsets
}
type rawSQOffsets struct {
	Head         uint32
	Tail         uint32
	Ring_mask    uint32
	Ring_entries uint32
	Flags        uint32
	Dropped      uint32
	Array        uint32
	Resv1        uint32
	User_addr    uint64
}
type rawCQOffsets struct {
	Head         uint32
	Tail         uint32
	Ring_mask    uint32
	Ring_entries uint32
	Overflow     uint32
	Cqes         uint32
	Flags        uint32
	Resv1        uint32
	User_addr    uint64
}
type rawFilesUpdate struct {
	Offset uint32
	Resv   uint32
	Fds    uint64
}
type rawRsrcRegister struct {
	Nr    uint32
	Flags uint32
	Resv2 uint64
	Data  uint64
	Tags  uint64
}
type rawTimespec struct {
	Sec  int64
	Nsec int64
}
type rawSyncCancelReg struct {
	Addr    uint64
	Fd      int32
	Flags   uint32
	Timeout rawTimespec
	Opcode  uint8
	Pad     [7]uint8
	Pad2    [3]uint64
}
type rawProbeHeader struct {
	Last_op uint8
	Ops_len uint8
	Resv    uint16
	Resv2   [3]uint32
}
type rawProbeOp struct {
	Op    uint8
	Resv  uint8
	Flags uint16
	Resv2 uint32
}

const (
	rawSqeFixedFile  rawSQEFlags = 0x1
	rawSqeIODrain    rawSQEFlags = 0x2
	rawSqeIOLink     rawSQEFlags = 0x4
	rawSqeIOHardlink rawSQEFlags = 0x8

	rawCQEMore rawCQEFlags = 0x2

	rawSetupIOPoll       rawSetupFlags   = 0x1
	rawSetupSQPoll       rawSetupFlags   = 0x2
	rawSetupSQAff        rawSetupFlags   = 0x4
	rawSetupCQSize       rawSetupFlags   = 0x8
	rawSetupClamp        rawSetupFlags   = 0x10
	rawSetupSubmitAll    rawSetupFlags   = 0x80
	rawSetupCoopTaskrun  rawSetupFlags   = 0x100
	rawSetupTaskrunFlag  rawSetupFlags   = 0x200
	rawSetupNoSQArray    rawSetupFlags   = 0x10000
	rawSetupHybridIOPoll rawSetupFlags   = 0x20000
	rawFeatSingleMMap    rawFeatureFlags = 0x1
	rawFeatNoDrop        rawFeatureFlags = 0x2
	rawFeatLinkedFile    rawFeatureFlags = 0x1000
	rawSQNeedWakeup      rawSQFlags      = 0x1
	rawSQCQOverflow      rawSQFlags      = 0x2
	rawSQTaskrun         rawSQFlags      = 0x4
	rawEnterGetEvents    rawEnterFlags   = 0x1
	rawEnterSQWakeup     rawEnterFlags   = 0x2

	rawOpNop           rawOpcode = 0x0
	rawOpReadv         rawOpcode = 0x1
	rawOpWritev        rawOpcode = 0x2
	rawOpFsync         rawOpcode = 0x3
	rawOpReadFixed     rawOpcode = 0x4
	rawOpWriteFixed    rawOpcode = 0x5
	rawOpPollAdd       rawOpcode = 0x6
	rawOpPollRemove    rawOpcode = 0x7
	rawOpTimeout       rawOpcode = 0xb
	rawOpTimeoutRemove rawOpcode = 0xc
	rawOpAsyncCancel   rawOpcode = 0xe
	rawOpLinkTimeout   rawOpcode = 0xf
	rawOpFallocate     rawOpcode = 0x11
	rawOpOpenat        rawOpcode = 0x12
	rawOpClose         rawOpcode = 0x13
	rawOpStatx         rawOpcode = 0x15
	rawOpRead          rawOpcode = 0x16
	rawOpWrite         rawOpcode = 0x17
	rawOpOpenat2       rawOpcode = 0x1c
	rawOpFtruncate     rawOpcode = 0x37
	rawOpReadvFixed    rawOpcode = 0x3c
	rawOpWritevFixed   rawOpcode = 0x3d
	rawOpLast          rawOpcode = 0x41

	rawFsyncDatasync uint32 = 0x1
	rawPollMultishot uint32 = 0x1

	rawAsyncCancelAll     rawCancelFlags = 0x1
	rawAsyncCancelFD      rawCancelFlags = 0x2
	rawAsyncCancelAny     rawCancelFlags = 0x4
	rawAsyncCancelFDFixed rawCancelFlags = 0x8

	rawRegisterBuffers      rawRegisterOpcode = 0x0
	rawRegisterFiles        rawRegisterOpcode = 0x2
	rawRegisterFilesUpdate  rawRegisterOpcode = 0x6
	rawRegisterEventFD      rawRegisterOpcode = 0x4
	rawUnregisterEventFD    rawRegisterOpcode = 0x5
	rawRegisterEventFDAsync rawRegisterOpcode = 0x7
	rawRegisterProbe        rawRegisterOpcode = 0x8
	rawRegisterFiles2       rawRegisterOpcode = 0xd
	rawRegisterSyncCancel   rawRegisterOpcode = 0x18
	rawRsrcRegisterSparse   uint32            = 0x1
	rawOpSupported          uint16            = 0x1

	rawSQRingOffset uint64 = 0x0
	rawCQRingOffset uint64 = 0x8000000
	rawSQEsOffset   uint64 = 0x10000000
)
