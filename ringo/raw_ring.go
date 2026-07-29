//go:build linux

package ringo

import (
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type rawSubmissionQueue struct {
	head        *uint32
	tail        *uint32
	ringMask    *uint32
	ringEntries *uint32
	flags       *uint32
	array       *uint32

	ringMemory []byte
	sqeMemory  []byte
	sqeSize    uintptr
	sqeHead    uint32
	sqeTail    uint32
}

type rawCompletionQueue struct {
	head     *uint32
	tail     *uint32
	ringMask *uint32

	ringMemory []byte
	cqeBase    unsafe.Pointer
	cqeSize    uintptr
}

// rawRing is the private Linux transport beneath Ring.
type rawRing struct {
	sq       rawSubmissionQueue
	cq       rawCompletionQueue
	flags    rawSetupFlags
	features rawFeatureFlags
	fd       int
}

func newRaw() *rawRing {
	return &rawRing{fd: -1}
}

func (ring *rawRing) hasFeature(feature rawFeatureFlags) bool {
	return ring.features&feature != 0
}

func (ring *rawRing) queueInit(entries uint32, params rawParams) error {
	// SUBMIT_ALL is part of Ringo's submission contract. NO_SQARRAY is only
	// an implementation optimization, so retry without that flag when an
	// older kernel rejects it.
	remove := [...]rawSetupFlags{
		0,
		rawSetupNoSQArray,
	}
	var (
		actual rawParams
		fd     int
		errno  syscall.Errno
	)
	for _, omitted := range remove {
		actual = params
		actual.Flags &^= uint32(omitted)
		fd, errno = setupRing(entries, &actual)
		if errno == 0 || errno != syscall.EINVAL {
			break
		}
	}
	if errno != 0 {
		return errno
	}
	ring.fd = fd
	if err := ring.mapMemory(&actual); err != nil {
		_ = syscall.Close(ring.fd)
		ring.fd = -1
		return err
	}

	if rawSetupFlags(actual.Flags)&rawSetupNoSQArray == 0 {
		for index := uint32(0); index < *ring.sq.ringEntries; index++ {
			*(*uint32)(unsafe.Add(
				unsafe.Pointer(ring.sq.array),
				uintptr(index)*unsafe.Sizeof(uint32(0)),
			)) = index
		}
	}
	ring.flags = rawSetupFlags(actual.Flags)
	ring.features = rawFeatureFlags(actual.Features)
	return nil
}

func setupRing(entries uint32, params *rawParams) (int, syscall.Errno) {
	fd, _, errno := syscall.Syscall(
		unix.SYS_IO_URING_SETUP,
		uintptr(entries),
		uintptr(unsafe.Pointer(params)),
		0,
	)
	return int(fd), errno
}

func (ring *rawRing) mapMemory(params *rawParams) error {
	sqRingSize := uintptr(params.Sq_off.Array) +
		uintptr(params.Sq_entries)*unsafe.Sizeof(uint32(0))
	cqeSize := unsafe.Sizeof(rawCQE{})
	cqRingSize := uintptr(params.Cq_off.Cqes) +
		uintptr(params.Cq_entries)*cqeSize

	shared := rawFeatureFlags(params.Features)&rawFeatSingleMMap != 0
	if shared && cqRingSize > sqRingSize {
		sqRingSize = cqRingSize
	}
	sqMemory, err := mapRingMemory(ring.fd, rawSQRingOffset, sqRingSize)
	if err != nil {
		return err
	}
	ring.sq.ringMemory = sqMemory

	if shared {
		ring.cq.ringMemory = sqMemory
	} else {
		cqMemory, mapErr := mapRingMemory(ring.fd, rawCQRingOffset, cqRingSize)
		if mapErr != nil {
			ring.unmap()
			return mapErr
		}
		ring.cq.ringMemory = cqMemory
	}

	sqeSize := unsafe.Sizeof(rawSQE{})
	sqeMemory, err := mapRingMemory(
		ring.fd,
		rawSQEsOffset,
		uintptr(params.Sq_entries)*sqeSize,
	)
	if err != nil {
		ring.unmap()
		return err
	}
	ring.sq.sqeMemory = sqeMemory
	ring.sq.sqeSize = sqeSize
	ring.cq.cqeSize = cqeSize
	ring.bindPointers(params)
	return nil
}

func mapRingMemory(fd int, offset uint64, size uintptr) ([]byte, error) {
	maxInt := uintptr(^uint(0) >> 1)
	if size == 0 || size > maxInt {
		return nil, errors.New("ringo: invalid io_uring mapping size")
	}
	return unix.Mmap(
		fd,
		int64(offset),
		int(size),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED|syscall.MAP_POPULATE,
	)
}

func mappingPointer(memory []byte, offset uint32) unsafe.Pointer {
	return unsafe.Add(unsafe.Pointer(unsafe.SliceData(memory)), uintptr(offset))
}

func (ring *rawRing) bindPointers(params *rawParams) {
	ring.sq.head = (*uint32)(mappingPointer(ring.sq.ringMemory, params.Sq_off.Head))
	ring.sq.tail = (*uint32)(mappingPointer(ring.sq.ringMemory, params.Sq_off.Tail))
	ring.sq.ringMask = (*uint32)(mappingPointer(ring.sq.ringMemory, params.Sq_off.Ring_mask))
	ring.sq.ringEntries = (*uint32)(mappingPointer(
		ring.sq.ringMemory,
		params.Sq_off.Ring_entries,
	))
	ring.sq.flags = (*uint32)(mappingPointer(ring.sq.ringMemory, params.Sq_off.Flags))
	if rawSetupFlags(params.Flags)&rawSetupNoSQArray == 0 {
		ring.sq.array = (*uint32)(mappingPointer(
			ring.sq.ringMemory,
			params.Sq_off.Array,
		))
	}

	ring.cq.head = (*uint32)(mappingPointer(ring.cq.ringMemory, params.Cq_off.Head))
	ring.cq.tail = (*uint32)(mappingPointer(ring.cq.ringMemory, params.Cq_off.Tail))
	ring.cq.ringMask = (*uint32)(mappingPointer(
		ring.cq.ringMemory,
		params.Cq_off.Ring_mask,
	))
	ring.cq.cqeBase = mappingPointer(ring.cq.ringMemory, params.Cq_off.Cqes)
}

func (ring *rawRing) unmap() {
	sqRing := ring.sq.ringMemory
	cqRing := ring.cq.ringMemory
	if ring.sq.sqeMemory != nil {
		_ = unix.Munmap(ring.sq.sqeMemory)
	}
	if sqRing != nil {
		_ = unix.Munmap(sqRing)
	}
	if cqRing != nil && unsafe.SliceData(cqRing) != unsafe.SliceData(sqRing) {
		_ = unix.Munmap(cqRing)
	}
	ring.sq = rawSubmissionQueue{}
	ring.cq = rawCompletionQueue{}
}

func (ring *rawRing) queueExit() error {
	var err error
	if ring.fd != -1 {
		closeErr := syscall.Close(ring.fd)
		if err == nil {
			err = closeErr
		}
		ring.fd = -1
	}
	ring.unmap()
	return err
}

func (ring *rawRing) sqCapacity() uint32 {
	return *ring.sq.ringEntries
}

func (ring *rawRing) sqSpaceLeft() uint32 {
	return *ring.sq.ringEntries -
		(ring.sq.sqeTail - atomic.LoadUint32(ring.sq.head))
}

func (ring *rawRing) getSQE() *rawSQE {
	head := atomic.LoadUint32(ring.sq.head)
	next := ring.sq.sqeTail + 1
	if next-head > *ring.sq.ringEntries {
		return nil
	}
	index := ring.sq.sqeTail & *ring.sq.ringMask
	ring.sq.sqeTail = next
	return (*rawSQE)(unsafe.Add(
		unsafe.Pointer(unsafe.SliceData(ring.sq.sqeMemory)),
		uintptr(index)*ring.sq.sqeSize,
	))
}

func (ring *rawRing) flushSQ() uint32 {
	tail := ring.sq.sqeTail
	if ring.sq.sqeHead != tail {
		ring.sq.sqeHead = tail
		atomic.StoreUint32(ring.sq.tail, tail)
	}
	return tail - atomic.LoadUint32(ring.sq.head)
}

func (ring *rawRing) submitAndWait(waitFor uint32) (uint, error) {
	submitted := ring.flushSQ()
	var flags rawEnterFlags
	sqFlags := rawSQFlags(atomic.LoadUint32(ring.sq.flags))
	if waitFor != 0 || sqFlags&(rawSQCQOverflow|rawSQTaskrun) != 0 {
		flags |= rawEnterGetEvents
	}
	if ring.flags&rawSetupSQPoll != 0 && sqFlags&rawSQNeedWakeup != 0 {
		flags |= rawEnterSQWakeup
	}
	// An awake SQPOLL thread observes the published tail directly. Enter the
	// kernel only when it must be woken, completion work must run, or the
	// caller asked to wait.
	if ring.flags&rawSetupSQPoll != 0 && waitFor == 0 && flags == 0 {
		return uint(submitted), nil
	}
	if submitted == 0 && waitFor == 0 && flags == 0 {
		return 0, nil
	}
	return ring.enter(submitted, waitFor, flags)
}

func (ring *rawRing) enter(
	submitted uint32,
	waitFor uint32,
	flags rawEnterFlags,
) (uint, error) {
	consumed, _, errno := syscall.Syscall6(
		unix.SYS_IO_URING_ENTER,
		uintptr(ring.fd),
		uintptr(submitted),
		uintptr(waitFor),
		uintptr(flags),
		0,
		0,
	)
	if errno != 0 {
		return 0, errno
	}
	return uint(consumed), nil
}

func (ring *rawRing) cqReady() uint32 {
	return atomic.LoadUint32(ring.cq.tail) - atomic.LoadUint32(ring.cq.head)
}

func (ring *rawRing) peekCQE() *rawCQE {
	head := atomic.LoadUint32(ring.cq.head)
	if head == atomic.LoadUint32(ring.cq.tail) {
		return nil
	}
	index := head & *ring.cq.ringMask
	return (*rawCQE)(unsafe.Add(ring.cq.cqeBase, uintptr(index)*ring.cq.cqeSize))
}

func (ring *rawRing) advanceCQ(count uint32) {
	atomic.StoreUint32(ring.cq.head, atomic.LoadUint32(ring.cq.head)+count)
}

func (ring *rawRing) register(
	opcode rawRegisterOpcode,
	argument unsafe.Pointer,
	count uint32,
) (uint, syscall.Errno) {
	result, _, errno := syscall.Syscall6(
		unix.SYS_IO_URING_REGISTER,
		uintptr(ring.fd),
		uintptr(opcode),
		uintptr(argument),
		uintptr(count),
		0,
		0,
	)
	return uint(result), errno
}

func (ring *rawRing) registerResult(
	opcode rawRegisterOpcode,
	argument unsafe.Pointer,
	count uint32,
) (uint, error) {
	result, errno := ring.register(opcode, argument, count)
	if errno != 0 {
		return 0, os.NewSyscallError("io_uring_register", errno)
	}
	return result, nil
}

func (ring *rawRing) registerBuffers(iovecs []syscall.Iovec) (uint, error) {
	result, err := ring.registerResult(
		rawRegisterBuffers,
		unsafe.Pointer(unsafe.SliceData(iovecs)),
		uint32(len(iovecs)),
	)
	return result, err
}

func (ring *rawRing) registerFilesSparse(count uint32) (uint, error) {
	registration := rawRsrcRegister{Nr: count, Flags: rawRsrcRegisterSparse}
	return ring.registerResult(
		rawRegisterFiles2,
		unsafe.Pointer(&registration),
		uint32(unsafe.Sizeof(registration)),
	)
}

func (ring *rawRing) registerSyncCancel(registration *rawSyncCancelReg) (uint, error) {
	result, err := ring.registerResult(
		rawRegisterSyncCancel,
		unsafe.Pointer(registration),
		1,
	)
	return result, err
}
