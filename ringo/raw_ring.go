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

// ringHooks stubs the transport's syscall boundary. Production leaves it nil.
// Tests set it to drive conditions a live kernel cannot be asked for, such as
// EAGAIN from a submit or a partial registration result.
//
// Only syscalls are stubbable. The queue arithmetic below is never replaced:
// tests run it over heap memory instead of a mapping, so masking, wraparound,
// and index math are the same code in both.
type ringHooks struct {
	enter    func(submitted, waitFor uint32, flags rawEnterFlags) (uint, syscall.Errno)
	register func(
		opcode rawRegisterOpcode, argument unsafe.Pointer, count uint32,
	) (uint, syscall.Errno)
	closeDescriptor func() error
	releaseMappings func()
}

// rawRing is the private Linux transport beneath Ring.
type rawRing struct {
	sq       rawSubmissionQueue
	cq       rawCompletionQueue
	flags    rawSetupFlags
	features rawFeatureFlags
	fd       int
	hooks    *ringHooks
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
		_ = ring.closeDescriptor()
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

	// Every kernel new enough for IORING_SETUP_SUBMIT_ALL (5.18), which New
	// requires, also provides IORING_FEAT_SINGLE_MMAP (5.4). The submission and
	// completion rings therefore always share one mapping sized to the larger
	// of the two.
	ringSize := max(sqRingSize, cqRingSize)
	sqMemory, err := mapRingMemory(ring.fd, rawSQRingOffset, ringSize)
	if err != nil {
		return err
	}
	ring.sq.ringMemory = sqMemory
	ring.cq.ringMemory = sqMemory

	sqeSize := unsafe.Sizeof(rawSQE{})
	sqeMemory, err := mapRingMemory(
		ring.fd,
		rawSQEsOffset,
		uintptr(params.Sq_entries)*sqeSize,
	)
	if err != nil {
		ring.releaseMappings()
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

// closeDescriptor closes the io_uring_setup descriptor, which asks the kernel
// to release the context. io_uring_setup(2) documents that this frees the
// context's resources but may do so asynchronously, so it is not a barrier for
// requests still in flight.
func (ring *rawRing) closeDescriptor() error {
	if ring.hooks != nil && ring.hooks.closeDescriptor != nil {
		return ring.hooks.closeDescriptor()
	}
	if ring.fd == -1 {
		return nil
	}
	err := syscall.Close(ring.fd)
	ring.fd = -1
	return err
}

// queueExit tears the ring down completely. It is only for a ring that owns no
// operations, which is why New's failure paths may use it: nothing was ever
// submitted, so nothing can still reference the mappings.
func (ring *rawRing) queueExit() error {
	err := ring.closeDescriptor()
	ring.releaseMappings()
	return err
}

// releaseMappings unmaps the ring memory. Only a caller that knows no request
// can still reference it may do this: the kernel owns the underlying pages and
// keeps its own reference, but nothing documents that unmapping is safe while
// requests are outstanding.
func (ring *rawRing) releaseMappings() {
	if ring.hooks != nil && ring.hooks.releaseMappings != nil {
		ring.hooks.releaseMappings()
		return
	}
	// The submission and completion rings share one mapping (SINGLE_MMAP), so
	// ring.cq.ringMemory aliases ring.sq.ringMemory and is not unmapped twice.
	if ring.sq.sqeMemory != nil {
		_ = unix.Munmap(ring.sq.sqeMemory)
	}
	if ring.sq.ringMemory != nil {
		_ = unix.Munmap(ring.sq.ringMemory)
	}
	ring.sq = rawSubmissionQueue{}
	ring.cq = rawCompletionQueue{}
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
	sqe := ring.sqeAt(ring.sq.sqeTail)
	ring.sq.sqeTail = next
	return sqe
}

// sqeAt addresses the submission queue entry holding position sequence. Ringo
// keeps the kernel's index array as the identity permutation when it is present
// at all, so an entry's position is also its index.
func (ring *rawRing) sqeAt(sequence uint32) *rawSQE {
	index := sequence & *ring.sq.ringMask
	return (*rawSQE)(unsafe.Add(
		unsafe.Pointer(unsafe.SliceData(ring.sq.sqeMemory)),
		uintptr(index)*ring.sq.sqeSize,
	))
}

// flushSQ publishes the queued entries and reports how many the kernel has yet
// to consume, along with the position it will start consuming from.
func (ring *rawRing) flushSQ() (submitted, head uint32) {
	tail := ring.sq.sqeTail
	if ring.sq.sqeHead != tail {
		ring.sq.sqeHead = tail
		atomic.StoreUint32(ring.sq.tail, tail)
	}
	head = atomic.LoadUint32(ring.sq.head)
	return tail - head, head
}

// splitChain reports whether a short submission cut a linked chain: the kernel
// consumed positions [head, head+consumed), so a chain survived only if the
// last entry taken did not expect a successor. ErrChainSplit describes when
// this happens and what it costs.
//
// Reading that entry back is safe because the kernel is finished with
// submission queue memory once io_uring_enter returns and Ringo is its only
// writer. Neither holds under SQPOLL, where a kernel thread reads the queue
// asynchronously and the returned count is just the requested one, so the check
// is skipped there and a cut goes undetected.
func (ring *rawRing) splitChain(head, consumed uint32) bool {
	if consumed == 0 || ring.flags&rawSetupSQPoll != 0 {
		return false
	}
	last := ring.sqeAt(head + consumed - 1)
	return rawSQEFlags(last.Flags)&(rawSqeIOLink|rawSqeIOHardlink) != 0
}

func (ring *rawRing) submitAndWait(waitFor uint32) (uint, error) {
	submitted, head := ring.flushSQ()
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
	for {
		consumed, errno := ring.enter(submitted, waitFor, flags)
		// io_uring returns EINTR only when it consumed no SQE, so the same
		// submission count stays correct on retry. The interrupted call lost no
		// completion either, which leaves the caller nothing to act on:
		// interruption of an operation is reported in that operation's own CQE.
		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			return 0, errno
		}
		if uint32(consumed) < submitted && ring.splitChain(head, uint32(consumed)) {
			return 0, ErrChainSplit
		}
		return consumed, nil
	}
}

func (ring *rawRing) enter(
	submitted uint32,
	waitFor uint32,
	flags rawEnterFlags,
) (uint, syscall.Errno) {
	if ring.hooks != nil && ring.hooks.enter != nil {
		return ring.hooks.enter(submitted, waitFor, flags)
	}
	consumed, _, errno := syscall.Syscall6(
		unix.SYS_IO_URING_ENTER,
		uintptr(ring.fd),
		uintptr(submitted),
		uintptr(waitFor),
		uintptr(flags),
		0,
		0,
	)
	return uint(consumed), errno
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
	if ring.hooks != nil && ring.hooks.register != nil {
		return ring.hooks.register(opcode, argument, count)
	}
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
