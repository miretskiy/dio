//go:build linux

package ringo

import (
	"errors"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/miretskiy/dio/internal/intrusive"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// alignedBytes returns size bytes with 8-byte alignment, which the uint32 and
// rawCQE pointers bound into ring memory require.
func alignedBytes(size uintptr) []byte {
	words := make([]uint64, (size+7)/8)
	return unsafe.Slice(
		(*byte)(unsafe.Pointer(unsafe.SliceData(words))), len(words)*8,
	)
}

// newHeapRing builds a real rawRing over heap memory instead of a mapping.
// bindPointers only needs bytes and a layout, so every queue operation --
// getSQE, flushSQ, sqSpaceLeft, cqReady, peekCQE, advanceCQ -- runs its
// production arithmetic here, including masking and wraparound. Only the
// syscall boundary is stubbed, through rawRing.hooks.
//
// The offsets below need not match the kernel's, only be self-consistent, since
// the code under test reads them from rawParams exactly as io_uring_setup
// reports them. entries and cqEntries must be powers of two.
func newHeapRing(entries, cqEntries uint32) *rawRing {
	params := rawParams{
		Sq_entries: entries,
		Cq_entries: cqEntries,
		Flags:      uint32(rawSetupNoSQArray),
		Sq_off: rawSQOffsets{
			Head: 0, Tail: 4, Ring_mask: 8, Ring_entries: 12, Flags: 16,
		},
		Cq_off: rawCQOffsets{
			Head: 20, Tail: 24, Ring_mask: 28, Ring_entries: 32, Cqes: 64,
		},
	}
	cqeSize := unsafe.Sizeof(rawCQE{})
	ring := &rawRing{fd: -1, flags: rawSetupFlags(params.Flags)}
	ring.sq.ringMemory = alignedBytes(
		uintptr(params.Cq_off.Cqes) + uintptr(cqEntries)*cqeSize,
	)
	ring.cq.ringMemory = ring.sq.ringMemory
	ring.sq.sqeSize = unsafe.Sizeof(rawSQE{})
	ring.sq.sqeMemory = alignedBytes(uintptr(entries) * ring.sq.sqeSize)
	ring.cq.cqeSize = cqeSize
	ring.bindPointers(&params)

	*ring.sq.ringMask = entries - 1
	*ring.sq.ringEntries = entries
	*ring.cq.ringMask = cqEntries - 1
	return ring
}

// testTransport stubs the syscall boundary of a heap-backed rawRing and lets a
// test post completions the way the kernel does.
type testTransport struct {
	raw *rawRing

	submitErrno  syscall.Errno
	submitErrnos []syscall.Errno
	submitCalls  int
	// consumeAtMost caps how many entries a submission takes, standing in for
	// the kernel giving up partway through a batch. Zero takes them all.
	consumeAtMost uint32

	registeredBuffers []syscall.Iovec
	registeredFiles   uint32
	updatedFileOffset uint32
	cancel            *rawSyncCancelReg
	registerOpcode    rawRegisterOpcode
	registerCount     uint32
	registerResult    uint
	registerErrno     syscall.Errno

	closeFn  func()
	closed   bool
	unmapped bool
}

func (transport *testTransport) enter(
	submitted, _ uint32, _ rawEnterFlags,
) (uint, syscall.Errno) {
	transport.submitCalls++
	errno := transport.submitErrno
	if len(transport.submitErrnos) != 0 {
		errno = transport.submitErrnos[0]
		transport.submitErrnos = transport.submitErrnos[1:]
	}
	if errno != 0 {
		return 0, errno
	}
	// Consume the published entries the way the kernel does, so submission
	// capacity is released and the queues genuinely wrap.
	consumed := submitted
	if transport.consumeAtMost != 0 && transport.consumeAtMost < consumed {
		consumed = transport.consumeAtMost
	}
	head := transport.raw.sq.head
	atomic.StoreUint32(head, atomic.LoadUint32(head)+consumed)
	return uint(consumed), 0
}

func (transport *testTransport) register(
	opcode rawRegisterOpcode, argument unsafe.Pointer, count uint32,
) (uint, syscall.Errno) {
	transport.registerOpcode = opcode
	transport.registerCount = count
	switch opcode {
	case rawRegisterFilesUpdate:
		transport.updatedFileOffset = (*rawFilesUpdate)(argument).Offset
	case rawRegisterBuffers:
		transport.registeredBuffers = append(
			[]syscall.Iovec(nil),
			unsafe.Slice((*syscall.Iovec)(argument), count)...,
		)
	case rawRegisterFiles2:
		transport.registeredFiles = (*rawRsrcRegister)(argument).Nr
	case rawRegisterSyncCancel:
		transport.cancel = new(rawSyncCancelReg)
		*transport.cancel = *(*rawSyncCancelReg)(argument)
	}
	return transport.registerResult, transport.registerErrno
}

// complete posts a completion for handle exactly as the kernel would: it writes
// the entry at the masked tail and publishes the new tail.
func (transport *testTransport) complete(
	handle Handle, result int32, flags rawCQEFlags,
) {
	transport.completeIdentity(uint64(handle.slot), result, flags)
}

// completeIdentity posts a completion carrying an arbitrary user_data, which is
// how a test reaches identities the kernel would never produce.
func (transport *testTransport) completeIdentity(
	userData uint64, result int32, flags rawCQEFlags,
) {
	cq := &transport.raw.cq
	tail := atomic.LoadUint32(cq.tail)
	entry := (*rawCQE)(unsafe.Add(
		cq.cqeBase, uintptr(tail&*cq.ringMask)*cq.cqeSize,
	))
	*entry = rawCQE{Data: userData, Res: result, Flags: uint32(flags)}
	atomic.StoreUint32(cq.tail, tail+1)
}

// sqe returns the submission queue entry the Ring wrote at index.
func (transport *testTransport) sqe(index int) rawSQE {
	sq := &transport.raw.sq
	return *(*rawSQE)(unsafe.Add(
		unsafe.Pointer(unsafe.SliceData(sq.sqeMemory)),
		uintptr(uint32(index)&*sq.ringMask)*sq.sqeSize,
	))
}

// queued reports how many entries the Ring has written into the submission
// queue over its lifetime.
func (transport *testTransport) queued() uint32 {
	return transport.raw.sq.sqeTail
}

// reaped reports how many completions the Ring has consumed.
func (transport *testTransport) reaped() uint32 {
	return atomic.LoadUint32(transport.raw.cq.head)
}

func newFakeRing(depth int) (*Ring, *testTransport) {
	raw := newHeapRing(uint32(depth), uint32(depth)*2)
	transport := &testTransport{raw: raw}
	raw.hooks = &ringHooks{
		enter:    transport.enter,
		register: transport.register,
		closeDescriptor: func() error {
			if transport.closeFn != nil {
				transport.closeFn()
			}
			transport.closed = true
			return nil
		},
		releaseMappings: func() { transport.unmapped = true },
	}
	ring := &Ring{
		backend: raw,
		id:      nextRingID.Add(1),
		pending: intrusive.MakeFixedList[pendingSlot](depth),
	}
	return ring, transport
}

// TestShortSubmissionReportsOnlyASplitChain covers the one short submission
// IORING_SETUP_SUBMIT_ALL does not prevent: the kernel stops partway through a
// batch because it cannot allocate a request. Stopping inside a chain is
// reported, since the remainder will run as a chain of its own; stopping on a
// chain boundary broke nothing and is left to the next submission.
func TestShortSubmissionReportsOnlyASplitChain(t *testing.T) {
	for _, test := range []struct {
		name      string
		consume   uint32
		wantSplit bool
	}{
		{name: "inside the chain", consume: 2, wantSplit: true},
		{name: "on the chain boundary", consume: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			ring, transport := newFakeRing(8)
			// A three-operation chain, then unrelated work behind it.
			_, err := ring.PushLinked(Nop(), Then(LinkSoft, Nop()), Then(LinkHard, Nop()))
			require.NoError(t, err)
			_, err = ring.Push(Nop())
			require.NoError(t, err)

			transport.consumeAtMost = test.consume
			submitted, err := ring.Submit()
			if test.wantSplit {
				require.ErrorIs(t, err, ErrChainSplit)
				require.Zero(t, submitted, "a split must not also report progress")
				return
			}
			require.NoError(t, err)
			require.Equal(t, int(test.consume), submitted)
		})
	}
}

func TestSetupOptionsMapToKernelParameters(t *testing.T) {
	config := defaultConfig()
	options := []Option{
		WithDepth(32),
		WithSQPollCPU(7),
		WithSQPollIdle(250),
		WithHybridIOPoll(),
		WithCQSize(64),
		WithTaskRunFlag(),
		WithFixedFiles(8),
	}
	for _, option := range options {
		require.NoError(t, option.apply(&config))
	}
	wantFlags := rawSetupSQPoll |
		rawSetupSQAff |
		rawSetupIOPoll |
		rawSetupHybridIOPoll |
		rawSetupCQSize |
		rawSetupClamp |
		rawSetupSubmitAll |
		rawSetupCoopTaskrun |
		rawSetupTaskrunFlag |
		rawSetupNoSQArray
	require.Equal(t, uint32(32), config.depth)
	require.Equal(t, wantFlags, config.flags)
	require.Equal(t, uint32(64), config.cqEntries)
	require.Equal(t, uint32(7), config.sqThreadCPU)
	require.Equal(t, uint32(250), config.sqThreadIdle)
	require.Equal(t, uint32(8), config.fixedFiles)
}

// rejects asserts that ring refuses op for the stated reason and returns the
// rejection, so a caller can assert on the specific error. A refused push must
// also hand back no handle.
func rejects(t *testing.T, ring *Ring, op Op, why string) error {
	t.Helper()
	handle, err := ring.Push(op)
	require.Error(t, err, why)
	require.Zero(t, handle, why)
	return err
}

func fakeFile(fd uintptr) *os.File {
	return os.NewFile(fd, "ringo-test")
}

// TestRawRingQueueArithmetic drives the transport's index math directly, past a
// full wrap of both queues. A live kernel only reaches this after tens of
// thousands of operations, so the integration tests never do.
func TestRawRingQueueArithmetic(t *testing.T) {
	const entries = 4
	raw := newHeapRing(entries, entries*2)
	transport := &testTransport{raw: raw}
	raw.hooks = &ringHooks{enter: transport.enter}

	require.Equal(t, uint32(entries), raw.sqCapacity())
	require.Equal(t, uint32(entries), raw.sqSpaceLeft())

	// Three laps, so both the submission and completion indices wrap.
	for lap := range 3 * entries {
		require.Equal(t, uint32(entries), raw.sqSpaceLeft(), "lap %d", lap)

		sqes := make([]*rawSQE, entries)
		for i := range sqes {
			sqes[i] = raw.getSQE()
			require.NotNil(t, sqes[i], "lap %d entry %d", lap, i)
			sqes[i].User_data = uint64(lap*entries + i)
		}
		require.Zero(t, raw.sqSpaceLeft(), "lap %d: full queue reports space", lap)
		require.Nil(t, raw.getSQE(), "lap %d: full queue handed out an entry", lap)

		// Distinct entries within a lap, and the same storage reused across laps.
		require.NotEqual(t, sqes[0], sqes[1])
		require.Equal(t, uintptr(unsafe.Pointer(sqes[0])),
			uintptr(unsafe.Pointer(&raw.sq.sqeMemory[0])),
			"lap %d: first entry is not at the ring base", lap)

		submitted, err := raw.submitAndWait(0)
		require.NoError(t, err)
		require.Equal(t, uint(entries), submitted, "lap %d", lap)

		for i := range entries {
			transport.completeIdentity(uint64(lap*entries+i), int32(i), 0)
		}
		require.Equal(t, uint32(entries), raw.cqReady(), "lap %d", lap)
		for i := range entries {
			cqe := raw.peekCQE()
			require.NotNil(t, cqe, "lap %d entry %d", lap, i)
			require.Equal(t, uint64(lap*entries+i), cqe.Data,
				"lap %d: completion %d resolved to the wrong entry", lap, i)
			require.Equal(t, int32(i), cqe.Res)
			raw.advanceCQ(1)
		}
		require.Zero(t, raw.cqReady(), "lap %d", lap)
		require.Nil(t, raw.peekCQE(), "lap %d: drained queue produced an entry", lap)
	}
}

func TestPathOperationsUseTypedDirectoryDescriptors(t *testing.T) {
	ring, _ := newFakeRing(4)
	directory := fakeFile(12340)
	handle, err := ring.Push(OpenAt(FileFD(directory), "child", 0, 0))
	require.NoError(t, err)
	// Encoding is the conformance suite's job; what matters here is that the
	// Ring retains the directory file the operation named.
	pushed := ring.pending.Value(handle.slot).op.(*openAtOp)
	require.Same(t, directory, pushed.dir.file,
		"OpenAt did not retain its directory file")

	result := new(unix.Statx_t)
	rejects(t, ring,
		StatxAt(FileFD(directory), "", 0, unix.STATX_SIZE, result),
		"StatxAt accepted an empty path without AT_EMPTY_PATH")
	_, err = ring.Push(
		StatxAt(FileFD(directory), "", unix.AT_EMPTY_PATH, unix.STATX_SIZE, result),
	)
	require.NoError(t, err, "StatxAt rejected AT_EMPTY_PATH")

	ring.files = &FixedFiles{
		ring: ring, count: 1,
	}
	slot, err := ring.files.File(0)
	require.NoError(t, err)
	rejects(t, ring,
		OpenAtDirect(FixedFD(slot), "child", 0, 0, slot),
		"OpenAtDirect accepted one slot as both directory and result")
}

func TestPointerInputsAreCopiedAccordingToSemantics(t *testing.T) {
	ring, _ := newFakeRing(4)

	spec := syscall.Timespec{Sec: 7, Nsec: 11}
	timeout, err := ring.Push(Timeout(spec, 1, 0))
	require.NoError(t, err)
	spec.Sec = 99

	// The operation owns a __kernel_timespec copy rather than pointing at the
	// caller's syscall.Timespec, whose fields are 32-bit on 32-bit Linux.
	// escape_test covers that the SQE encodes this copy's address.
	ownedSpec := ring.pending.Value(timeout.slot).op.(*timeoutOp).spec
	require.Equal(t, rawTimespec{Sec: 7, Nsec: 11}, ownedSpec,
		"timeout did not own an input copy")
}

func TestPollMaskEncodingMatchesKernelUnionLayout(t *testing.T) {
	const mask uint32 = 0x12345678
	require.EqualValues(t, mask, uint32(encodePollMaskForEndian(mask, false)), "little-endian poll mask")
	require.Equal(t, uint32(0x56781234),
		uint32(encodePollMaskForEndian(mask, true)), "big-endian poll mask")
}

func TestSemanticFlagsRejectRawDiscriminatorBits(t *testing.T) {
	ring, _ := newFakeRing(4)
	rejects(t, ring,
		Timeout(syscall.Timespec{}, 1, TimeoutBoottime|TimeoutRealtime),
		"Timeout accepted two clock selections")

	target, err := ring.Push(Nop())
	require.NoError(t, err)
	rejects(t, ring,
		TimeoutUpdate(syscall.Timespec{}, target, TimeoutUpdateFlags(1<<1)),
		"TimeoutUpdate accepted the kernel's private update bit")
	rejects(t, ring,
		Fallocate(FileFD(fakeFile(12340)), -1, 1),
		"Fallocate accepted a negative offset")
	rejects(t, ring,
		FallocateMode(BorrowedFD(12340), FallocateFlags(1<<20), 0, 1),
		"FallocateMode accepted unknown mode bits")
	rejects(t, ring,
		Readv(BorrowedFD(12340), nil, 0, -1),
		"Readv accepted negative flags")
	rejects(t, ring,
		Ftruncate(BorrowedFD(12340), -1),
		"Ftruncate accepted a negative length")
}

func TestVectorOperationsUseInlineMetadataForCommonSizes(t *testing.T) {
	vectors := [][]byte{make([]byte, 1), nil, make([]byte, 2)}

	write := newWritevOp(nil, BorrowedFD(1), vectors, 0, 0, FixedBuffer{}, false)
	require.Same(t, &write.inlineIovecs[0], unsafe.SliceData(write.iovecs),
		"small writev allocated metadata outside its operation")
}

func growStack(depth int) int {
	var data [128]byte
	if depth == 0 {
		return int(data[0])
	}
	return int(data[depth%len(data)]) + growStack(depth-1)
}

func TestPushBuildsOwnedIovecs(t *testing.T) {
	ring, _ := newFakeRing(1)
	handle, first, second := pushLocalVectors(ring, fakeFile(12345))
	runtime.GC()

	pending := ring.pending.Value(handle.slot)
	write := pending.op.(*writevOp)
	// Empty vectors are skipped, and each iovec must still address the caller's
	// buffer after the [][]byte that described it has gone out of scope. The
	// typed Base pointers are the operation's only retention of those buffers.
	require.Len(t, write.iovecs, 2)
	require.Same(t, first, write.iovecs[0].Base,
		"iovec does not address the caller's buffer")
	require.Same(t, second, write.iovecs[1].Base,
		"iovec does not address the caller's buffer")
	require.EqualValues(t, 3, write.iovecs[0].Len)
	require.EqualValues(t, 5, write.iovecs[1].Len)
}

// pushLocalVectors builds the [][]byte in its own frame and returns only the
// buffer addresses, so the caller cannot accidentally keep the vector slice
// alive on the operation's behalf.
func pushLocalVectors(ring *Ring, file *os.File) (Handle, *byte, *byte) {
	first := make([]byte, 3)
	second := make([]byte, 5)
	first[0] = 1
	second[0] = 2
	handle, err := ring.Push(
		Writev(FileFD(file), [][]byte{first, nil, second}, 9, 0),
	)
	if err != nil {
		panic(err)
	}
	return handle, unsafe.SliceData(first), unsafe.SliceData(second)
}

func TestDrainMutatesInactiveOp(t *testing.T) {
	op := Nop()
	alias := op

	var sqe rawSQE
	op.Drain()
	alias.prepare(&sqe)
	require.Equal(t, rawSqeIODrain, rawSQEFlags(sqe.Flags),
		"Drain flags through alias")
}

// TestPushedOperationReuseIsNotDiagnosed pins the ownership contract: reuse is
// forbidden by the rule, not by a runtime check. Ringo keeps no per-Op state to
// detect it, so pushing one twice queues two independent operations instead of
// failing, and Drain never panics.
func TestPushedOperationReuseIsNotDiagnosed(t *testing.T) {
	ring, fake := newFakeRing(4)
	op := Nop()
	first, err := ring.Push(op)
	require.NoError(t, err)
	second, err := ring.Push(op)
	require.NoError(t, err)

	require.NotEqual(t, first, second, "reuse produced one identity")
	require.Equal(t, uint32(2), fake.queued())
	require.NotEqual(t, fake.sqe(0).User_data, fake.sqe(1).User_data)

	handles, err := ring.PushLinked(op, Then(LinkSoft, op))
	require.NoError(t, err, "repeated Op in a linked sequence")
	require.Len(t, handles, 2)
	require.NotEqual(t, handles[0], handles[1])

	require.NotPanics(t, func() { op.Drain() })
}

func TestFailedPushDoesNotConsumeOperation(t *testing.T) {
	ring, fake := newFakeRing(1)
	first, err := ring.Push(Nop())
	require.NoError(t, err)

	retry := Nop()
	require.ErrorIs(t, rejects(t, ring, retry, "full ring"), ErrFull)
	retry.Drain()

	_, err = ring.Submit()
	require.NoError(t, err)
	fake.complete(first, 0, 0)
	for range ring.Reap() {
	}
	_, err = ring.Push(retry)
	require.NoError(t, err, "push after failed attempt")
}

func TestPushLinkedRejectsInvalidSequenceWithoutMutatingOperations(t *testing.T) {
	ring, fake := newFakeRing(2)
	first, second := Nop(), Nop()

	_, err := ring.PushLinked(first, Then(LinkType(0xff), second))
	require.Error(t, err, "invalid link type was accepted")
	_, err = ring.PushLinked(first, Then(LinkSoft, nil))
	require.Error(t, err, "nil linked operation was accepted")
	for index, op := range []Op{first, second} {
		var sqe rawSQE
		op.prepare(&sqe)
		require.Falsef(t, sqe.Flags != 0, "operation %d was modified after rejection: flags=%#x", index, sqe.Flags)
	}
	require.False(t, ring.pending.Len() != 0 || fake.queued() != 0, "invalid sequence changed ring state")

	_, err = ring.PushLinked(first, Then(LinkHard, second))
	require.NoError(t, err, "operations were consumed by rejected sequence")
}

// TestReadWriteOperationsReleaseAfterCompletionYield checks the retention
// boundary: the Ring holds an operation's operands through its completion's
// iterator step, and drops them only once that step ends.
func TestReadWriteOperationsReleaseAfterCompletionYield(t *testing.T) {
	for _, tc := range []struct {
		name      string
		construct func(FD, []byte) Op
	}{
		{"read", func(fd FD, buf []byte) Op { return Read(fd, buf, 0) }},
		{"write", func(fd FD, buf []byte) Op { return Write(fd, buf, 0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ring, fake := newFakeRing(1)
			handle, err := ring.Push(
				tc.construct(BorrowedFD(12345), make([]byte, 32)),
			)
			require.NoError(t, err)

			fake.complete(handle, 32, 0)
			for range ring.Reap() {
				_, retained := ring.pending.TryValue(handle.slot)
				require.True(t, retained,
					"operands released before the completion was yielded")
			}
			_, retained := ring.pending.TryValue(handle.slot)
			require.False(t, retained,
				"operands retained after the final completion")
		})
	}
}

func TestPushLinkedIsAtomicAndEncodesOperationFlags(t *testing.T) {
	ring, fake := newFakeRing(4)
	if _, err := ring.PushLinked(
		Nop(),
		Then(LinkSoft, Nop()),
		Then(LinkSoft, Nop()),
		Then(LinkSoft, Nop()),
		Then(LinkSoft, Nop()),
	); !errors.Is(err, ErrFull) {
		require.ErrorIs(t, err, ErrFull, "oversized sequence error")
	}
	require.False(t, ring.pending.Len() != 0 || fake.queued() != 0, "oversized sequence changed ring state")

	handles, err := ring.PushLinked(
		Nop(),
		Then(LinkSoft, Nop()),
		Then(LinkSoft, Nop()),
		Then(LinkHard, Nop()),
	)
	require.False(t, err != nil, err)
	require.Falsef(t, len(handles) != 4, "handles: got %d want 4", len(handles))
	require.Falsef(t, rawSQEFlags(fake.sqe(0).Flags) != rawSqeIOLink, "soft-link flags: %#x", fake.sqe(0).Flags)
	require.Falsef(t, rawSQEFlags(fake.sqe(1).Flags) != rawSqeIOLink, "soft-link boundary flags: %#x", fake.sqe(1).Flags)
	require.Falsef(t, rawSQEFlags(fake.sqe(2).Flags) != rawSqeIOHardlink, "hard-link flags: %#x", fake.sqe(2).Flags)
	require.Falsef(t, fake.sqe(3).Flags != 0, "last flags: %#x", fake.sqe(3).Flags)
}

// TestSubmitErrorPreservesOwnership pins that a failed submit does not hand a
// pushed operation back. The entry stays published, so the kernel may still
// consume it, and the Ring keeps owning its operands either way.
func TestSubmitErrorPreservesOwnership(t *testing.T) {
	ring, fake := newFakeRing(1)
	handle, err := ring.Push(Nop())
	require.NoError(t, err)

	fake.submitErrno = syscall.EAGAIN
	_, err = ring.Submit()
	require.ErrorIs(t, err, syscall.EAGAIN)
	require.Equal(t, uint32(1), fake.queued(), "failed submit unpublished the entry")

	_, retained := ring.pending.TryValue(handle.slot)
	require.True(t, retained, "submit error released pushed ownership")
}

// TestSubmitReportsResourceErrors pins that the kernel's temporary resource
// conditions stay visible: the caller must reap before entering again, so Ringo
// cannot retry them on its own. EINTR is the opposite case and is absorbed one
// layer down, around the io_uring_enter call itself.
func TestSubmitReportsResourceErrors(t *testing.T) {
	ring, fake := newFakeRing(2)
	_, err := ring.Push(Nop())
	require.NoError(t, err)

	for _, resource := range []syscall.Errno{syscall.EAGAIN, syscall.EBUSY} {
		fake.submitErrnos = []syscall.Errno{resource}
		_, err = ring.SubmitAndWait(1)
		require.ErrorIs(t, err, resource, "resource error was absorbed")
	}
}

func TestReapCompletionAndGenerationSafety(t *testing.T) {
	ring, fake := newFakeRing(1)
	first, err := ring.Push(Nop())
	require.NoError(t, err)
	_, _ = ring.Submit()
	fake.complete(first, 7, 0)
	completions := collect(ring.Reap())
	require.Len(t, completions, 1)
	require.Equal(t, first, completions[0].Handle)
	require.Equal(t, 7, completions[0].Result)
	require.NoError(t, completions[0].Err)
	require.False(t, ring.pending.Len() != 0 || fake.reaped() != 1, "final completion did not advance and release")

	second, err := ring.Push(Nop())
	require.NoError(t, err)
	require.False(t, first.slot == second.slot, "slot generation did not advance")
	// An identity Reap cannot resolve means Ringo's own finality bookkeeping is
	// wrong, so it asserts. This is a test binary, so the assertion panics
	// before the production path drops the entry; that path is unobservable
	// here because buildutil test mode cannot be turned back off.
	fake.complete(first, 0, 0)
	require.Panics(t, func() { collect(ring.Reap()) },
		"unresolvable identity did not trip the assertion")

	// Whichever way the assertion goes, no live operation was released.
	_, live := ring.pending.TryValue(second.slot)
	require.True(t, live, "stale completion released a newer operation")
}

func TestReapRetainsMultishotUntilFinalCompletion(t *testing.T) {
	ring, fake := newFakeRing(1)
	handle, err := ring.Push(Nop())
	require.NoError(t, err)
	_, _ = ring.Submit()
	fake.complete(handle, 1, rawCQEMore)
	first := collect(ring.Reap())
	require.Falsef(t, len(first) != 1 || !first[0].Flags.More(), "multishot completion: %+v", first)
	_, retained := ring.pending.TryValue(handle.slot)
	require.True(t, retained, "IORING_CQE_F_MORE released the pending slot")

	fake.complete(handle, -int32(syscall.EIO), 0)
	final := collect(ring.Reap())
	require.Falsef(t, len(final) != 1 || !errors.Is(final[0].Err, syscall.EIO), "final completion: %+v", final)
	require.False(t, ring.pending.Len() != 0, "final multishot completion did not release the slot")
}

// TestReapPublishesConsumedEntriesOnce pins the two halves of Reap's bookkeeping
// apart: a pending slot is released as its own step ends, while the completion
// queue head -- the store that tells the kernel those entries are reusable --
// moves once, after the iterator is done.
func TestReapPublishesConsumedEntriesOnce(t *testing.T) {
	const count = 4
	ring, fake := newFakeRing(count)
	handles := make([]Handle, count)
	for i := range handles {
		var err error
		handles[i], err = ring.Push(Nop())
		require.NoError(t, err)
	}
	_, err := ring.Submit()
	require.NoError(t, err)
	for _, handle := range handles {
		fake.complete(handle, 0, 0)
	}

	seen := 0
	for range ring.Reap() {
		seen++
		require.Zerof(t, fake.reaped(), "head moved during step %d", seen)
		// This step's own operation is still owned; every earlier one is not.
		require.Equalf(t, count-seen+1, ring.pending.Len(), "pending at step %d", seen)
	}
	require.Equal(t, count, seen, "completions yielded")
	require.EqualValues(t, count, fake.reaped(), "head did not advance by the batch")
	require.Zero(t, ring.pending.Len(), "operations retained after the batch")
}

func TestReapBreakAndPanicAlwaysCleanupYieldedCompletion(t *testing.T) {
	t.Run("break", func(t *testing.T) {
		ring, fake := newFakeRing(2)
		first, _ := ring.Push(Nop())
		second, _ := ring.Push(Nop())
		_, _ = ring.Submit()
		fake.complete(first, 0, 0)
		fake.complete(second, 0, 0)

		for range ring.Reap() {
			break
		}
		require.Falsef(t, fake.reaped() != 1 || ring.pending.Len() != 1, "after break: cqHead=%d pending=%d", fake.reaped(), ring.pending.Len())
		require.EqualValues(t, 1, len(collect(ring.Reap())), "remaining completions")
	})

	t.Run("panic", func(t *testing.T) {
		ring, fake := newFakeRing(1)
		handle, _ := ring.Push(Nop())
		_, _ = ring.Submit()
		fake.complete(handle, 0, 0)
		func() {
			defer func() {
				require.False(t, recover() == nil, "reap body did not panic")
			}()
			for range ring.Reap() {
				panic("body")
			}
		}()
		require.EqualValues(t, 1, fake.reaped(), "after panic")
		require.Zero(t, ring.pending.Len(), "after panic")
	})
}

func TestRegisteredResourcesAreRingScopedAndRetained(t *testing.T) {
	ring, fake := newFakeRing(2)
	t.Cleanup(func() {
		require.NoError(t, ring.Close(), "close fake ring")
	})
	ring.files = &FixedFiles{ring: ring, count: 2}
	file, err := ring.files.File(1)
	require.NoError(t, err)
	data := make([]byte, 64)
	set, err := ring.RegisterBuffers(data)
	require.NoError(t, err)
	buffer, err := set.Bind(data[8:24])
	require.NoError(t, err)
	require.Len(t, fake.registeredBuffers, 1)
	require.Same(t, unsafe.SliceData(data), fake.registeredBuffers[0].Base,
		"registration did not use the retained backing buffer")

	handle, err := ring.Push(ReadFixed(FixedFD(file), buffer, 3))
	require.NoError(t, err)
	sqe := fake.sqe(0)
	require.NotZero(t, rawSQEFlags(sqe.Flags)&rawSqeFixedFile, "fixed/direct SQE")
	require.Zero(t, sqe.Buf_index, "fixed/direct SQE")
	require.Equal(t, uint64(slicePtr(data[8:24])), sqe.Addr, "fixed/direct SQE")
	_, retained := ring.pending.TryValue(handle.slot)
	require.True(t, retained, "fixed operation was not retained")

	other, _ := newFakeRing(1)
	require.ErrorIs(t,
		rejects(t, other, Read(FixedFD(file), make([]byte, 1), 0), "foreign fixed file"),
		ErrWrongRing)
	require.ErrorIs(t,
		rejects(t, other, ReadFixed(FileFD(fakeFile(12346)), buffer, 0), "foreign registered buffer"),
		ErrWrongRing)
	_, err = set.Bind(make([]byte, 1))
	require.Error(t, err, "Bind accepted an unregistered buffer")

	_, err = ring.Submit()
	require.NoError(t, err)
	fake.complete(handle, 16, 0)
	require.Len(t, collect(ring.Reap()), 1, "final completions")
	require.NoError(t, ring.Close(), "close idle ring")
	require.False(t, len(set.buffers) != 1, "Close mutated the caller's buffer table")
}

func TestFixedFilesUpdate(t *testing.T) {
	ring, fake := newFakeRing(2)
	files, err := ring.RegisterSparseFiles(3)
	require.NoError(t, err)
	require.Falsef(t, len(files.owners) != files.Len(), "owner slots: got %d want %d", len(files.owners), files.Len())

	first := fakeFile(12345)
	fake.registerResult = 2
	updated, err := files.Update(1, first, nil)
	require.NoError(t, err)
	require.Equal(t, 2, updated, "fixed-file update")
	require.Equal(t, rawRegisterFilesUpdate, fake.registerOpcode)
	require.EqualValues(t, 1, fake.updatedFileOffset)
	require.EqualValues(t, 2, fake.registerCount)
	require.False(t, files.owners[1] != first || files.owners[2] != nil, "fixed-file update did not retain replacement owners")

	updated, err = files.Update(0)
	require.Error(t, err, "empty update")
	require.Zero(t, updated, "empty update")
	updated, err = files.Update(3, first)
	require.Error(t, err, "out-of-bounds update")
	require.Zero(t, updated, "out-of-bounds update")

	second := fakeFile(12346)
	fake.registerResult = 0
	fake.registerErrno = syscall.EBADF
	updated, err = files.Update(0, second)
	require.ErrorIs(t, err, syscall.EBADF, "failed update")
	require.Zero(t, updated, "failed update")
	require.False(t, files.owners[0] != nil, "failed update changed retained owners")

	fake.registerErrno = 0
	fake.registerResult = 1
	updated, err = files.Update(0, second, nil)
	require.Falsef(t, err == nil || updated != 1, "short update: updated=%d err=%v", updated, err)
	require.False(t, files.owners[0] != second || files.owners[1] != first, "short update did not retain exactly the changed owners")
}

// TestCloseRetainsPendingOperations covers the deliberate close: the descriptor
// always closes, but a Ring that still owns operations keeps them, keeps its
// mappings, and roots itself, because nothing can report when the kernel has
// finished with their operands.
func TestCloseRetainsPendingOperations(t *testing.T) {
	ring, fake := newFakeRing(1)
	handle, err := ring.Push(Write(FileFD(fakeFile(12345)), make([]byte, 1), 0))
	require.NoError(t, err)
	_, err = ring.Submit()
	require.NoError(t, err)

	require.ErrorIs(t, ring.Close(), ErrPending, "close with a pending operation")
	require.True(t, fake.closed, "descriptor was not closed")
	require.False(t, fake.unmapped, "ring memory was unmapped with work in flight")
	require.Equal(t, 1, ring.pending.Len(), "pending operand was released")
	_, rooted := abandoned.Load(ring)
	require.True(t, rooted, "retained ring was not rooted, so it can be collected")
	abandoned.Delete(ring)

	// The kernel completing afterwards changes nothing: the Ring is closed.
	fake.complete(handle, 1, 0)
	require.Empty(t, collect(ring.Reap()), "closed ring yielded completions")
	require.NoError(t, ring.Close(), "idempotent Close")
}

// TestCloseReleasesDrainedRing covers the orderly close: everything reaped, so
// Close unmaps and drops every reference and roots nothing.
func TestCloseReleasesDrainedRing(t *testing.T) {
	ring, fake := newFakeRing(1)
	set, err := ring.RegisterBuffers(make([]byte, 16))
	require.NoError(t, err)
	handle, err := ring.Push(Write(FileFD(fakeFile(12345)), make([]byte, 1), 0))
	require.NoError(t, err)
	_, err = ring.Submit()
	require.NoError(t, err)

	fake.complete(handle, 1, 0)
	for range ring.Reap() {
	}
	fake.closeFn = func() {
		require.NotNil(t, ring.buffers,
			"Close released resources before closing the descriptor")
	}
	require.NoError(t, ring.Close())
	require.True(t, fake.closed)
	require.True(t, fake.unmapped, "drained ring kept its mappings")
	require.Nil(t, ring.backend)
	require.Nil(t, ring.buffers)
	_, rooted := abandoned.Load(ring)
	require.False(t, rooted, "drained ring was rooted")

	// Close drops the Ring's own references but leaves the caller's registered
	// table immutable, so its lookups stay safe to call from any goroutine.
	require.Len(t, set.buffers, 1, "Close mutated the caller's buffer table")
	_, err = set.Buffer(0)
	require.NoError(t, err, "registered-buffer lookup after Close")

	require.NoError(t, ring.Close(), "idempotent Close")
}

func TestCancelAllUsesBoundedSynchronousCancellation(t *testing.T) {
	ring, fake := newFakeRing(1)
	require.NoError(t, ring.CancelAll(1500*time.Millisecond))
	spec := syscall.NsecToTimespec((1500 * time.Millisecond).Nanoseconds())
	require.NotNil(t, fake.cancel, "cancel registration")
	require.Equal(t, rawAsyncCancelAny|rawAsyncCancelAll,
		rawCancelFlags(fake.cancel.Flags), "cancel registration")
	require.Equal(t, rawTimespec{Sec: spec.Sec, Nsec: spec.Nsec},
		fake.cancel.Timeout, "cancel registration")
}

func BenchmarkPushSubmitReap(b *testing.B) {
	ring, fake := newFakeRing(1)
	b.ReportAllocs()
	for b.Loop() {
		handle, err := ring.Push(Nop())
		if err != nil {
			b.Fatal(err)
		}
		if _, err := ring.Submit(); err != nil {
			b.Fatal(err)
		}
		fake.complete(handle, 0, 0)
		for completion := range ring.Reap() {
			if completion.Err != nil {
				b.Fatal(completion.Err)
			}
		}

	}
}

func BenchmarkReadPushSubmitReap(b *testing.B) {
	ring, fake := newFakeRing(1)
	buffer := make([]byte, 4096)
	fd := BorrowedFD(12345)
	b.ReportAllocs()
	for b.Loop() {
		handle, err := ring.Push(Read(fd, buffer, 0))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := ring.Submit(); err != nil {
			b.Fatal(err)
		}
		fake.complete(handle, int32(len(buffer)), 0)
		for completion := range ring.Reap() {
			if completion.Err != nil {
				b.Fatal(completion.Err)
			}
		}

	}
}

var benchmarkOperation Op

func BenchmarkConstructOperation(b *testing.B) {
	b.Run("nop", func(b *testing.B) {
		for b.Loop() {
			benchmarkOperation = Nop()
		}
	})

	b.Run("read", func(b *testing.B) {
		buffer := make([]byte, 4096)
		fd := BorrowedFD(12345)
		for b.Loop() {
			benchmarkOperation = Read(fd, buffer, 0)
		}
	})
}

func BenchmarkDrainModifier(b *testing.B) {
	op := Nop()
	b.ReportAllocs()
	for b.Loop() {
		op.Drain()
	}
	benchmarkOperation = op
}

func collect(sequence func(func(Completion) bool)) []Completion {
	var completions []Completion
	for completion := range sequence {
		completions = append(completions, completion)
	}
	return completions
}
