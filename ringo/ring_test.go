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
	head := transport.raw.sq.head
	atomic.StoreUint32(head, atomic.LoadUint32(head)+submitted)
	return uint(submitted), 0
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
		if err := option.apply(&config); err != nil {
			t.Fatal(err)
		}
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
	if config.depth != 32 ||
		config.flags != wantFlags ||
		config.cqEntries != 64 ||
		config.sqThreadCPU != 7 ||
		config.sqThreadIdle != 250 ||
		config.fixedFiles != 8 {
		t.Fatalf("unexpected setup configuration: %+v", config)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	// Encoding is the conformance suite's job; what matters here is that the
	// Ring retains the directory file the operation named.
	pushed := ring.pending.Value(handle.slot).op.(*openAtOp)
	require.Same(t, directory, pushed.dir.file,
		"OpenAt did not retain its directory file")

	result := new(unix.Statx_t)
	if _, err := ring.Push(StatxAt(FileFD(directory), "", 0, unix.STATX_SIZE, result)); err == nil {
		t.Fatal("StatxAt accepted an empty path without AT_EMPTY_PATH")
	}
	if _, err := ring.Push(
		StatxAt(FileFD(directory), "", unix.AT_EMPTY_PATH, unix.STATX_SIZE, result),
	); err != nil {
		t.Fatalf("StatxAt rejected AT_EMPTY_PATH: %v", err)
	}

	ring.files = &FixedFiles{
		ring: ring, count: 1,
	}
	slot, err := ring.files.File(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ring.Push(
		OpenAtDirect(FixedFD(slot), "child", 0, 0, slot),
	); err == nil {
		t.Fatal("OpenAtDirect accepted one slot as both directory and result")
	}
}

func TestPointerInputsAreCopiedAccordingToSemantics(t *testing.T) {
	ring, _ := newFakeRing(4)

	spec := syscall.Timespec{Sec: 7, Nsec: 11}
	timeout, err := ring.Push(Timeout(spec, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
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
	if got := uint32(encodePollMaskForEndian(mask, false)); got != mask {
		t.Fatalf("little-endian poll mask: got %#x want %#x", got, mask)
	}
	if got, want := uint32(encodePollMaskForEndian(mask, true)), uint32(0x56781234); got != want {
		t.Fatalf("big-endian poll mask: got %#x want %#x", got, want)
	}
}

func TestSemanticFlagsRejectRawDiscriminatorBits(t *testing.T) {
	ring, _ := newFakeRing(4)
	if _, err := ring.Push(
		Timeout(syscall.Timespec{}, 1, TimeoutBoottime|TimeoutRealtime),
	); err == nil {
		t.Fatal("Timeout accepted two clock selections")
	}

	target, err := ring.Push(Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ring.Push(
		TimeoutUpdate(syscall.Timespec{}, target, TimeoutUpdateFlags(1<<1)),
	); err == nil {
		t.Fatal("TimeoutUpdate accepted the kernel's private update bit")
	}
	if _, err := ring.Push(
		PollUpdate(target, unix.POLLIN, PollUpdateFlags(1<<1)),
	); err == nil {
		t.Fatal("PollUpdate accepted the kernel's private update-events bit")
	}
	if _, err := ring.Push(Fallocate(FileFD(fakeFile(12340)), -1, 1)); err == nil {
		t.Fatal("Fallocate accepted a negative offset")
	}
	if _, err := ring.Push(
		FallocateMode(BorrowedFD(12340), FallocateFlags(1<<20), 0, 1),
	); err == nil {
		t.Fatal("FallocateMode accepted unknown mode bits")
	}
	if _, err := ring.Push(
		Readv(BorrowedFD(12340), nil, 0, -1),
	); err == nil {
		t.Fatal("Readv accepted negative flags")
	}
	if _, err := ring.Push(Ftruncate(BorrowedFD(12340), -1)); err == nil {
		t.Fatal("Ftruncate accepted a negative length")
	}
}

func TestVectorOperationsUseInlineMetadataForCommonSizes(t *testing.T) {
	vectors := [][]byte{make([]byte, 1), nil, make([]byte, 2)}

	write := newWritevOp(BorrowedFD(1), vectors, 0, 0, FixedBuffer{}, false)
	if unsafe.SliceData(write.buffers) != &write.inlineBuffers[0] ||
		unsafe.SliceData(write.iovecs) != &write.inlineIovecs[0] {
		t.Fatal("small writev allocated metadata outside its operation")
	}
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
	handle, err := pushLocalVectors(ring, fakeFile(12345))
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()

	pending := ring.pending.Value(handle.slot)
	write := pending.op.(*writevOp)
	// Empty vectors are skipped, and each iovec must point at the buffer the
	// operation retained rather than at the caller's original slice header.
	require.Len(t, write.iovecs, 2)
	require.Equal(t, unsafe.SliceData(write.buffers[0]), write.iovecs[0].Base,
		"iovec does not point at the retained buffer")
	require.Equal(t, unsafe.SliceData(write.buffers[2]), write.iovecs[1].Base,
		"iovec does not point at the retained buffer")
}

func pushLocalVectors(ring *Ring, file *os.File) (Handle, error) {
	var first [3]byte
	var second [5]byte
	first[0] = 1
	second[0] = 2
	return ring.Push(Writev(FileFD(file), [][]byte{first[:], nil, second[:]}, 9, 0))
}

func TestDrainMutatesInactiveOp(t *testing.T) {
	op := Nop()
	alias := op

	var sqe rawSQE
	op.Drain()
	alias.prepare(&sqe)
	if got := rawSQEFlags(sqe.Flags); got != rawSqeIODrain {
		t.Fatalf(
			"Drain flags through alias: got %#x want %#x",
			got,
			rawSqeIODrain,
		)
	}
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
	if err != nil {
		t.Fatal(err)
	}

	retry := Nop()
	if _, err := ring.Push(retry); !errors.Is(err, ErrFull) {
		t.Fatalf("full ring error: %v", err)
	}
	retry.Drain()

	if _, err := ring.Submit(); err != nil {
		t.Fatal(err)
	}
	fake.complete(first, 0, 0)
	for range ring.Reap() {
	}
	if _, err := ring.Push(retry); err != nil {
		t.Fatalf("push after failed attempt: %v", err)
	}
}

func TestPushLinkedRejectsInvalidSequenceWithoutMutatingOperations(t *testing.T) {
	ring, fake := newFakeRing(2)
	first, second := Nop(), Nop()

	if _, err := ring.PushLinked(first, Then(LinkType(0xff), second)); err == nil {
		t.Fatal("invalid link type was accepted")
	}
	if _, err := ring.PushLinked(first, Then(LinkSoft, nil)); err == nil {
		t.Fatal("nil linked operation was accepted")
	}
	for index, op := range []Op{first, second} {
		var sqe rawSQE
		op.prepare(&sqe)
		if sqe.Flags != 0 {
			t.Fatalf("operation %d was modified after rejection: flags=%#x", index, sqe.Flags)
		}
	}
	if ring.pending.Len() != 0 || fake.queued() != 0 {
		t.Fatal("invalid sequence changed ring state")
	}

	if _, err := ring.PushLinked(first, Then(LinkHard, second)); err != nil {
		t.Fatalf("operations were consumed by rejected sequence: %v", err)
	}
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
		t.Fatalf("oversized sequence error: %v", err)
	}
	if ring.pending.Len() != 0 || fake.queued() != 0 {
		t.Fatal("oversized sequence changed ring state")
	}

	handles, err := ring.PushLinked(
		Nop(),
		Then(LinkSoft, Nop()),
		Then(LinkSoft, Nop()),
		Then(LinkHard, Nop()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(handles) != 4 {
		t.Fatalf("handles: got %d want 4", len(handles))
	}
	if rawSQEFlags(fake.sqe(0).Flags) != rawSqeIOLink {
		t.Fatalf("soft-link flags: %#x", fake.sqe(0).Flags)
	}
	if rawSQEFlags(fake.sqe(1).Flags) != rawSqeIOLink {
		t.Fatalf("soft-link boundary flags: %#x", fake.sqe(1).Flags)
	}
	if rawSQEFlags(fake.sqe(2).Flags) != rawSqeIOHardlink {
		t.Fatalf("hard-link flags: %#x", fake.sqe(2).Flags)
	}
	if fake.sqe(3).Flags != 0 {
		t.Fatalf("last flags: %#x", fake.sqe(3).Flags)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	_, _ = ring.Submit()
	fake.complete(first, 7, 0)
	completions := collect(ring.Reap())
	if len(completions) != 1 || completions[0].Handle != first ||
		completions[0].Result != 7 || completions[0].Err != nil {
		t.Fatalf("completion: %+v", completions)
	}
	if ring.pending.Len() != 0 || fake.reaped() != 1 {
		t.Fatal("final completion did not advance and release")
	}

	second, err := ring.Push(Nop())
	if err != nil {
		t.Fatal(err)
	}
	if first.slot == second.slot {
		t.Fatal("slot generation did not advance")
	}
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
	if err != nil {
		t.Fatal(err)
	}
	_, _ = ring.Submit()
	fake.complete(handle, 1, rawCQEMore)
	first := collect(ring.Reap())
	if len(first) != 1 || !first[0].Flags.More() {
		t.Fatalf("multishot completion: %+v", first)
	}
	if _, ok := ring.pending.TryValue(handle.slot); !ok {
		t.Fatal("IORING_CQE_F_MORE released the pending slot")
	}

	fake.complete(handle, -int32(syscall.EIO), 0)
	final := collect(ring.Reap())
	if len(final) != 1 || !errors.Is(final[0].Err, syscall.EIO) {
		t.Fatalf("final completion: %+v", final)
	}
	if ring.pending.Len() != 0 {
		t.Fatal("final multishot completion did not release the slot")
	}
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
		if fake.reaped() != 1 || ring.pending.Len() != 1 {
			t.Fatalf("after break: cqHead=%d pending=%d", fake.reaped(), ring.pending.Len())
		}
		if got := len(collect(ring.Reap())); got != 1 {
			t.Fatalf("remaining completions: got %d want 1", got)
		}
	})

	t.Run("panic", func(t *testing.T) {
		ring, fake := newFakeRing(1)
		handle, _ := ring.Push(Nop())
		_, _ = ring.Submit()
		fake.complete(handle, 0, 0)
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("reap body did not panic")
				}
			}()
			for range ring.Reap() {
				panic("body")
			}
		}()
		if fake.reaped() != 1 || ring.pending.Len() != 0 {
			t.Fatalf(
				"after panic: cqHead=%d pending=%d",
				fake.reaped(), ring.pending.Len(),
			)
		}
	})
}

func TestRegisteredResourcesAreRingScopedAndRetained(t *testing.T) {
	ring, fake := newFakeRing(2)
	t.Cleanup(func() {
		if err := ring.Close(); err != nil {
			t.Errorf("close fake ring: %v", err)
		}
	})
	ring.files = &FixedFiles{ring: ring, count: 2}
	file, err := ring.files.File(1)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 64)
	set, err := ring.RegisterBuffers(data)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := set.Bind(data[8:24])
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.registeredBuffers) != 1 ||
		fake.registeredBuffers[0].Base != unsafe.SliceData(data) {
		t.Fatal("registration did not use the retained backing buffer")
	}

	handle, err := ring.Push(ReadFixed(FixedFD(file), buffer, 3))
	if err != nil {
		t.Fatal(err)
	}
	sqe := fake.sqe(0)
	if rawSQEFlags(sqe.Flags)&rawSqeFixedFile == 0 || sqe.Buf_index != 0 ||
		sqe.Addr != uint64(slicePtr(data[8:24])) {
		t.Fatalf("fixed/direct SQE: %+v", sqe)
	}
	if _, ok := ring.pending.TryValue(handle.slot); !ok {
		t.Fatal("fixed operation was not retained")
	}

	other, _ := newFakeRing(1)
	if _, err := other.Push(Read(FixedFD(file), make([]byte, 1), 0)); !errors.Is(err, ErrWrongRing) {
		t.Fatalf("foreign fixed file: %v", err)
	}
	if _, err := other.Push(ReadFixed(FileFD(fakeFile(12346)), buffer, 0)); !errors.Is(err, ErrWrongRing) {
		t.Fatalf("foreign registered buffer: %v", err)
	}
	if _, err := set.Bind(make([]byte, 1)); err == nil {
		t.Fatal("Bind accepted an unregistered buffer")
	}

	if _, err := ring.Submit(); err != nil {
		t.Fatal(err)
	}
	fake.complete(handle, 16, 0)
	if completions := collect(ring.Reap()); len(completions) != 1 {
		t.Fatalf("final completions: got %d want 1", len(completions))
	}
	if err := ring.Close(); err != nil {
		t.Fatalf("close idle ring: %v", err)
	}
	if len(set.buffers) != 1 {
		t.Fatal("Close mutated the caller's buffer table")
	}
}

func TestFixedFilesUpdate(t *testing.T) {
	ring, fake := newFakeRing(2)
	files, err := ring.RegisterSparseFiles(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(files.owners) != files.Len() {
		t.Fatalf("owner slots: got %d want %d", len(files.owners), files.Len())
	}

	first := fakeFile(12345)
	fake.registerResult = 2
	updated, err := files.Update(1, first, nil)
	if err != nil {
		t.Fatal(err)
	}
	if updated != 2 ||
		fake.registerOpcode != rawRegisterFilesUpdate ||
		fake.updatedFileOffset != 1 ||
		fake.registerCount != 2 {
		t.Fatalf(
			"fixed-file update: updated=%d offset=%d count=%d",
			updated,
			fake.updatedFileOffset,
			fake.registerCount,
		)
	}
	if files.owners[1] != first || files.owners[2] != nil {
		t.Fatal("fixed-file update did not retain replacement owners")
	}

	if updated, err := files.Update(0); err == nil || updated != 0 {
		t.Fatalf("empty update: updated=%d err=%v", updated, err)
	}
	if updated, err := files.Update(3, first); err == nil || updated != 0 {
		t.Fatalf("out-of-bounds update: updated=%d err=%v", updated, err)
	}

	second := fakeFile(12346)
	fake.registerResult = 0
	fake.registerErrno = syscall.EBADF
	if updated, err := files.Update(0, second); !errors.Is(err, syscall.EBADF) ||
		updated != 0 {
		t.Fatalf("failed update: updated=%d err=%v", updated, err)
	}
	if files.owners[0] != nil {
		t.Fatal("failed update changed retained owners")
	}

	fake.registerErrno = 0
	fake.registerResult = 1
	updated, err = files.Update(0, second, nil)
	if err == nil || updated != 1 {
		t.Fatalf("short update: updated=%d err=%v", updated, err)
	}
	if files.owners[0] != second || files.owners[1] != first {
		t.Fatal("short update did not retain exactly the changed owners")
	}
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
	if err := ring.CancelAll(1500 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	spec := syscall.NsecToTimespec((1500 * time.Millisecond).Nanoseconds())
	if fake.cancel == nil ||
		rawCancelFlags(fake.cancel.Flags) != rawAsyncCancelAny|rawAsyncCancelAll ||
		fake.cancel.Timeout != (rawTimespec{Sec: spec.Sec, Nsec: spec.Nsec}) {
		t.Fatalf("cancel registration: %+v", fake.cancel)
	}
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
