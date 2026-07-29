//go:build linux

package ringo

import (
	"errors"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/miretskiy/dio/internal/intrusive"
	"golang.org/x/sys/unix"
)

type fakeBackend struct {
	capacity uint32
	sqes     []rawSQE
	sqHead   int
	cqes     []rawCQE
	cqHead   int

	submitted uint
	submitErr error

	registeredBuffers []syscall.Iovec
	registeredFiles   uint32
	updatedFileOffset uint32
	cancel            *rawSyncCancelReg
	registerOpcode    rawRegisterOpcode
	registerArgument  unsafe.Pointer
	registerCount     uint32
	registerResult    uint
	registerErrno     syscall.Errno
	closeFn           func()
	closed            bool
}

func (fake *fakeBackend) sqCapacity() uint32 {
	return fake.capacity
}

func (fake *fakeBackend) sqSpaceLeft() uint32 {
	return fake.capacity - uint32(len(fake.sqes)-fake.sqHead)
}

func (fake *fakeBackend) getSQE() *rawSQE {
	if fake.sqSpaceLeft() == 0 {
		return nil
	}
	fake.sqes = append(fake.sqes, rawSQE{})
	return &fake.sqes[len(fake.sqes)-1]
}

func (fake *fakeBackend) submitAndWait(uint32) (uint, error) {
	submitted := uint(len(fake.sqes) - fake.sqHead)
	fake.sqHead = len(fake.sqes)
	if fake.submitted != 0 {
		submitted = fake.submitted
	}
	return submitted, fake.submitErr
}

func (fake *fakeBackend) cqReady() uint32 {
	return uint32(len(fake.cqes) - fake.cqHead)
}

func (fake *fakeBackend) peekCQE() *rawCQE {
	if fake.cqHead == len(fake.cqes) {
		return nil
	}
	return &fake.cqes[fake.cqHead]
}

func (fake *fakeBackend) advanceCQ(count uint32) {
	fake.cqHead += int(count)
}

func (fake *fakeBackend) registerBuffers(iovecs []syscall.Iovec) (uint, error) {
	fake.registeredBuffers = append([]syscall.Iovec(nil), iovecs...)
	return 0, nil
}

func (fake *fakeBackend) registerFilesSparse(count uint32) (uint, error) {
	fake.registeredFiles = count
	return 0, nil
}

func (fake *fakeBackend) registerSyncCancel(cancel *rawSyncCancelReg) (uint, error) {
	copy := *cancel
	fake.cancel = &copy
	return 0, nil
}

func (fake *fakeBackend) register(
	opcode rawRegisterOpcode,
	argument unsafe.Pointer,
	count uint32,
) (uint, syscall.Errno) {
	fake.registerOpcode = opcode
	fake.registerArgument = argument
	fake.registerCount = count
	if opcode == rawRegisterFilesUpdate {
		update := (*rawFilesUpdate)(argument)
		fake.updatedFileOffset = update.Offset
	}
	return fake.registerResult, fake.registerErrno
}

func (fake *fakeBackend) queueExit() error {
	if fake.closeFn != nil {
		fake.closeFn()
	}
	fake.closed = true
	return nil
}

func (fake *fakeBackend) complete(
	handle Handle,
	result int32,
	flags rawCQEFlags,
) {
	fake.cqes = append(fake.cqes, rawCQE{
		Data:  uint64(handle.slot),
		Res:   result,
		Flags: uint32(flags),
	})
}

func newFakeRing(depth int) (*Ring, *fakeBackend) {
	fake := &fakeBackend{capacity: uint32(depth)}
	ring := &Ring{
		backend: fake,
		id:      nextRingID.Add(1),
		pending: intrusive.MakeFixedList[pendingSlot](depth),
	}
	return ring, fake
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

func TestPushRetainsStableOperationStorage(t *testing.T) {
	ring, fake := newFakeRing(4)
	file := fakeFile(12345)
	handle, err := pushLocalBuffer(ring, file)
	if err != nil {
		t.Fatal(err)
	}

	growStack(32)
	runtime.GC()

	pending := ring.pending.Value(handle.slot)
	read := pending.op.(*readOp)
	buffer := read.buffer
	if got, want := fake.sqes[0].Addr, uint64(slicePtr(buffer)); got != want {
		t.Fatalf("encoded buffer address moved: got %#x want %#x", got, want)
	}
	if read.fd.file != file || buffer[0] != 42 {
		t.Fatal("pushed operation did not retain its file and buffer")
	}

	open := OpenAt(BorrowedFD(-100), "stable-name", 0, 0)
	openHandle, err := ring.Push(open)
	if err != nil {
		t.Fatal(err)
	}
	pushedOpen := ring.pending.Value(openHandle.slot)
	path := pushedOpen.op.(*openAtOp).path
	if got, want := fake.sqes[1].Addr, uint64(slicePtr(path)); got != want {
		t.Fatalf("encoded path address: got %#x want %#x", got, want)
	}
	if got := string(path[:len(path)-1]); got != "stable-name" {
		t.Fatalf("pushed path changed with source Op: %q", got)
	}
}

func TestPathOperationsUseTypedDirectoryDescriptors(t *testing.T) {
	ring, fake := newFakeRing(4)
	directory := fakeFile(12340)
	handle, err := ring.Push(OpenAt(FileFD(directory), "child", 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	pushed := ring.pending.Value(handle.slot).op.(*openAtOp)
	if pushed.dir.file != directory || fake.sqes[0].Fd != int32(directory.Fd()) {
		t.Fatal("OpenAt did not retain and encode its directory file")
	}

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
	ring, fake := newFakeRing(4)

	spec := syscall.Timespec{Sec: 7, Nsec: 11}
	timeout, err := ring.Push(Timeout(spec, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	spec.Sec = 99

	ownedSpec := &ring.pending.Value(timeout.slot).op.(*timeoutOp).spec
	if got, want := *ownedSpec, (syscall.Timespec{Sec: 7, Nsec: 11}); got != want {
		t.Fatalf("timeout did not own an input copy: got %+v want %+v", got, want)
	}
	if got, want := fake.sqes[0].Addr, uint64(uintptr(unsafe.Pointer(ownedSpec))); got != want {
		t.Fatalf("timeout address: got %#x want %#x", got, want)
	}

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

func pushLocalBuffer(ring *Ring, file *os.File) (Handle, error) {
	var buffer [32]byte
	buffer[0] = 42
	return ring.Push(Read(FileFD(file), buffer[:], 17))
}

func growStack(depth int) int {
	var data [128]byte
	if depth == 0 {
		return int(data[0])
	}
	return int(data[depth%len(data)]) + growStack(depth-1)
}

func TestPushBuildsOwnedIovecs(t *testing.T) {
	ring, fake := newFakeRing(1)
	handle, err := pushLocalVectors(ring, fakeFile(12345))
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()

	pending := ring.pending.Value(handle.slot)
	write := pending.op.(*writevOp)
	if len(write.iovecs) != 2 {
		t.Fatalf("iovecs: got %d want 2", len(write.iovecs))
	}
	if got, want := fake.sqes[0].Addr, uint64(slicePtr(write.iovecs)); got != want {
		t.Fatalf("encoded iovec address: got %#x want %#x", got, want)
	}
	if write.iovecs[0].Base != unsafe.SliceData(write.buffers[0]) {
		t.Fatal("iovec does not point at the retained buffer")
	}
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

func TestConsumedOperationMisusePanics(t *testing.T) {
	ring, fake := newFakeRing(1)
	op := Nop()
	alias := op
	handle, err := ring.Push(op)
	if err != nil {
		t.Fatal(err)
	}

	mustPanic(t, func() { alias.Drain() })

	if _, err := ring.Submit(); err != nil {
		t.Fatal(err)
	}
	fake.complete(handle, 0, 0)
	for range ring.Reap() {
	}
	mustPanic(t, func() { alias.Drain() })
	mustPanic(t, func() {
		_, _ = ring.Push(alias)
	})
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

func TestPushLinkedRejectsDuplicateOperation(t *testing.T) {
	ring, _ := newFakeRing(2)
	op := Nop()
	mustPanic(t, func() {
		_, _ = ring.PushLinked(op, Then(LinkSoft, op))
	})
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
	if ring.pending.Len() != 0 || len(fake.sqes) != 0 {
		t.Fatal("invalid sequence changed ring state")
	}

	if _, err := ring.PushLinked(first, Then(LinkHard, second)); err != nil {
		t.Fatalf("operations were consumed by rejected sequence: %v", err)
	}
}

func TestReadWriteOperationsReleaseAfterCompletionYield(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		ring, fake := newFakeRing(1)
		operation := Read(BorrowedFD(12345), make([]byte, 32), 0)
		state := operation.(*readOp)
		handle, err := ring.Push(operation)
		if err != nil {
			t.Fatal(err)
		}
		fake.complete(handle, 32, 0)
		for range ring.Reap() {
			if state.buffer == nil || state.fd.kind != descriptorBorrowed {
				t.Fatal("read operation released before its completion was yielded")
			}
		}
		if state.buffer != nil || state.fd != (FD{}) || !state.consumed {
			t.Fatal("read operation was not completely cleared before pooling")
		}
		mustPanic(t, func() { operation.Drain() })
	})

	t.Run("write", func(t *testing.T) {
		ring, fake := newFakeRing(1)
		operation := Write(BorrowedFD(12345), make([]byte, 32), 0)
		state := operation.(*writeOp)
		handle, err := ring.Push(operation)
		if err != nil {
			t.Fatal(err)
		}
		fake.complete(handle, 32, 0)
		for range ring.Reap() {
			if state.buffer == nil || state.fd.kind != descriptorBorrowed {
				t.Fatal("write operation released before its completion was yielded")
			}
		}
		if state.buffer != nil || state.fd != (FD{}) || !state.consumed {
			t.Fatal("write operation was not completely cleared before pooling")
		}
		mustPanic(t, func() { operation.Drain() })
	})
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
	if ring.pending.Len() != 0 || len(fake.sqes) != 0 {
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
	if rawSQEFlags(fake.sqes[0].Flags) != rawSqeIOLink {
		t.Fatalf("soft-link flags: %#x", fake.sqes[0].Flags)
	}
	if rawSQEFlags(fake.sqes[1].Flags) != rawSqeIOLink {
		t.Fatalf("soft-link boundary flags: %#x", fake.sqes[1].Flags)
	}
	if rawSQEFlags(fake.sqes[2].Flags) != rawSqeIOHardlink {
		t.Fatalf("hard-link flags: %#x", fake.sqes[2].Flags)
	}
	if fake.sqes[3].Flags != 0 {
		t.Fatalf("last flags: %#x", fake.sqes[3].Flags)
	}
}

func TestSubmitErrorPreservesOwnership(t *testing.T) {
	ring, fake := newFakeRing(1)
	handle, err := ring.Push(Nop())
	if err != nil {
		t.Fatal(err)
	}
	fake.submitted = 1
	fake.submitErr = syscall.EAGAIN
	submitted, err := ring.Submit()
	if submitted != 1 || !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("submit: progress=%d error=%v", submitted, err)
	}
	if _, ok := ring.pending.TryValue(handle.slot); !ok {
		t.Fatal("submit error released pushed ownership")
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
	if ring.pending.Len() != 0 || fake.cqHead != 1 {
		t.Fatal("final completion did not advance and release")
	}

	second, err := ring.Push(Nop())
	if err != nil {
		t.Fatal(err)
	}
	if first.slot == second.slot {
		t.Fatal("slot generation did not advance")
	}
	fake.complete(first, 0, 0)
	completions = collect(ring.Reap())
	if len(completions) != 1 || !errors.Is(completions[0].Err, ErrCorruptCompletion) {
		t.Fatalf("stale completion: %+v", completions)
	}
	if _, ok := ring.pending.TryValue(second.slot); !ok {
		t.Fatal("stale completion released a newer operation")
	}
	if _, err := ring.Push(Nop()); !errors.Is(err, ErrCorruptCompletion) {
		t.Fatalf("ring did not preserve fatal consistency error: %v", err)
	}
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
		if fake.cqHead != 1 || ring.pending.Len() != 1 {
			t.Fatalf("after break: cqHead=%d pending=%d", fake.cqHead, ring.pending.Len())
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
		if fake.cqHead != 1 || ring.pending.Len() != 0 {
			t.Fatalf(
				"after panic: cqHead=%d pending=%d",
				fake.cqHead, ring.pending.Len(),
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
	sqe := fake.sqes[0]
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

func TestCloseTearsDownKernelBeforeClearingReferences(t *testing.T) {
	ring, fake := newFakeRing(1)
	set, err := ring.RegisterBuffers(make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ring.Push(Write(FileFD(fakeFile(12345)), make([]byte, 1), 0)); err != nil {
		t.Fatal(err)
	}
	fake.closeFn = func() {
		if ring.pending.Len() != 1 || ring.buffers == nil || len(set.buffers) != 1 {
			t.Fatal("Close cleared references before kernel teardown")
		}
	}
	if err := ring.Close(); err != nil {
		t.Fatal(err)
	}
	if !fake.closed || ring.pending.Len() != 0 || ring.backend != nil ||
		ring.buffers != nil || len(set.buffers) != 0 {
		t.Fatal("Close did not release state after kernel teardown")
	}
	if err := ring.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}
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
		fake.sqes = fake.sqes[:0]
		fake.sqHead = 0
		fake.cqes = fake.cqes[:0]
		fake.cqHead = 0
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
		fake.sqes = fake.sqes[:0]
		fake.sqHead = 0
		fake.cqes = fake.cqes[:0]
		fake.cqHead = 0
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

func mustPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("operation did not panic")
		}
	}()
	f()
}

func collect(sequence func(func(Completion) bool)) []Completion {
	var completions []Completion
	for completion := range sequence {
		completions = append(completions, completion)
	}
	return completions
}
