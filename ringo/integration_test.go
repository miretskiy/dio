//go:build linux

package ringo

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func newIntegrationRing(t *testing.T, options ...Option) *Ring {
	t.Helper()
	ring, err := New(options...)
	if err == nil {
		t.Cleanup(func() {
			if err := ring.Close(); err != nil {
				t.Errorf("close ring: %v", err)
			}
		})
		return ring
	}
	if os.Getenv("RINGO_REQUIRE_IO_URING") != "" {
		t.Fatalf("io_uring is required: %v", err)
	}
	if errors.Is(err, syscall.ENOSYS) ||
		errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.EACCES) ||
		errors.Is(err, syscall.EOPNOTSUPP) {
		t.Skipf("io_uring unavailable: %v", err)
	}
	t.Fatalf("create io_uring: %v", err)
	return nil
}

func awaitCompletions(t *testing.T, ring *Ring, count int) map[Handle]Completion {
	t.Helper()
	completions := make(map[Handle]Completion, count)
	for len(completions) < count {
		_, err := ring.SubmitAndWait(1)
		if err != nil {
			if errors.Is(err, syscall.EINTR) ||
				errors.Is(err, syscall.EAGAIN) ||
				errors.Is(err, syscall.EBUSY) {
				continue
			}
			t.Fatalf("submit and wait: %v", err)
		}
		for completion := range ring.Reap() {
			if completion.Err != nil {
				t.Fatalf("completion %v: %v", completion.Handle, completion.Err)
			}
			completions[completion.Handle] = completion
		}
	}
	return completions
}

func TestRingIntegrationReadWriteAndVectoredIO(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(8))
	file, err := os.CreateTemp(t.TempDir(), "ringo-io-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if err := file.Truncate(4096); err != nil {
		t.Fatal(err)
	}

	write := []byte("lifetime-safe")
	read := make([]byte, len(write))
	handles, err := ring.PushLinked(
		Write(file, write, 0),
		Then(LinkSoft, Fdatasync(file)),
		Then(LinkSoft, Read(file, read, 0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	growStack(64)
	runtime.GC()
	completions := awaitCompletions(t, ring, len(handles))
	if got := completions[handles[0]].Result; got != len(write) {
		t.Fatalf("write result: got %d want %d", got, len(write))
	}
	if got := completions[handles[2]].Result; got != len(read) {
		t.Fatalf("read result: got %d want %d", got, len(read))
	}
	if string(read) != string(write) {
		t.Fatalf("read data: got %q want %q", read, write)
	}

	vectorWrite := [][]byte{[]byte("vector-"), nil, []byte("io")}
	vectorRead := [][]byte{make([]byte, 7), nil, make([]byte, 2)}
	handles, err = ring.PushLinked(
		Writev(file, vectorWrite, 128),
		Then(LinkSoft, Readv(file, vectorRead, 128)),
	)
	if err != nil {
		t.Fatal(err)
	}
	completions = awaitCompletions(t, ring, len(handles))
	if got := completions[handles[0]].Result; got != 9 {
		t.Fatalf("writev result: got %d want 9", got)
	}
	if got := completions[handles[1]].Result; got != 9 {
		t.Fatalf("readv result: got %d want 9", got)
	}
	if got := string(vectorRead[0]) + string(vectorRead[2]); got != "vector-io" {
		t.Fatalf("readv data: got %q want vector-io", got)
	}
}

func TestRingIntegrationDirectFileLifecycle(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(8), WithFixedFiles(1))
	slot, err := ring.FixedFiles().File(0)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "direct.dat")
	write := []byte("direct-file")
	read := make([]byte, len(write))
	open := OpenAtDirect(
		BorrowedFD(unix.AT_FDCWD),
		path,
		unix.O_CREAT|unix.O_TRUNC|unix.O_RDWR,
		0o600,
		slot,
	)
	openHandle, err := ring.Push(open)
	if err != nil {
		t.Fatal(err)
	}
	openCompletions := awaitCompletions(t, ring, 1)
	if completion := openCompletions[openHandle]; completion.Err != nil {
		t.Fatalf("direct open: %v", completion.Err)
	}

	handles, err := ring.PushLinked(
		FallocateDirect(slot, 0, 4096),
		Then(LinkHard, WriteDirect(slot, write, 0)),
		Then(LinkHard, FdatasyncDirect(slot)),
		Then(LinkHard, ReadDirect(slot, read, 0)),
		Then(LinkHard, CloseDirect(slot)),
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	completions := awaitCompletions(t, ring, len(handles))
	if got := completions[handles[1]].Result; got != len(write) {
		t.Fatalf("direct write result: got %d want %d", got, len(write))
	}
	if got := completions[handles[3]].Result; got != len(read) {
		t.Fatalf("direct read result: got %d want %d", got, len(read))
	}
	if string(read) != string(write) {
		t.Fatalf("direct read data: got %q want %q", read, write)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("direct-opened file: %v", err)
	}
}

func TestRingIntegrationLinkedDirectOpen(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(4), WithFixedFiles(1))
	slot, err := ring.FixedFiles().File(0)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "linked-direct.dat")
	data := []byte("open-and-write")
	handles, err := ring.PushLinked(
		OpenDirect(path, unix.O_CREAT|unix.O_RDWR, 0o600, slot),
		Then(LinkSoft, WriteDirect(slot, data, 0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	completions := awaitCompletions(t, ring, len(handles))
	for _, handle := range handles {
		if completion := completions[handle]; completion.Err != nil {
			t.Fatalf("linked direct operation: %v", completion.Err)
		}
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != string(data) {
		t.Fatalf("linked direct file: data=%q err=%v", got, err)
	}
	closeHandle, err := ring.Push(CloseDirect(slot))
	if err != nil {
		t.Fatal(err)
	}
	if completion := awaitCompletions(t, ring, 1)[closeHandle]; completion.Err != nil {
		t.Fatalf("close direct file: %v", completion.Err)
	}
}

func TestRingIntegrationProbeEventFDAndRegisteredFiles(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(4))

	probe, err := ring.Probe()
	if err != nil {
		t.Fatal(err)
	}
	if !probe.Supports(Nop()) {
		t.Fatal("kernel probe did not report IORING_OP_NOP")
	}

	eventFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	eventFile := os.NewFile(uintptr(eventFD), "ringo-eventfd")
	t.Cleanup(func() { _ = eventFile.Close() })
	if err := ring.RegisterEventFD(eventFile); err != nil {
		t.Fatal(err)
	}
	eventHandle, err := ring.Push(Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ring.SubmitAndWait(1); err != nil {
		t.Fatal(err)
	}
	var notification [8]byte
	if count, err := unix.Read(eventFD, notification[:]); err != nil || count != len(notification) {
		t.Fatalf("read eventfd notification: count=%d err=%v", count, err)
	}
	if value := binary.NativeEndian.Uint64(notification[:]); value == 0 {
		t.Fatal("eventfd notification count is zero")
	}
	completions := collect(ring.Reap())
	if len(completions) != 1 || completions[0].Handle != eventHandle ||
		completions[0].Err != nil {
		t.Fatalf("eventfd completion: %+v", completions)
	}
	if err := ring.UnregisterEventFD(); err != nil {
		t.Fatal(err)
	}

	file, err := os.CreateTemp(t.TempDir(), "ringo-registered-file-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	files, err := ring.RegisterFiles(file)
	if err != nil {
		t.Fatal(err)
	}
	slot, err := files.File(0)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("registered-file")
	write, err := ring.Push(WriteDirect(slot, data, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got := awaitCompletions(t, ring, 1)[write].Result; got != len(data) {
		t.Fatalf("registered-file write: got %d want %d", got, len(data))
	}
}

func TestRingIntegrationFixedFilesUpdate(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(2), WithFixedFiles(1))
	files := ring.FixedFiles()
	slot, err := files.File(0)
	if err != nil {
		t.Fatal(err)
	}

	first, err := os.CreateTemp(t.TempDir(), "ringo-fixed-update-first-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := os.CreateTemp(t.TempDir(), "ringo-fixed-update-second-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })

	for _, test := range []struct {
		file *os.File
		data []byte
	}{
		{file: first, data: []byte("first")},
		{file: second, data: []byte("second")},
	} {
		if updated, err := files.Update(0, test.file); err != nil || updated != 1 {
			t.Fatalf("update fixed file: updated=%d err=%v", updated, err)
		}
		handle, err := ring.Push(WriteDirect(slot, test.data, 0))
		if err != nil {
			t.Fatal(err)
		}
		if got := awaitCompletions(t, ring, 1)[handle].Result; got != len(test.data) {
			t.Fatalf("fixed-file write: got %d want %d", got, len(test.data))
		}
	}

	if got, err := os.ReadFile(first.Name()); err != nil || string(got) != "first" {
		t.Fatalf("first fixed file: data=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(second.Name()); err != nil || string(got) != "second" {
		t.Fatalf("second fixed file: data=%q err=%v", got, err)
	}
	if updated, err := files.Update(0, nil); err != nil || updated != 1 {
		t.Fatalf("clear fixed file: updated=%d err=%v", updated, err)
	}
}

func TestRingIntegrationMmapBackedFixedBuffer(t *testing.T) {
	memory, err := syscall.Mmap(
		-1,
		0,
		4096,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANON,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := syscall.Munmap(memory); err != nil {
			t.Errorf("unmap registered buffer: %v", err)
		}
	})

	// Register the Ring cleanup after the mapping cleanup so it runs first.
	ring := newIntegrationRing(t, WithDepth(2))
	copy(memory, "mmap-registered")
	set, err := ring.RegisterBuffers(memory)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := set.Buffer(0)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "ringo-mmap-buffer-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	handle, err := ring.Push(WriteFixed(file, buffer, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got := awaitCompletions(t, ring, 1)[handle].Result; got != len(memory) {
		t.Fatalf("mmap registered-buffer write: got %d want %d", got, len(memory))
	}
}

func TestRingIntegrationSetupModes(t *testing.T) {
	assertNop := func(t *testing.T, ring *Ring) {
		t.Helper()
		handle, err := ring.Push(Nop())
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := awaitCompletions(t, ring, 1)[handle]; !ok {
			t.Fatal("setup-mode completion is missing")
		}
	}

	t.Run("queue-layout-and-taskrun", func(t *testing.T) {
		ring := newIntegrationRing(
			t,
			WithDepth(2),
			WithCQSize(4),
			WithTaskRunFlag(),
		)
		assertNop(t, ring)
	})

	t.Run("submission-queue-polling", func(t *testing.T) {
		ring := newIntegrationRing(t, WithDepth(2), WithSQPoll())
		handle, err := ring.Push(Nop())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ring.Submit(); err != nil {
			t.Fatal(err)
		}
		if _, ok := awaitCompletions(t, ring, 1)[handle]; !ok {
			t.Fatal("SQPOLL completion is missing")
		}
	})
}
