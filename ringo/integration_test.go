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

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newIntegrationRing(t *testing.T, options ...Option) *Ring {
	t.Helper()
	ring, err := New(options...)
	if err == nil {
		t.Cleanup(func() {
			require.NoError(t, ring.Close(), "close ring")
		})
		return ring
	}
	require.Falsef(t, os.Getenv("RINGO_REQUIRE_IO_URING") != "", "io_uring is required: %v", err)
	if errors.Is(err, syscall.ENOSYS) ||
		errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.EACCES) ||
		errors.Is(err, syscall.EOPNOTSUPP) {
		t.Skipf("io_uring unavailable: %v", err)
	}
	require.NoError(t, err, "create io_uring")
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
			require.NoError(t, err, "submit and wait")
		}
		for completion := range ring.Reap() {
			require.Falsef(t, completion.Err != nil, "completion %v: %v", completion.Handle, completion.Err)
			completions[completion.Handle] = completion
		}
	}
	return completions
}

func TestRingIntegrationReadWriteAndVectoredIO(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(8))
	file, err := os.CreateTemp(t.TempDir(), "ringo-io-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	require.NoError(t, file.Truncate(4096))

	write := []byte("lifetime-safe")
	read := make([]byte, len(write))
	handles, err := ring.PushLinked(
		Write(FileFD(file), write, 0),
		Then(LinkSoft, Fdatasync(FileFD(file))),
		Then(LinkSoft, Read(FileFD(file), read, 0)),
	)
	require.False(t, err != nil, err)
	growStack(64)
	runtime.GC()
	completions := awaitCompletions(t, ring, len(handles))
	require.EqualValues(t, len(write), completions[handles[0]].Result, "write result")
	require.EqualValues(t, len(read), completions[handles[2]].Result, "read result")
	require.Falsef(t, string(read) != string(write), "read data: got %q want %q", read, write)

	vectorWrite := [][]byte{[]byte("vector-"), nil, []byte("io")}
	vectorRead := [][]byte{make([]byte, 7), nil, make([]byte, 2)}
	handles, err = ring.PushLinked(
		Writev(FileFD(file), vectorWrite, 128, 0),
		Then(LinkSoft, Readv(FileFD(file), vectorRead, 128, 0)),
	)
	require.False(t, err != nil, err)
	completions = awaitCompletions(t, ring, len(handles))
	require.EqualValues(t, 9, completions[handles[0]].Result, "writev result")
	require.EqualValues(t, 9, completions[handles[1]].Result, "readv result")
	require.EqualValues(t, "vector-io", string(vectorRead[0])+string(vectorRead[2]), "readv data")
}

func TestRingIntegrationDirectFileLifecycle(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(8), WithFixedFiles(1))
	slot, err := ring.FixedFiles().File(0)
	require.NoError(t, err)
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
	require.NoError(t, err)
	openCompletions := awaitCompletions(t, ring, 1)
	require.NoError(t, openCompletions[openHandle].Err, "direct open")

	handles, err := ring.PushLinked(
		Fallocate(FixedFD(slot), 0, 4096),
		Then(LinkHard, Write(FixedFD(slot), write, 0)),
		Then(LinkHard, Fdatasync(FixedFD(slot))),
		Then(LinkHard, Read(FixedFD(slot), read, 0)),
		Then(LinkHard, CloseDirect(slot)),
	)
	require.False(t, err != nil, err)
	runtime.GC()
	completions := awaitCompletions(t, ring, len(handles))
	require.EqualValues(t, len(write), completions[handles[1]].Result, "direct write result")
	require.EqualValues(t, len(read), completions[handles[3]].Result, "direct read result")
	require.Falsef(t, string(read) != string(write), "direct read data: got %q want %q", read, write)
	_, err = os.Stat(path)
	require.NoError(t, err, "direct-opened file")
}

func TestRingIntegrationLinkedDirectOpen(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(4), WithFixedFiles(1))
	slot, err := ring.FixedFiles().File(0)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "linked-direct.dat")
	data := []byte("open-and-write")
	handles, err := ring.PushLinked(
		OpenAtDirect(AtCWD(), path, unix.O_CREAT|unix.O_RDWR, 0o600, slot),
		Then(LinkSoft, Write(FixedFD(slot), data, 0)),
	)
	require.False(t, err != nil, err)
	completions := awaitCompletions(t, ring, len(handles))
	for _, handle := range handles {
		require.NoError(t, completions[handle].Err, "linked direct operation")
	}
	contents, err := os.ReadFile(path)
	require.NoError(t, err, "linked direct file")
	require.Equal(t, string(data), string(contents), "linked direct file")
	closeHandle, err := ring.Push(CloseDirect(slot))
	require.NoError(t, err)
	require.NoError(t, awaitCompletions(t, ring, 1)[closeHandle].Err,
		"close direct file")
}

func TestRingIntegrationProbeEventFDAndRegisteredFiles(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(4))

	probe, err := ring.Probe()
	require.NoError(t, err)
	require.True(t, probe.Supports(Nop()), "kernel probe did not report IORING_OP_NOP")

	eventFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	require.NoError(t, err)
	eventFile := os.NewFile(uintptr(eventFD), "ringo-eventfd")
	t.Cleanup(func() { _ = eventFile.Close() })
	require.NoError(t, ring.RegisterEventFD(eventFile))
	eventHandle, err := ring.Push(Nop())
	require.NoError(t, err)
	_, err = ring.SubmitAndWait(1)
	require.NoError(t, err)
	var notification [8]byte
	count, err := unix.Read(eventFD, notification[:])
	require.NoError(t, err, "read eventfd notification")
	require.Equal(t, len(notification), count, "read eventfd notification")
	require.NotZero(t, binary.NativeEndian.Uint64(notification[:]),
		"eventfd notification count is zero")
	completions := collect(ring.Reap())
	require.Len(t, completions, 1, "eventfd completion")
	require.Equal(t, eventHandle, completions[0].Handle, "eventfd completion")
	require.NoError(t, completions[0].Err, "eventfd completion")
	require.NoError(t, ring.UnregisterEventFD())

	file, err := os.CreateTemp(t.TempDir(), "ringo-registered-file-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	files, err := ring.RegisterFiles(file)
	require.NoError(t, err)
	slot, err := files.File(0)
	require.NoError(t, err)
	data := []byte("registered-file")
	write, err := ring.Push(Write(FixedFD(slot), data, 0))
	require.NoError(t, err)
	require.EqualValues(t, len(data), awaitCompletions(t, ring, 1)[write].Result, "registered-file write")
}

func TestRingIntegrationFixedFilesUpdate(t *testing.T) {
	ring := newIntegrationRing(t, WithDepth(2), WithFixedFiles(1))
	files := ring.FixedFiles()
	slot, err := files.File(0)
	require.NoError(t, err)

	first, err := os.CreateTemp(t.TempDir(), "ringo-fixed-update-first-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	second, err := os.CreateTemp(t.TempDir(), "ringo-fixed-update-second-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	for _, test := range []struct {
		file *os.File
		data []byte
	}{
		{file: first, data: []byte("first")},
		{file: second, data: []byte("second")},
	} {
		updated, err := files.Update(0, test.file)
		require.NoError(t, err, "update fixed file")
		require.Equal(t, 1, updated, "update fixed file")
		handle, err := ring.Push(Write(FixedFD(slot), test.data, 0))
		require.NoError(t, err)
		require.EqualValues(t, len(test.data), awaitCompletions(t, ring, 1)[handle].Result, "fixed-file write")
	}

	firstContents, err := os.ReadFile(first.Name())
	require.NoError(t, err, "first fixed file")
	require.Equal(t, "first", string(firstContents), "first fixed file")
	secondContents, err := os.ReadFile(second.Name())
	require.NoError(t, err, "second fixed file")
	require.Equal(t, "second", string(secondContents), "second fixed file")
	updated, err := files.Update(0, nil)
	require.NoError(t, err, "clear fixed file")
	require.Equal(t, 1, updated, "clear fixed file")
}

func TestRingIntegrationMmapBackedFixedBuffer(t *testing.T) {
	memory, err := syscall.Mmap(
		-1,
		0,
		4096,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANON,
	)
	require.False(t, err != nil, err)
	t.Cleanup(func() {
		require.NoError(t, syscall.Munmap(memory), "unmap registered buffer")
	})

	// Register the Ring cleanup after the mapping cleanup so it runs first.
	ring := newIntegrationRing(t, WithDepth(2))
	copy(memory, "mmap-registered")
	set, err := ring.RegisterBuffers(memory)
	require.NoError(t, err)
	buffer, err := set.Buffer(0)
	require.NoError(t, err)
	file, err := os.CreateTemp(t.TempDir(), "ringo-mmap-buffer-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	handle, err := ring.Push(WriteFixed(FileFD(file), buffer, 0))
	require.NoError(t, err)
	require.EqualValues(t, len(memory), awaitCompletions(t, ring, 1)[handle].Result, "mmap registered-buffer write")
}

func TestRingIntegrationSetupModes(t *testing.T) {
	assertNop := func(t *testing.T, ring *Ring) {
		t.Helper()
		handle, err := ring.Push(Nop())
		require.NoError(t, err)
		_, completed := awaitCompletions(t, ring, 1)[handle]
		require.True(t, completed, "setup-mode completion is missing")
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
		require.NoError(t, err)
		_, err = ring.Submit()
		require.NoError(t, err)
		_, completed := awaitCompletions(t, ring, 1)[handle]
		require.True(t, completed, "SQPOLL completion is missing")
	})
}
