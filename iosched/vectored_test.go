package iosched_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/iosched"
)

func newEmptyFile(t *testing.T) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vec.dat")
	require.NoError(t, os.WriteFile(path, nil, 0644))
	return openRWFile(t, path)
}

func newFileWith(t *testing.T, data []byte) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vec.dat")
	require.NoError(t, os.WriteFile(path, data, 0644))
	return openRWFile(t, path)
}

func bytesConcat(bs ...[]byte) []byte {
	var out []byte
	for _, b := range bs {
		out = append(out, b...)
	}
	return out
}

// Each Test runs against every available scheduler (POSIX everywhere, io_uring
// on Linux) via forEachScheduler; the test logic is identical across backends.

func TestVectoredWritevRegularRead(t *testing.T) { forEachScheduler(t, testWritevRegularRead) }
func TestVectoredRegularWriteReadv(t *testing.T) { forEachScheduler(t, testRegularWriteReadv) }
func TestVectoredShortRead(t *testing.T)         { forEachScheduler(t, testVectoredShortRead) }
func TestVectoredOffsetAndEmpty(t *testing.T)    { forEachScheduler(t, testVectoredOffsetAndEmpty) }
func TestVectoredVirtual(t *testing.T)           { forEachScheduler(t, testVectoredVirtual) }

// testWritevRegularRead writes three unequal chunks with one writev, then reads
// them back with regular reads — one contiguous read over the whole region and
// one read per chunk. The point is not to test the kernel's writev but that our
// iovec/offset use lands bytes at the right absolute file offsets.
func testWritevRegularRead(t *testing.T, s iosched.Scheduler) {
	f := newEmptyFile(t)
	a, b, c := []byte("aaaa"), []byte("bbbbbb"), []byte("cc")
	want := bytesConcat(a, b, c)
	const base = int64(8)

	require.Equal(t, len(want), runOp(t, s, iosched.WritevOp(f, [][]byte{a, b, c}, base)))

	// One contiguous regular read over the whole region.
	whole := make([]byte, len(want))
	require.Equal(t, len(want), runOp(t, s, iosched.ReadOp(f, whole, base)))
	require.Equal(t, want, whole)

	// One regular read per chunk, at its computed offset.
	off := base
	for _, chunk := range [][]byte{a, b, c} {
		got := make([]byte, len(chunk))
		require.Equal(t, len(chunk), runOp(t, s, iosched.ReadOp(f, got, off)))
		require.Equal(t, chunk, got)
		off += int64(len(chunk))
	}
}

// testRegularWriteReadv writes three chunks with separate regular writes, then
// reads them back with readv using different chunk counts (2, then 4) than the
// writes — verifying readv reassembles the same contiguous bytes regardless of
// how the destination buffers are split.
func testRegularWriteReadv(t *testing.T, s iosched.Scheduler) {
	f := newEmptyFile(t)
	a, b, c := []byte("aaaa"), []byte("bbbbbb"), []byte("cc")
	want := bytesConcat(a, b, c)
	const base = int64(4)

	off := base
	for _, chunk := range [][]byte{a, b, c} {
		require.Equal(t, len(chunk), runOp(t, s, iosched.WriteOp(f, chunk, off)))
		off += int64(len(chunk))
	}

	// readv into 2 buffers (5+7).
	p1, p2 := make([]byte, 5), make([]byte, 7)
	require.Equal(t, len(want), runOp(t, s, iosched.ReadvOp(f, [][]byte{p1, p2}, base)))
	require.Equal(t, want, bytesConcat(p1, p2))

	// readv into 4 equal buffers (3+3+3+3).
	q := [][]byte{make([]byte, 3), make([]byte, 3), make([]byte, 3), make([]byte, 3)}
	require.Equal(t, len(want), runOp(t, s, iosched.ReadvOp(f, q, base)))
	require.Equal(t, want, bytesConcat(q...))
}

func testVectoredShortRead(t *testing.T, s iosched.Scheduler) {
	f := newFileWith(t, []byte("0123456789"))  // 10 bytes
	p1, p2 := make([]byte, 6), make([]byte, 8) // request 14, only 10 available
	require.Equal(t, 10, runOp(t, s, iosched.ReadvOp(f, [][]byte{p1, p2}, 0)))
	require.Equal(t, []byte("012345"), p1)
	require.Equal(t, []byte("6789"), p2[:4])
}

func testVectoredOffsetAndEmpty(t *testing.T, s iosched.Scheduler) {
	f := newFileWith(t, make([]byte, 16))
	head, empty, tail := []byte("XY"), []byte(nil), []byte("Z")
	require.Equal(t, 3, runOp(t, s, iosched.WritevOp(f, [][]byte{head, empty, tail}, 4)))

	got := make([]byte, 3)
	require.Equal(t, 3, runOp(t, s, iosched.ReadOp(f, got, 4)))
	require.Equal(t, []byte("XYZ"), got)
}

// testVectoredVirtual exercises writev/readv against a virtual descriptor
// (io_uring FIXED_FILE path; POSIX emulates via its descriptor table), reading
// back with a different chunk count than the write.
func testVectoredVirtual(t *testing.T, s iosched.Scheduler) {
	const vfd = uint32(0)
	path := filepath.Join(t.TempDir(), "vvec.dat")
	runOp(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, vfd))

	a, b := []byte("hello "), []byte("world")
	require.Equal(t, len(a)+len(b), runOp(t, s, iosched.VWritevOp(vfd, [][]byte{a, b}, 0)))

	p := [][]byte{make([]byte, 4), make([]byte, 4), make([]byte, 3)} // 11 bytes, 3 chunks
	require.Equal(t, 11, runOp(t, s, iosched.VReadvOp(vfd, p, 0)))
	require.Equal(t, []byte("hello world"), bytesConcat(p...))

	runOp(t, s, iosched.VCloseOp(vfd))
}
