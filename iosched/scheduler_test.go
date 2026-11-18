package iosched_test

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// schedulerFactory names a scheduler backend and constructs a fresh instance
// with cleanup registered on t. newSched may Skip the test when the platform
// can't provide the backend; since it runs inside a subtest, a skip drops only
// that backend, not the whole test.
type schedulerFactory struct {
	name     string
	newSched func(t *testing.T) iosched.Scheduler
}

// forEachScheduler runs fn as a subtest against every available backend, each
// with its own fresh scheduler. The backend list is platform-specific:
// availableSchedulers is defined per GOOS (POSIX everywhere, io_uring on Linux).
func forEachScheduler(t *testing.T, fn func(t *testing.T, s iosched.Scheduler)) {
	for _, f := range availableSchedulers() {
		t.Run(f.name, func(t *testing.T) { fn(t, f.newSched(t)) })
	}
}

// availableSchedulers lists POSIX and, where the kernel provides it, io_uring,
// each with a two-slot virtual-file table.
func availableSchedulers() []schedulerFactory {
	factories := []schedulerFactory{{name: "POSIX", newSched: newPOSIX}}
	if iosched.IOUringAvailable {
		factories = append(factories, schedulerFactory{name: "URing", newSched: func(t *testing.T) iosched.Scheduler {
			return newVURingSched(t, iosched.WithRingDepth(16), iosched.WithVFiles(2))
		}})
	}
	return factories
}

func newPOSIX(t *testing.T) iosched.Scheduler {
	s := iosched.NewPOSIXScheduler(iosched.WithVFiles(2))
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// runOp submits op, waits for it, and returns its count.
func runOp(t *testing.T, s iosched.Scheduler, op iosched.Op) int {
	t.Helper()
	ticket, err := s.Submit(op)
	require.NoError(t, err)
	n, err := ticket.Wait()
	require.NoError(t, err)
	return n
}

func TestVirtualOpenReplacesIdleSlot(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		dir := t.TempDir()
		firstPath := filepath.Join(dir, "first.dat")
		secondPath := filepath.Join(dir, "second.dat")
		const vfd = uint32(0)

		runOp(t, s, iosched.VOpenatOp(unix.AT_FDCWD, firstPath,
			unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd))
		runOp(t, s, iosched.VWriteOp(vfd, []byte("first"), 0))

		runOp(t, s, iosched.VOpenatOp(unix.AT_FDCWD, secondPath,
			unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd))
		runOp(t, s, iosched.VWriteOp(vfd, []byte("second"), 0))
		runOp(t, s, iosched.VCloseOp(vfd))

		first, err := os.ReadFile(firstPath)
		require.NoError(t, err)
		require.Equal(t, []byte("first"), first)
		second, err := os.ReadFile(secondPath)
		require.NoError(t, err)
		require.Equal(t, []byte("second"), second)
	})
}

func TestDurableWrite(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		f := newEmptyFile(t)
		want := []byte("durable payload")

		beforeSubmit := time.Now()
		ticket, err := s.Submit(iosched.WriteOp(f, want, 0).Durable())
		require.NoError(t, err)
		n, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, len(want), n)
		require.Positive(t, ticket.Slat)
		require.Positive(t, ticket.Clat)
		require.LessOrEqual(t, ticket.Slat+ticket.Clat, time.Since(beforeSubmit))

		got := make([]byte, len(want))
		read, err := s.Submit(iosched.ReadOp(f, got, 0))
		require.NoError(t, err)
		_, err = read.Wait()
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}

func TestDrainWaitsAndLeavesRegularFileOpen(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		f, err := os.CreateTemp(t.TempDir(), "drain-*.dat")
		require.NoError(t, err)

		const writes = 16
		payload := make([]byte, 4096)
		tickets := make([]iosched.Ticket, writes)
		for i := range tickets {
			tickets[i], err = s.Submit(iosched.WriteOp(f, payload, int64(i*len(payload))).Durable())
			require.NoError(t, err)
		}
		drain, err := s.Submit(iosched.DrainOp(f))
		require.NoError(t, err)
		_, err = drain.Wait()
		require.NoError(t, err)

		for _, ticket := range tickets {
			n, err := ticket.Wait()
			require.NoError(t, err)
			require.Equal(t, len(payload), n)
		}
		_, err = f.Stat()
		require.NoError(t, err, "DrainOp closed the caller-owned os.File")
		require.NoError(t, f.Close())
	})
}

// Close completes every accepted ticket: each finishes or reports the close.
// The schedulers are built here because Close runs once, inside the test.
func TestSchedulerCloseCompletesPending(t *testing.T) {
	schedulers := map[string]func() (iosched.Scheduler, error){
		"POSIX": func() (iosched.Scheduler, error) { return iosched.NewPOSIXScheduler(), nil },
	}
	if iosched.IOUringAvailable {
		schedulers["URing"] = func() (iosched.Scheduler, error) { return iosched.NewURingScheduler(iosched.WithRingDepth(16)) }
	}
	for name, newScheduler := range schedulers {
		t.Run(name, func(t *testing.T) {
			s, err := newScheduler()
			require.NoError(t, err)
			testSchedulerCloseCompletesPending(t, s)
		})
	}
}

func testSchedulerCloseCompletesPending(t *testing.T, s iosched.Scheduler) {
	f := newEmptyFile(t)
	const n = 64
	buf := make([]byte, 512)
	tickets := make([]iosched.Ticket, n)
	for i := range n {
		tk, err := s.Submit(iosched.WriteOp(f, buf, int64(i*len(buf))))
		require.NoError(t, err)
		tickets[i] = tk
	}

	require.NoError(t, s.Close()) // close without waiting for the burst

	done := make(chan struct{})
	go func() {
		for _, tk := range tickets {
			tk.Wait()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("tickets did not all finish after Close")
	}

	for _, tk := range tickets {
		if _, err := tk.Wait(); err != nil {
			require.ErrorContains(t, err, "closed") // completed, or the close error
		}
	}
}

func TestTicketResults(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		f := newEmptyFile(t)
		got := make([]byte, 3)
		ticket, err := s.Submit(iosched.WriteOp(f, []byte("value"), 0).Link(
			iosched.ReadOp(f, got, 1), iosched.FdatasyncOp(f)))
		require.NoError(t, err)
		n, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, "alu", string(got))
		require.Equal(t, []iosched.Result{{N: 5}, {N: 3}, {}}, ticket.AllResults())

		readonly, err := os.Open(f.Name())
		require.NoError(t, err)
		defer readonly.Close()
		for _, hard := range []bool{false, true} {
			root := iosched.WriteOp(readonly, []byte("fails"), 0)
			tail := iosched.WriteOp(f, []byte("ok"), 0).Durable()
			if hard {
				root = root.HardLink(tail)
			} else {
				root = root.Link(tail)
			}
			ticket, err := s.Submit(root)
			require.NoError(t, err)
			_, err = ticket.Wait()
			require.ErrorIs(t, err, syscall.EBADF)
			results := ticket.AllResults()
			require.Len(t, results, 2)
			require.ErrorIs(t, results[0].Err, syscall.EBADF)
			if hard {
				require.Equal(t, iosched.Result{N: 2}, results[1])
			} else {
				require.ErrorIs(t, results[1].Err, syscall.ECANCELED)
			}
		}
	})
}

func TestTicketResultsRemainIndependent(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		f := newEmptyFile(t)
		op := iosched.WriteOp(f, []byte("same"), 0).Link(iosched.FsyncOp(f))
		tickets := make([]iosched.Ticket, 16)
		for i := range tickets {
			var err error
			tickets[i], err = s.Submit(op)
			require.NoError(t, err)
		}
		for _, ticket := range tickets {
			_, err := ticket.Wait()
			require.NoError(t, err)
			require.Equal(t, []iosched.Result{{N: 4}, {}}, ticket.AllResults())
		}
	})
}

func TestTicketAllResults(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		f := newEmptyFile(t)
		for _, linked := range []bool{false, true} {
			op := iosched.WriteOp(f, []byte("value"), 0)
			want := []iosched.Result{{N: 5}}
			if linked {
				op = op.Link(iosched.ReadOp(f, make([]byte, 3), 1))
				want = append(want, iosched.Result{N: 3})
			}
			ticket, err := s.Submit(op)
			require.NoError(t, err)
			n, err := ticket.Wait()
			require.NoError(t, err)
			require.Equal(t, 5, n)
			require.Equal(t, want, ticket.AllResults())
		}
	})
}

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

func TestVectoredWritevRegularRead(t *testing.T) { forEachScheduler(t, testWritevRegularRead) }

func TestVectoredRegularWriteReadv(t *testing.T) { forEachScheduler(t, testRegularWriteReadv) }

func TestVectoredShortRead(t *testing.T) { forEachScheduler(t, testVectoredShortRead) }

func TestVectoredOffsetAndEmpty(t *testing.T) { forEachScheduler(t, testVectoredOffsetAndEmpty) }

func TestVectoredVirtual(t *testing.T) { forEachScheduler(t, testVectoredVirtual) }

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
// (io_uring FIXED_FILE path), reading
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

func TestInitialVirtualFiles(t *testing.T) {
	for name, makeScheduler := range map[string]func(...iosched.Option) iosched.Scheduler{
		"posix":   func(opts ...iosched.Option) iosched.Scheduler { return iosched.NewPOSIXScheduler(opts...) },
		"default": func(opts ...iosched.Option) iosched.Scheduler { return iosched.NewDefaultScheduler(opts...) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newEmptyFile(t)
			s := makeScheduler(iosched.WithVFiles(2), iosched.WithFiles(f))
			t.Cleanup(func() {
				if s != nil {
					require.NoError(t, s.Close())
				}
			})
			runOp(t, s, iosched.VWriteOp(0, []byte("data"), 0))
			path := filepath.Join(t.TempDir(), "object")
			ticket, err := s.Submit(iosched.VOpenatOp(unix.AT_FDCWD, path, os.O_CREATE|os.O_RDWR, 0600, iosched.AllocateVFD))
			require.NoError(t, err)
			vfd, err := ticket.Wait()
			require.NoError(t, err)
			require.Equal(t, 1, vfd, "initial slots must not be dynamically allocated")
			ticket, err = s.Submit(iosched.VOpenatOp(unix.AT_FDCWD, path, os.O_RDWR, 0600, iosched.AllocateVFD))
			require.NoError(t, err)
			_, err = ticket.Wait()
			require.ErrorIs(t, err, unix.ENFILE)
			runOp(t, s, iosched.VCloseOp(uint32(vfd)))
			// Borrowed files survive scheduler shutdown.
			require.NoError(t, s.Close())
			s = nil
			_, err = f.WriteAt([]byte("live"), 0)
			require.NoError(t, err)
		})
	}
}

func TestWriteBufferLengthAndOwnership(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, scheduler iosched.Scheduler) {
		allocator := alloc.NewAllocator(4 << 20)
		defer allocator.Close()
		buffer := alloc.NewAllocatingBuffer(allocator)
		defer buffer.Release()
		_, err := buffer.Write(bytes.Repeat([]byte("a"), 4096))
		require.NoError(t, err)
		gap, err := allocator.TryAcquire(4096)
		require.NoError(t, err)
		defer gap.Unpin()
		_, err = buffer.Write(bytes.Repeat([]byte("b"), 256<<10))
		require.NoError(t, err)
		const length = 4096 + 37
		const offset = 13
		f := newEmptyFile(t)
		before := allocator.Available()
		op := iosched.WriteBufferOp(f, buffer, length, offset)
		require.Equal(t, length, runOp(t, scheduler, op))
		require.Equal(t, before, allocator.Available(), "submission only borrows the buffer")
		contents, err := os.ReadFile(f.Name())
		require.NoError(t, err)
		require.Len(t, contents, offset+length)
		require.Equal(t, bytesConcat(bytes.Repeat([]byte("a"), 4096), bytes.Repeat([]byte("b"), 37)), contents[offset:])
		var constructed iosched.Op
		require.Zero(t, testing.AllocsPerRun(100, func() { constructed = iosched.WriteBufferOp(f, buffer, length, offset) }), "buffer operation construction must not allocate a vector slice")
		_ = constructed
	})
}

func TestReadBufferLengthAndOwnership(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		a := alloc.NewAllocator(4 << 20)
		defer a.Close()
		var held []alloc.Memory
		for {
			m, err := a.TryAcquire(4096)
			if err != nil {
				require.ErrorIs(t, err, alloc.ErrExhausted)
				break
			}
			held = append(held, m)
		}
		for i := 0; i < len(held); i += 2 {
			held[i].Unpin()
		}
		defer func() {
			for i := 1; i < len(held); i += 2 {
				held[i].Unpin()
			}
		}()
		b := alloc.NewAllocatingBuffer(a)
		defer b.Release()
		_, err := b.Write(bytes.Repeat([]byte("z"), 260<<10))
		require.NoError(t, err)
		require.Greater(t, len(b.RawAllocations()), 1)

		const length = 4096 + 37
		const offset = 13
		want := bytes.Repeat([]byte("v"), length)
		f := newFileWith(t, append(make([]byte, offset), want...))
		before := a.Available()
		beforeLen := b.Len()
		require.Equal(t, length, runOp(t, s, iosched.ReadBufferOp(f, b, length, offset)))
		require.Equal(t, before, a.Available(), "I/O borrows existing allocations")
		require.Equal(t, beforeLen, b.Len(), "I/O does not change stream cursors")
		var got []byte
		for _, m := range b.RawAllocations() {
			got = append(got, m.Bytes()...)
		}
		require.Equal(t, want, got[:length])
		require.Equal(t, bytes.Repeat([]byte("z"), beforeLen-length), got[length:beforeLen], "bytes past the requested length are untouched")
		var constructed iosched.Op
		require.Zero(t, testing.AllocsPerRun(100, func() { constructed = iosched.ReadBufferOp(f, b, length, offset) }))
		_ = constructed
	})
}

func TestVirtualReadBuffer(t *testing.T) {
	for name, makeScheduler := range map[string]func(...iosched.Option) iosched.Scheduler{
		"posix":   func(opts ...iosched.Option) iosched.Scheduler { return iosched.NewPOSIXScheduler(opts...) },
		"default": func(opts ...iosched.Option) iosched.Scheduler { return iosched.NewDefaultScheduler(opts...) },
	} {
		t.Run(name, func(t *testing.T) {
			s := makeScheduler(iosched.WithVFiles(1))
			defer s.Close()
			a := alloc.NewAllocator(1 << 20)
			defer a.Close()
			b := alloc.NewAllocatingBuffer(a)
			defer b.Release()
			require.NoError(t, b.Grow(8192))
			want := bytes.Repeat([]byte("v"), 7000)
			f := newFileWith(t, want)
			op := iosched.VOpenatOp(unix.AT_FDCWD, f.Name(), os.O_RDONLY, 0, iosched.AllocateVFD).Link(
				iosched.VReadBufferOp(iosched.AllocateVFD, b, len(want), 0).HardLink(iosched.VCloseOp(iosched.AllocateVFD)))
			ticket, err := s.Submit(op)
			require.NoError(t, err)
			_, err = ticket.Wait()
			require.NoError(t, err)
			require.Equal(t, len(want), ticket.AllResults()[1].N)
			require.Equal(t, want, b.RawAllocations()[0].Bytes()[:len(want)])
		})
	}
}

func TestUnlink(t *testing.T) {
	forEachScheduler(t, testUnlink)
}

func testUnlink(t *testing.T, s iosched.Scheduler) {
	dir := t.TempDir()
	d, err := os.Open(dir)
	require.NoError(t, err)
	defer d.Close()
	path := filepath.Join(dir, "victim")
	require.NoError(t, os.WriteFile(path, []byte("value"), 0600))
	_, err = submitAndWait(t, s, iosched.UnlinkatOp(int(d.Fd()), "victim"))
	require.NoError(t, err)
	require.NoFileExists(t, path)

	require.NoError(t, os.WriteFile(path, []byte("next"), 0600))
	_, err = submitAndWait(t, s, iosched.UnlinkatOp(int(d.Fd()), "missing").HardLink(iosched.UnlinkatOp(int(d.Fd()), "victim")))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoFileExists(t, path) // a failed hard-linked unlink does not cancel its peer
}
