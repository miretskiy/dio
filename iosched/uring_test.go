package iosched_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/sys"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newURingSched(t *testing.T, opts ...iosched.Option) *iosched.URingScheduler {
	t.Helper()
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	if len(opts) == 0 {
		opts = []iosched.Option{iosched.WithRingDepth(64)}
	}
	s, err := iosched.NewURingScheduler(append([]iosched.Option{
		iosched.WithDiskPolicy(iosched.NewDiskPolicy(iosched.WithoutIOBudget()))}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func newVURingSched(t *testing.T, opts ...iosched.Option) *iosched.URingScheduler {
	t.Helper()
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	s, err := iosched.NewURingScheduler(append([]iosched.Option{
		iosched.WithDiskPolicy(iosched.NewDiskPolicy(iosched.WithoutIOBudget()))}, opts...)...)
	if err != nil {
		switch {
		case errors.Is(err, unix.EINVAL), errors.Is(err, unix.EOPNOTSUPP), errors.Is(err, unix.ENOSYS):
			t.Skipf("io_uring virtual files not available on this kernel: %v", err)
		default:
			require.NoError(t, err)
		}
	}
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func writeUringFile(t *testing.T, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "uring.dat")
	require.NoError(t, os.WriteFile(path, data, 0644))
	return path, data
}

func openRW(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	return f
}

func submitOne(t *testing.T, s *iosched.URingScheduler, op iosched.Op) (int, error) {
	t.Helper()
	ticket, err := s.Submit(op)
	require.NoError(t, err)
	return ticket.Wait()
}

func TestURing_CloseThenSubmit(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	s, err := iosched.NewURingScheduler()
	require.NoError(t, err)
	require.NoError(t, s.Close())

	path := filepath.Join(t.TempDir(), "dummy.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	buf := make([]byte, 4096)
	_, err = s.Submit(iosched.ReadOp(f, buf, 0))
	require.Error(t, err)
}

func TestURing_SQPOLL(t *testing.T) {
	s, err := iosched.NewURingScheduler(iosched.WithRingDepth(64), iosched.WithSQPOLL())
	if err != nil {
		t.Skipf("SQPOLL not available (may require CAP_SYS_NICE): %v", err)
	}
	defer s.Close()

	path, data := writeUringFile(t, 4096)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	buf := make([]byte, 4096)
	_, err = submitOne(t, s, iosched.ReadOp(f, buf, 0))
	require.NoError(t, err)
	require.Equal(t, data, buf)
}

func TestURing_ConcurrentDirectWriteAndFdatasync(t *testing.T) {
	const (
		workers = 16
		rounds  = 64
	)
	s := newURingSched(t, iosched.WithRingDepth(32))

	path := filepath.Join(t.TempDir(), "parallel-direct.dat")
	f, err := sys.CreateDirect(path, sys.FlDirectIO)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	payloads := make([][]byte, workers)
	for worker := range workers {
		payload := align.AllocAligned(align.BlockSize)
		for i := range payload {
			payload[i] = byte(worker + i)
		}
		payloads[worker] = payload
		t.Cleanup(func() { align.FreeAligned(payload) })
	}

	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			payload := payloads[worker]
			for round := range rounds {
				offset := int64((worker*rounds + round) * align.BlockSize)
				ticket, err := s.Submit(iosched.WriteOp(f, payload, offset).Link(iosched.FdatasyncOp(f)))
				if err != nil {
					errs <- fmt.Errorf("worker %d round %d submit: %w", worker, round, err)
					return
				}
				n, err := ticket.Wait()

				if err != nil {
					errs <- fmt.Errorf("worker %d round %d write: n=%d err=%w", worker, round, n, err)
					return
				}
				if n != len(payload) {
					errs <- fmt.Errorf("worker %d round %d write count: got %d want %d", worker, round, n, len(payload))
					return
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

func TestURing_VOpenUnlinkedWriteReadClose(t *testing.T) {
	s := newVURingSched(t, iosched.WithRingDepth(16), iosched.WithVFiles(4))
	vfd := uint32(0)

	path := filepath.Join(t.TempDir(), "vopen.dat")
	payload := []byte("virtual file payload")

	_, err := submitOne(t, s, iosched.VOpenatOp(
		unix.AT_FDCWD,
		path,
		unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC,
		0o600,
		vfd,
	))
	require.NoError(t, err)

	// This basic path waits each result before the next operation.
	n, err := submitOne(t, s, iosched.VWriteOp(vfd, payload, 0))
	require.NoError(t, err)
	require.Equal(t, len(payload), n)

	readBuf := make([]byte, len(payload))
	_, err = submitOne(t, s, iosched.VReadOp(vfd, readBuf, 0))
	require.NoError(t, err)
	require.Equal(t, payload, readBuf)

	_, err = submitOne(t, s, iosched.VCloseOp(vfd))
	require.NoError(t, err)

	closedSlotTicket, err := s.Submit(iosched.VWriteOp(vfd, payload, 0))
	require.NoError(t, err)
	_, err = closedSlotTicket.Wait()
	require.Error(t, err)
}

func TestNewDefaultScheduler_UsesURing(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}
	s := iosched.NewDefaultScheduler()
	defer s.Close()
	_, ok := s.(*iosched.URingScheduler)
	require.True(t, ok)
}

func TestURing_ChainLengthIsLimited(t *testing.T) {
	s := newURingSched(t, iosched.WithRingDepth(64))
	path, _ := writeUringFile(t, 4096)
	f := openRW(t, path)
	buf := make([]byte, 512)
	reads := func(n int) iosched.Op {
		rest := make([]iosched.Op, n-1)
		for i := range rest {
			rest[i] = iosched.ReadOp(f, buf, 0)
		}
		return iosched.ReadOp(f, buf, 0).Link(rest...)
	}

	_, err := submitOne(t, s, reads(8))
	require.NoError(t, err)
	_, err = s.Submit(reads(9))
	require.ErrorContains(t, err, "linked chain of 9 operations")
	require.ErrorContains(t, err, "exceeds the limit of 8")
}

// TestURing_VOpenFallocateWriteChain verifies the create-and-first-write fusion
// the slab write path relies on.
func TestURing_VOpenFallocateWriteChain(t *testing.T) {
	s := newVURingSched(t, iosched.WithRingDepth(16), iosched.WithVFiles(4))
	const vfd = uint32(1)
	const size = int64(1 << 16)

	path := filepath.Join(t.TempDir(), "chain.dat")
	first := []byte("first-record")
	second := []byte("second-record")

	chain := iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, vfd).
		Link(iosched.VFallocateOp(vfd, size), iosched.VWriteOp(vfd, first, 0))
	chainTkt, err := s.Submit(chain)
	require.NoError(t, err)

	_, err = chainTkt.Wait()
	require.NoError(t, err)
	secondTkt, err := s.Submit(iosched.VWriteOp(vfd, second, int64(len(first))))
	require.NoError(t, err)
	_, err = secondTkt.Wait()
	require.NoError(t, err)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, info.Size(), size)

	got := make([]byte, len(first)+len(second))
	_, err = submitOne(t, s, iosched.VReadOp(vfd, got, 0))
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, first...), second...), got)

	_, err = submitOne(t, s, iosched.VCloseOp(vfd))
	require.NoError(t, err)
}

// TestURing_DrainWaitsForInflightWrites_Regular submits durable writes followed
// immediately by DrainOp. Drain waits through their synthesized fdatasyncs but
// leaves the os.File open and caller-owned.
func TestURing_DrainWaitsForInflightWrites_Regular(t *testing.T) {
	s := newURingSched(t)
	const n = 64
	const recLen = 64

	f, err := os.CreateTemp(t.TempDir(), "drain-*.dat")
	require.NoError(t, err)
	path := f.Name()

	rec := func(i int) []byte {
		b := make([]byte, recLen)
		for j := range b {
			b[j] = byte('a' + (i+j)%26)
		}
		return b
	}

	writes := make([]iosched.Ticket, n)
	for i := range writes {
		writes[i], err = s.Submit(iosched.WriteOp(f, rec(i), int64(i)*recLen).Durable())
		require.NoError(t, err)
	}
	drainTkt, err := s.Submit(iosched.DrainOp(f))
	require.NoError(t, err)

	for i, w := range writes {
		n, err := w.Wait()
		require.NoErrorf(t, err, "write %d", i)
		require.Equal(t, recLen, n)
	}
	_, err = drainTkt.Wait()
	require.NoError(t, err)

	// Drain does not close f. The caller can still inspect it and then performs
	// the one real close through os.File, clearing its finalizer safely.
	_, err = f.Stat()
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Read back via a fresh fd after the caller-owned close.
	rf, err := os.Open(path)
	require.NoError(t, err)
	defer rf.Close()
	for i := range writes {
		got := make([]byte, recLen)
		_, err := rf.ReadAt(got, int64(i)*recLen)
		require.NoError(t, err)
		require.Equalf(t, rec(i), got, "record %d", i)
	}
}

// TestURing_VSlot1_Recycle stresses a single virtual slot: open → write → read →
// close, repeated hundreds of times, so slot 0 is reopened every iteration.
func TestURing_VSlot1_Recycle(t *testing.T) {
	s := newVURingSched(t, iosched.WithRingDepth(16), iosched.WithVFiles(1))
	const vfd = uint32(0)
	const iters = 300
	dir := t.TempDir()

	for i := range iters {
		path := filepath.Join(dir, fmt.Sprintf("gen-%d.dat", i))
		want := fmt.Appendf(nil, "payload-for-generation-%d", i)

		_, err := submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path,
			unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600, vfd))
		require.NoErrorf(t, err, "iter %d: open", i)

		w, err := s.Submit(iosched.VWriteOp(vfd, want, 0).Durable())
		require.NoError(t, err)
		_, err = w.Wait()
		require.NoErrorf(t, err, "iter %d: write", i)
		cl, err := s.Submit(iosched.VCloseOp(vfd))
		require.NoError(t, err)
		_, err = cl.Wait()
		require.NoErrorf(t, err, "iter %d: close", i)

		// Recycle the slot: reopen and read the data back.
		_, err = submitOne(t, s, iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_RDWR, 0o600, vfd))
		require.NoErrorf(t, err, "iter %d: reopen", i)

		got := make([]byte, len(want))
		_, err = submitOne(t, s, iosched.VReadOp(vfd, got, 0))
		require.NoErrorf(t, err, "iter %d: read", i)
		require.Equalf(t, want, got, "iter %d: data", i)

		_, err = submitOne(t, s, iosched.VCloseOp(vfd))
		require.NoErrorf(t, err, "iter %d: close2", i)
	}
}
