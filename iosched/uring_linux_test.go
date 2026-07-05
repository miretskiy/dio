//go:build linux

package iosched_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/iosched"
	"github.com/miretskiy/dio/mempool"
	"github.com/miretskiy/dio/sys"
)

func newURingSched(t *testing.T, cfg ...iosched.URingConfig) *iosched.URingScheduler {
	t.Helper()
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	c := iosched.URingConfig{RingDepth: 64}
	if len(cfg) > 0 {
		c = cfg[0]
	}
	s, err := iosched.NewURingScheduler(c)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func newVURingSched(t *testing.T, cfg iosched.URingConfig) *iosched.URingScheduler {
	t.Helper()
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	s, err := iosched.NewURingScheduler(cfg)
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

func submitOne(t *testing.T, s *iosched.URingScheduler, op iosched.Op) *iosched.Ticket {
	t.Helper()
	ticket, err := s.Submit(op)
	require.NoError(t, err)
	ticket.Wait()
	return ticket
}

func submitResult(t *testing.T, s *iosched.URingScheduler, op iosched.Op) iosched.Result {
	t.Helper()
	ticket := submitOne(t, s, op)
	defer ticket.Release()
	return ticket.Result()
}

func linkAll(ops ...iosched.Op) iosched.Op {
	if len(ops) == 0 {
		panic("empty linked op chain")
	}
	root := ops[len(ops)-1]
	for i := len(ops) - 2; i >= 0; i-- {
		root = ops[i].Link(root)
	}
	return root
}

func TestURing_Submit_Read(t *testing.T) {
	s := newURingSched(t)
	path, data := writeUringFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	res := submitResult(t, s, iosched.ReadOp(f, buf, 0))
	require.NoError(t, res.Err)
	require.Equal(t, 4096, res.N)
	require.Equal(t, data, buf)
}

func TestURing_Submit_Write(t *testing.T) {
	s := newURingSched(t)
	path := filepath.Join(t.TempDir(), "write.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)

	res := submitResult(t, s, iosched.WriteOp(f, payload, 0))
	require.NoError(t, res.Err)
	require.Equal(t, 4096, res.N)

	got := make([]byte, 4096)
	res = submitResult(t, s, iosched.ReadOp(f, got, 0))
	require.NoError(t, res.Err)
	require.Equal(t, payload, got)
}

func TestURing_GroupCommit(t *testing.T) {
	const chunks = 16
	s := newURingSched(t, iosched.URingConfig{RingDepth: 8})

	path := filepath.Join(t.TempDir(), "gc.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, chunks*4096), 0644))
	f := openRW(t, path)

	payloads := make([][]byte, chunks)
	tickets := make([]*iosched.Ticket, chunks)
	for i := range chunks {
		payloads[i] = make([]byte, 4096)
		_, _ = rand.Read(payloads[i])
		ticket, err := s.Submit(iosched.WriteOp(f, payloads[i], int64(i*4096)))
		require.NoError(t, err)
		tickets[i] = ticket
	}

	for i, ticket := range tickets {
		ticket.Wait()
		res := ticket.Result()
		require.NoError(t, res.Err, "chunk %d", i)
		require.Equal(t, 4096, res.N, "chunk %d", i)
		ticket.Release()
	}

	for i := range chunks {
		buf := make([]byte, 4096)
		res := submitResult(t, s, iosched.ReadOp(f, buf, int64(i*4096)))
		require.NoError(t, res.Err)
		require.Equal(t, payloads[i], buf, "mismatch at chunk %d", i)
	}
}

func TestURing_LinkedOps_WriteFdatasync(t *testing.T) {
	s := newURingSched(t)
	path := filepath.Join(t.TempDir(), "linked.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)

	ticket := submitOne(t, s, iosched.WriteOp(f, payload, 0).Link(iosched.FdatasyncOp(f)))
	defer ticket.Release()
	require.NoError(t, ticket.Error())
	require.Equal(t, 4096, ticket.Result().N)
}

func TestURing_Concurrent(t *testing.T) {
	const (
		fileSize          = 1 << 20
		goroutines        = 64
		readsPerGoroutine = 50
		readSize          = 4096
	)
	s := newURingSched(t)
	path, data := writeUringFile(t, fileSize)
	f := openRW(t, path)

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for worker := range goroutines {
		_ = worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, readSize)
			for i := range readsPerGoroutine {
				offset := int64((i * readSize) % (fileSize - readSize))
				ticket, err := s.Submit(iosched.ReadOp(f, buf, offset))
				if err != nil {
					errs <- fmt.Errorf("submit at %d: %w", offset, err)
					return
				}
				ticket.Wait()
				res := ticket.Result()
				ticket.Release()
				if res.Err != nil {
					errs <- fmt.Errorf("read at %d: %w", offset, res.Err)
					return
				}
				if res.N != readSize {
					errs <- fmt.Errorf("short read at %d: %d", offset, res.N)
					return
				}
				for j := range buf {
					if buf[j] != data[offset+int64(j)] {
						errs <- fmt.Errorf("mismatch at %d+%d", offset, j)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestURing_LinkedChainTooLargeFailsTicket(t *testing.T) {
	const ringDepth = 32
	s := newURingSched(t, iosched.URingConfig{RingDepth: ringDepth})
	path, _ := writeUringFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	ops := make([]iosched.Op, ringDepth+1)
	for i := range ops {
		ops[i] = iosched.ReadOp(f, buf, 0)
	}
	ticket, err := s.Submit(linkAll(ops...))
	require.NoError(t, err)
	ticket.Wait()
	defer ticket.Release()
	require.ErrorContains(t, ticket.Error(), "exceeds ring depth")
}

func TestURing_CloseThenSubmit(t *testing.T) {
	s, err := iosched.NewURingScheduler(iosched.URingConfig{})
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
	s, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: 64, SQPOLL: true})
	if err != nil {
		t.Skipf("SQPOLL not available (may require CAP_SYS_NICE): %v", err)
	}
	defer s.Close()

	path, data := writeUringFile(t, 4096)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	buf := make([]byte, 4096)
	ticket := submitOne(t, s, iosched.ReadOp(f, buf, 0))
	defer ticket.Release()
	require.NoError(t, ticket.Result().Err)
	require.Equal(t, data, buf)
}

func TestURing_ReusedSQEWriteAfterFdatasync(t *testing.T) {
	s := newURingSched(t)

	path := filepath.Join(t.TempDir(), "direct.dat")
	f, err := sys.CreateDirect(path, sys.FlDirectIO)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	payload := align.AllocAligned(align.BlockSize)
	t.Cleanup(func() { align.FreeAligned(payload) })
	for i := range payload {
		payload[i] = byte(i)
	}

	res := submitResult(t, s, iosched.WriteOp(f, payload, 0))
	require.NoError(t, res.Err)
	require.Equal(t, len(payload), res.N)

	res = submitResult(t, s, iosched.FdatasyncOp(f))
	require.NoError(t, res.Err)

	res = submitResult(t, s, iosched.WriteOp(f, payload, int64(len(payload))))
	require.NoError(t, res.Err)
	require.Equal(t, len(payload), res.N)
}

func TestURing_ConcurrentDirectWriteAndFdatasync(t *testing.T) {
	const (
		workers = 16
		rounds  = 64
	)
	s := newURingSched(t, iosched.URingConfig{RingDepth: 32})

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
				ticket.Wait()

				r0 := ticket.Result()
				if r0.Err != nil {
					ticket.Release()
					errs <- fmt.Errorf("worker %d round %d write: n=%d err=%w", worker, round, r0.N, r0.Err)
					return
				}
				if r0.N != len(payload) {
					ticket.Release()
					errs <- fmt.Errorf("worker %d round %d write count: got %d want %d", worker, round, r0.N, len(payload))
					return
				}
				if err := ticket.Error(); err != nil {
					ticket.Release()
					errs <- fmt.Errorf("worker %d round %d linked chain: %w", worker, round, err)
					return
				}
				ticket.Release()
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
	s := newVURingSched(t, iosched.URingConfig{RingDepth: 16, VFiles: 4})
	vfd := uint32(0)

	path := filepath.Join(t.TempDir(), "vopen.dat")
	payload := []byte("virtual file payload")

	openTicket := submitOne(t, s, iosched.VOpenatOp(
		unix.AT_FDCWD,
		path,
		unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC,
		0o600,
		vfd,
	))
	defer openTicket.Release()
	require.NoError(t, openTicket.Error())

	// The slot is live only once the open has completed; the caller orders the
	// write after it (here by waiting the open), since the scheduler does not.
	writeTicket := submitOne(t, s, iosched.VWriteOp(vfd, payload, 0))
	defer writeTicket.Release()
	require.NoError(t, writeTicket.Error())
	require.Equal(t, len(payload), writeTicket.Result().N)

	readBuf := make([]byte, len(payload))
	readTicket := submitOne(t, s, iosched.VReadOp(vfd, readBuf, 0))
	defer readTicket.Release()
	require.NoError(t, readTicket.Error())
	require.Equal(t, payload, readBuf)

	closeTicket := submitOne(t, s, iosched.VCloseOp(vfd))
	defer closeTicket.Release()
	require.NoError(t, closeTicket.Error())

	closedSlotTicket, err := s.Submit(iosched.VWriteOp(vfd, payload, 0))
	require.NoError(t, err)
	closedSlotTicket.Wait()
	defer closedSlotTicket.Release()
	require.Error(t, closedSlotTicket.Error())
}

func newRegisteredPool(t *testing.T, s *iosched.URingScheduler) *mempool.SlabPool {
	t.Helper()
	pool, err := mempool.NewSlabPool(align.HugepageSize, align.BlockSize)
	require.NoError(t, err)

	require.NoError(t, iosched.RegisterDMASlab(s, pool))
	t.Cleanup(func() { pool.Close() })
	return pool
}

func TestURing_FixedOp_WriteRead(t *testing.T) {
	s := newURingSched(t)
	pool := newRegisteredPool(t, s)

	path := filepath.Join(t.TempDir(), "fixed.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, align.BlockSize), 0644))
	f := openRW(t, path)

	payload := make([]byte, align.BlockSize)
	_, _ = rand.Read(payload)

	wslot, err := pool.Acquire()
	require.NoError(t, err)
	copy(wslot.Data, payload)

	res := submitResult(t, s, iosched.WriteFixedOp(f, wslot.Data, 0))
	require.NoError(t, res.Err)
	require.Equal(t, align.BlockSize, res.N)
	wslot.Release()

	rslot, err := pool.Acquire()
	require.NoError(t, err)

	res = submitResult(t, s, iosched.ReadFixedOp(f, rslot.Data, 0))
	require.NoError(t, res.Err)
	require.Equal(t, align.BlockSize, res.N)
	require.Equal(t, payload, rslot.Data)
	rslot.Release()
}

func TestURing_FixedOp_MultipleSlots(t *testing.T) {
	const chunks = 8
	s := newURingSched(t)
	pool := newRegisteredPool(t, s)

	path := filepath.Join(t.TempDir(), "fixed_multi.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, chunks*align.BlockSize), 0644))
	f := openRW(t, path)

	payloads := make([][]byte, chunks)
	slots := make([]mempool.Slot, chunks)
	tickets := make([]*iosched.Ticket, chunks)

	for i := range chunks {
		payloads[i] = make([]byte, align.BlockSize)
		_, _ = rand.Read(payloads[i])

		slots[i], _ = pool.Acquire()
		copy(slots[i].Data, payloads[i])

		ticket, err := s.Submit(iosched.WriteFixedOp(f, slots[i].Data, int64(i*align.BlockSize)))
		require.NoError(t, err)
		tickets[i] = ticket
	}

	for i, ticket := range tickets {
		ticket.Wait()
		require.NoError(t, ticket.Result().Err, "chunk %d write failed", i)
		slots[i].Release()
		ticket.Release()
	}

	for i := range chunks {
		buf := make([]byte, align.BlockSize)
		res := submitResult(t, s, iosched.ReadOp(f, buf, int64(i*align.BlockSize)))
		require.NoError(t, res.Err)
		require.Equal(t, payloads[i], buf, "mismatch at chunk %d", i)
	}
}

func TestURing_FixedOp_WithoutRegistration(t *testing.T) {
	s := newURingSched(t)

	pool, err := mempool.NewSlabPool(align.HugepageSize, align.BlockSize)
	require.NoError(t, err)
	defer pool.Close()

	path := filepath.Join(t.TempDir(), "noreg.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, align.BlockSize), 0644))
	f := openRW(t, path)

	slot, err := pool.Acquire()
	require.NoError(t, err)
	defer slot.Release()

	res := submitResult(t, s, iosched.WriteFixedOp(f, slot.Data, 0))
	require.Error(t, res.Err)
	t.Logf("kernel error for unregistered fixed op: %v", res.Err)
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

func TestURing_Ticket_DeferReleaseAndDoubleRelease(t *testing.T) {
	s := newURingSched(t)
	path, data := writeUringFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	ticket, err := s.Submit(iosched.ReadOp(f, buf, 0))
	require.NoError(t, err)
	defer ticket.Release()

	ticket.Wait()
	require.NoError(t, ticket.Result().Err)
	require.Equal(t, data, buf)
	ticket.Release()
}
