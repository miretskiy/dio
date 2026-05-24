//go:build linux

package iosched_test

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/iosched"
	"github.com/miretskiy/dio/mempool"
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

func newURingIO(t *testing.T) *iosched.BlockingIO {
	t.Helper()
	return iosched.NewBlockingIO(newURingSched(t))
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
	return ticket.Op.Result
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
		res := ticket.Op.Result
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
	require.Equal(t, 4096, ticket.Op.Result.N)
}

func TestURing_BlockingIO_ReadAt(t *testing.T) {
	bio := newURingIO(t)
	path, data := writeUringFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	n, err := bio.ReadAt(f, buf, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, n)
	require.Equal(t, data, buf)
}

func TestURing_BlockingIO_WriteAt(t *testing.T) {
	bio := newURingIO(t)
	path := filepath.Join(t.TempDir(), "write.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)
	n, err := bio.WriteAt(f, payload, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, n)

	got := make([]byte, 4096)
	_, err = bio.ReadAt(f, got, 0)
	require.NoError(t, err)
	require.Equal(t, payload, got)
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
		wg.Go(func() {
			buf := make([]byte, readSize)
			for i := range readsPerGoroutine {
				offset := int64((i * readSize) % (fileSize - readSize))
				ticket, err := s.Submit(iosched.ReadOp(f, buf, offset))
				if err != nil {
					errs <- fmt.Errorf("submit at %d: %w", offset, err)
					return
				}
				ticket.Wait()
				res := ticket.Op.Result
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
		})
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
	require.NoError(t, ticket.Op.Result.Err)
	require.Equal(t, data, buf)
}

func newRegisteredPool(t *testing.T, s *iosched.URingScheduler) *mempool.SlabPool {
	t.Helper()
	pool, err := mempool.NewSlabPool(align.HugepageSize, align.BlockSize)
	require.NoError(t, err)

	require.NoError(t, iosched.RegisterDMASlab(s, pool))
	t.Cleanup(func() { pool.Close() })
	return pool
}

func TestURing_RegisterDMASlab(t *testing.T) {
	s := newURingSched(t)
	pool := newRegisteredPool(t, s)
	require.Equal(t, align.BlockSize, pool.SlotSize())
	require.Greater(t, pool.NumSlots(), 0)
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

	res := submitResult(t, s, iosched.WriteFixedOp(f, wslot, 0))
	require.NoError(t, res.Err)
	require.Equal(t, align.BlockSize, res.N)
	wslot.Release()

	rslot, err := pool.Acquire()
	require.NoError(t, err)

	res = submitResult(t, s, iosched.ReadFixedOp(f, rslot, 0))
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

		ticket, err := s.Submit(iosched.WriteFixedOp(f, slots[i], int64(i*align.BlockSize)))
		require.NoError(t, err)
		tickets[i] = ticket
	}

	for i, ticket := range tickets {
		ticket.Wait()
		require.NoError(t, ticket.Op.Result.Err, "chunk %d write failed", i)
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

	res := submitResult(t, s, iosched.WriteFixedOp(f, slot, 0))
	require.Error(t, res.Err)
	t.Logf("kernel error for unregistered fixed op: %v", res.Err)
}

func TestNewDefaultIO_UsesURing(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}
	bio := iosched.NewDefaultIO()
	defer bio.Close()
	require.NotNil(t, bio.S)
	_, ok := bio.S.(*iosched.URingScheduler)
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
	require.NoError(t, ticket.Op.Result.Err)
	require.Equal(t, data, buf)
	ticket.Release()
}
