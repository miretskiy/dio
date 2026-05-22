//go:build linux

package iosched_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	s := newURingSched(t)
	return iosched.NewBlockingIO(s)
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
	t.Cleanup(func() { f.Close() })
	return f
}

// submitWait acquires a Ticket, submits ops, waits, and returns the per-op
// results. Test helper for the common "submit one batch, wait for it"
// pattern. Releases the Ticket back to the pool before returning.
func submitWait(t *testing.T, s *iosched.URingScheduler, ops []iosched.Op) []iosched.Result {
	t.Helper()
	ticket := iosched.NewTicket()
	defer ticket.Release()
	ticket.Ops = ops
	require.NoError(t, s.Submit(ticket))
	ticket.Wait()
	results := make([]iosched.Result, len(ticket.Ops))
	for i := range ticket.Ops {
		results[i] = ticket.Ops[i].Result()
	}
	return results
}

// ── Basic Submit/Wait ─────────────────────────────────────────────────────────

func TestURing_Submit_Read(t *testing.T) {
	s := newURingSched(t)
	path, data := writeUringFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	results := submitWait(t, s, []iosched.Op{iosched.ReadOp(f, buf, 0)})
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)
	require.Equal(t, 4096, results[0].N)
	require.Equal(t, data, buf)
}

func TestURing_Submit_Write(t *testing.T) {
	s := newURingSched(t)
	path := filepath.Join(t.TempDir(), "write.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)

	results := submitWait(t, s, []iosched.Op{iosched.WriteOp(f, payload, 0)})
	require.NoError(t, results[0].Err)
	require.Equal(t, 4096, results[0].N)

	got := make([]byte, 4096)
	results = submitWait(t, s, []iosched.Op{iosched.ReadOp(f, got, 0)})
	require.NoError(t, results[0].Err)
	require.Equal(t, payload, got)
}

// ── Group commit: submit multiple submissions, wait on all ────────────────────

func TestURing_GroupCommit(t *testing.T) {
	const chunks = 16
	s := newURingSched(t)

	path := filepath.Join(t.TempDir(), "gc.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, chunks*4096), 0644))
	f := openRW(t, path)

	payloads := make([][]byte, chunks)
	tickets := make([]*iosched.Ticket, chunks)
	for i := range chunks {
		payloads[i] = make([]byte, 4096)
		_, _ = rand.Read(payloads[i])
		tickets[i] = iosched.NewTicket()
		tickets[i].Ops = []iosched.Op{iosched.WriteOp(f, payloads[i], int64(i*4096))}
		require.NoError(t, s.Submit(tickets[i]))
	}

	for i := range tickets {
		tickets[i].Wait()
		res := tickets[i].Ops[0].Result()
		require.NoError(t, res.Err, "chunk %d", i)
		require.Equal(t, 4096, res.N, "chunk %d", i)
		tickets[i].Release()
	}

	// Verify all writes.
	for i := range chunks {
		buf := make([]byte, 4096)
		results := submitWait(t, s, []iosched.Op{iosched.ReadOp(f, buf, int64(i*4096))})
		require.NoError(t, results[0].Err)
		require.Equal(t, payloads[i], buf, "mismatch at chunk %d", i)
	}
}

// ── Linked ops: write then fdatasync ─────────────────────────────────────────

func TestURing_LinkedOps_WriteFdatasync(t *testing.T) {
	s := newURingSched(t)
	path := filepath.Join(t.TempDir(), "linked.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)

	results := submitWait(t, s, []iosched.Op{
		iosched.WriteOp(f, payload, 0).Linked(),
		iosched.FdatasyncOp(f),
	})
	require.Len(t, results, 2)
	require.NoError(t, results[0].Err, "write result")
	require.Equal(t, 4096, results[0].N)
	require.NoError(t, results[1].Err, "fdatasync result")
}

// ── BlockingIO wrapping URingScheduler ────────────────────────────────────────

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

// ── Concurrent Submit/Wait ────────────────────────────────────────────────────

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

	for range goroutines {
		wg.Go(func() {
			buf := make([]byte, readSize)
			t := iosched.NewTicket()
			defer t.Release()
			for i := range readsPerGoroutine {
				offset := int64((i * readSize) % (fileSize - readSize))
				t.Ops = []iosched.Op{iosched.ReadOp(f, buf, offset)}
				if err := s.Submit(t); err != nil {
					errs <- fmt.Errorf("submit at %d: %w", offset, err)
					return
				}
				t.Wait()
				res := t.Ops[0].Result()
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

// ── Ring capacity overflow ────────────────────────────────────────────────────

// TestURing_BeyondRingCapacity submits more ops in a single Submission than
// the ring's SQ depth. The coordinator is expected to fill slots incrementally
// across multiple SubmitAndWait cycles until all ops complete.
func TestURing_BeyondRingCapacity(t *testing.T) {
	const ringDepth = 32 // deliberately small
	const numOps = ringDepth*3 + 7 // 103 ops — well beyond the ring

	s := newURingSched(t, iosched.URingConfig{RingDepth: ringDepth})

	// Write a file large enough to hold numOps × 4 KiB reads.
	data := make([]byte, numOps*4096)
	for i := range data {
		data[i] = byte(i)
	}
	path := filepath.Join(t.TempDir(), "overflow.dat")
	require.NoError(t, os.WriteFile(path, data, 0644))
	f := openRW(t, path)

	ops := make([]iosched.Op, numOps)
	bufs := make([][]byte, numOps)
	for i := range numOps {
		bufs[i] = make([]byte, 4096)
		ops[i] = iosched.ReadOp(f, bufs[i], int64(i*4096))
	}

	// Single Submission with numOps ops — coordinator must batch into the ring.
	results := submitWait(t, s, ops)
	require.Len(t, results, numOps)

	for i, r := range results {
		require.NoError(t, r.Err, "op %d failed", i)
		require.Equal(t, 4096, r.N, "op %d short read", i)
		require.Equal(t, data[i*4096:(i+1)*4096], bufs[i], "op %d data mismatch", i)
	}

	t.Logf("submitted %d ops into a ring of depth %d — all completed correctly", numOps, ringDepth)
}

// ── Close behaviour ───────────────────────────────────────────────────────────

func TestURing_CloseThenSubmit(t *testing.T) {
	s, err := iosched.NewURingScheduler(iosched.URingConfig{})
	require.NoError(t, err)
	require.NoError(t, s.Close())

	path := filepath.Join(t.TempDir(), "dummy.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	buf := make([]byte, 4096)
	ticket := iosched.NewTicket()
	defer ticket.Release()
	ticket.Ops = []iosched.Op{iosched.ReadOp(f, buf, 0)}
	err = s.Submit(ticket)
	require.Error(t, err)
}

// ── SQPOLL ────────────────────────────────────────────────────────────────────

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
	ticket := iosched.NewTicket()
	defer ticket.Release()
	ticket.Ops = []iosched.Op{iosched.ReadOp(f, buf, 0)}
	require.NoError(t, s.Submit(ticket))
	ticket.Wait()
	require.NoError(t, ticket.Ops[0].Result().Err)
	require.Equal(t, data, buf)
}

// ── RegisterDMASlab / fixed-buffer ops ───────────────────────────────────────

// newSlabPool creates a test SlabPool (2 MiB / 4 KiB slots) and registers it
// with the scheduler. The pool is closed in t.Cleanup after the scheduler.
func newRegisteredPool(t *testing.T, s *iosched.URingScheduler) *mempool.SlabPool {
	t.Helper()
	pool, err := mempool.NewSlabPool(align.HugepageSize, align.BlockSize)
	require.NoError(t, err)

	require.NoError(t, iosched.RegisterDMASlab(s, pool))
	t.Cleanup(func() { pool.Close() }) // pool must outlive the scheduler
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

	// Write via fixed buffer.
	wslot, err := pool.Acquire()
	require.NoError(t, err)
	copy(wslot.Data, payload)

	results := submitWait(t, s, []iosched.Op{iosched.WriteFixedOp(f, wslot, 0)})
	require.NoError(t, results[0].Err)
	require.Equal(t, align.BlockSize, results[0].N)
	wslot.Release()

	// Read back via fixed buffer.
	rslot, err := pool.Acquire()
	require.NoError(t, err)

	results = submitWait(t, s, []iosched.Op{iosched.ReadFixedOp(f, rslot, 0)})
	require.NoError(t, results[0].Err)
	require.Equal(t, align.BlockSize, results[0].N)
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

	// Submit all writes concurrently.
	for i := range chunks {
		payloads[i] = make([]byte, align.BlockSize)
		_, _ = rand.Read(payloads[i])

		slots[i], _ = pool.Acquire()
		copy(slots[i].Data, payloads[i])

		tickets[i] = iosched.NewTicket()
		tickets[i].Ops = []iosched.Op{iosched.WriteFixedOp(f, slots[i], int64(i*align.BlockSize))}
		require.NoError(t, s.Submit(tickets[i]))
	}

	for i := range tickets {
		tickets[i].Wait()
		require.NoError(t, tickets[i].Ops[0].Result().Err, "chunk %d write failed", i)
		slots[i].Release()
		tickets[i].Release()
	}

	// Verify on-disk content with normal reads.
	for i := range chunks {
		buf := make([]byte, align.BlockSize)
		results := submitWait(t, s, []iosched.Op{iosched.ReadOp(f, buf, int64(i*align.BlockSize))})
		require.NoError(t, results[0].Err)
		require.Equal(t, payloads[i], buf, "mismatch at chunk %d", i)
	}
}

func TestURing_FixedOp_WithoutRegistration(t *testing.T) {
	s := newURingSched(t)

	// Construct a slot from an unregistered pool — kernel has no pinned buffer.
	pool, err := mempool.NewSlabPool(align.HugepageSize, align.BlockSize)
	require.NoError(t, err)
	defer pool.Close()

	path := filepath.Join(t.TempDir(), "noreg.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, align.BlockSize), 0644))
	f := openRW(t, path)

	slot, err := pool.Acquire()
	require.NoError(t, err)
	defer slot.Release()

	results := submitWait(t, s, []iosched.Op{iosched.WriteFixedOp(f, slot, 0)})
	require.Error(t, results[0].Err) // the op failed — kernel rejects unregistered slot
	t.Logf("kernel error for unregistered fixed op: %v", results[0].Err)
}

// ── MaxInflightOps backpressure ──────────────────────────────────────────────

// TestURing_MaxInflightOps_ReturnsErrTooBusy verifies that Submit returns
// ErrTooBusy when the submission's op count alone exceeds the budget. This
// is the deterministic case — no timing required.
func TestURing_MaxInflightOps_ReturnsErrTooBusy(t *testing.T) {
	s := newURingSched(t, iosched.URingConfig{
		RingDepth:      256,
		MaxInflightOps: 4,
	})
	path, _ := writeUringFile(t, 5*4096)
	f := openRW(t, path)

	// Submission of 5 ops exceeds MaxInflightOps=4 immediately.
	ops := make([]iosched.Op, 5)
	bufs := make([][]byte, 5)
	for i := range ops {
		bufs[i] = make([]byte, 4096)
		ops[i] = iosched.ReadOp(f, bufs[i], int64(i*4096))
	}
	ticket := iosched.NewTicket()
	defer ticket.Release()
	ticket.Ops = ops
	err := s.Submit(ticket)
	require.ErrorIs(t, err, iosched.ErrTooBusy)
}

// TestURing_MaxInflightOps_RecoversAfterCompletion submits up to budget,
// waits for completion, and verifies a subsequent submission succeeds.
func TestURing_MaxInflightOps_RecoversAfterCompletion(t *testing.T) {
	s := newURingSched(t, iosched.URingConfig{
		RingDepth:      256,
		MaxInflightOps: 4,
	})
	path, _ := writeUringFile(t, 4096)
	f := openRW(t, path)

	// Submit a 4-op submission (exactly at budget).
	bufs := make([][]byte, 4)
	ops := make([]iosched.Op, 4)
	for i := range ops {
		bufs[i] = make([]byte, 4096)
		ops[i] = iosched.ReadOp(f, bufs[i], 0)
	}
	t1 := iosched.NewTicket()
	defer t1.Release()
	t1.Ops = ops
	require.NoError(t, s.Submit(t1))
	t1.Wait()

	// Budget should now be restored. Submit again.
	buf := make([]byte, 4096)
	t2 := iosched.NewTicket()
	defer t2.Release()
	t2.Ops = []iosched.Op{iosched.ReadOp(f, buf, 0)}
	require.NoError(t, s.Submit(t2))
	t2.Wait()
	require.NoError(t, t2.Ops[0].Result().Err)
}

// TestURing_MaxInflightOps_UnlimitedByDefault verifies that with the default
// MaxInflightOps=0, Submit never returns ErrTooBusy regardless of volume.
func TestURing_MaxInflightOps_UnlimitedByDefault(t *testing.T) {
	s := newURingSched(t) // default config; MaxInflightOps=0
	path, _ := writeUringFile(t, 4096)
	f := openRW(t, path)

	const N = 500
	tickets := make([]*iosched.Ticket, N)
	bufs := make([][]byte, N)
	for i := range tickets {
		bufs[i] = make([]byte, 4096)
		tickets[i] = iosched.NewTicket()
		tickets[i].Ops = []iosched.Op{iosched.ReadOp(f, bufs[i], 0)}
		require.NoError(t, s.Submit(tickets[i]))
	}
	for i := range tickets {
		tickets[i].Wait()
		require.NoError(t, tickets[i].Ops[0].Result().Err)
		tickets[i].Release()
	}
}

// ── DefaultIO uses URingScheduler on Linux ────────────────────────────────────

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

// ── Submission lifecycle / API ────────────────────────────────────────────────

// TestURing_Ticket_LongDelayBeforeWait verifies that a Ticket remains valid
// (and the results readable) even when the caller waits long after Submit.
// The library-pooled Ticket lets the scheduler complete the IO concurrently;
// Wait then returns immediately on the already-done WaitGroup.
func TestURing_Ticket_LongDelayBeforeWait(t *testing.T) {
	s := newURingSched(t)
	path, data := writeUringFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	ticket := iosched.NewTicket()
	defer ticket.Release()
	ticket.Ops = []iosched.Op{iosched.ReadOp(f, buf, 0)}
	require.NoError(t, s.Submit(ticket))

	// Sleep long enough that the op has surely completed.
	time.Sleep(100 * time.Millisecond)

	// Wait returns immediately since the WaitGroup counter is already 0.
	ticket.Wait()
	res := ticket.Ops[0].Result()
	require.NoError(t, res.Err)
	require.Equal(t, 4096, res.N)
	require.Equal(t, data, buf)
}

// TestURing_Ticket_PooledRoundTrip exercises explicit Ticket pool reuse.
// NewTicket already returns from a sync.Pool internally; this test confirms
// that NewTicket → Submit → Wait → Release cycle is repeatable.
func TestURing_Ticket_PooledRoundTrip(t *testing.T) {
	s := newURingSched(t)
	path, data := writeUringFile(t, 4096)
	f := openRW(t, path)

	for i := 0; i < 100; i++ {
		ticket := iosched.NewTicket()
		buf := make([]byte, 4096)
		ticket.Ops = []iosched.Op{iosched.ReadOp(f, buf, 0)}
		require.NoError(t, s.Submit(ticket))
		ticket.Wait()
		res := ticket.Ops[0].Result()
		require.NoError(t, res.Err, "iter %d", i)
		require.Equal(t, data, buf, "iter %d", i)
		ticket.Release()
	}
}

// TestURing_Ticket_ContextNotComposable documents that Ticket.Wait does NOT
// compose with context cancellation via select. (sync.WaitGroup does not
// expose a chan.) Callers needing cancellation must wrap Wait in their own
// goroutine; this test verifies that Wait still blocks until completion.
func TestURing_Ticket_ContextNotComposable(t *testing.T) {
	s := newURingSched(t)
	path, _ := writeUringFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	ticket := iosched.NewTicket()
	defer ticket.Release()
	ticket.Ops = []iosched.Op{iosched.ReadOp(f, buf, 0)}
	require.NoError(t, s.Submit(ticket))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	_ = ctx  // ctx is not selectable against ticket.Wait

	// ticket.Wait() blocks until the coordinator signals; ctx cancellation
	// is invisible to it. Wait should return cleanly.
	ticket.Wait()
	require.NoError(t, ticket.Ops[0].Result().Err)
}

// TestURing_Ticket_ManyConcurrentWithoutLimit verifies that with default
// MaxInflightOps=0 (unlimited), many concurrent Submits succeed.
func TestURing_Ticket_ManyConcurrentWithoutLimit(t *testing.T) {
	s := newURingSched(t) // default config; MaxInflightOps=0

	path, _ := writeUringFile(t, 4096)
	f := openRW(t, path)

	const N = 200
	tickets := make([]*iosched.Ticket, N)
	bufs := make([][]byte, N)

	var completed atomic.Int32
	for i := range N {
		bufs[i] = make([]byte, 4096)
		tickets[i] = iosched.NewTicket()
		tickets[i].Ops = []iosched.Op{iosched.ReadOp(f, bufs[i], 0)}
		require.NoError(t, s.Submit(tickets[i]))
	}
	for i := range tickets {
		tickets[i].Wait()
		if tickets[i].Ops[0].Result().Err == nil {
			completed.Add(1)
		}
		tickets[i].Release()
	}
	require.Equal(t, int32(N), completed.Load())
}
