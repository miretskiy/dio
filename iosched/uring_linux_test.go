//go:build linux

package iosched

import (
	"bytes"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/miretskiy/dio/mempool"
	"github.com/stretchr/testify/require"
)

// newURingForTest creates a URingScheduler, skipping the test when the
// environment forbids io_uring (e.g. seccomp-filtered sandboxes).
func newURingForTest(t *testing.T, cfg URingConfig) Scheduler {
	s, err := NewURingScheduler(cfg)
	if err != nil {
		t.Skipf("io_uring unavailable: %v", err)
		return nil
	}
	return s
}

func TestURingRingDepthValidation(t *testing.T) {
	_, err := NewURingScheduler(URingConfig{RingDepth: 100})
	require.Error(t, err)
}

func TestURingMaxInflightOps(t *testing.T) {
	s := newURingForTest(t, URingConfig{MaxInflightOps: 4})
	if s == nil {
		return
	}
	defer func() { require.NoError(t, s.Close()) }()

	f := tempFile(t)
	fd := int(f.Fd())
	buf := make([]byte, 16)

	ops := make([]Op, 5)
	for i := range ops {
		ops[i] = WriteOp(fd, buf, int64(i*16))
	}
	require.ErrorIs(t, s.Submit(ops), ErrTooBusy)

	// Within budget succeeds, and the budget is released on completion.
	for round := 0; round < 3; round++ {
		require.NoError(t, s.Submit(ops[:4]))
		for i := range ops[:4] {
			require.NoError(t, ops[i].Result().Err)
		}
	}
}

// TestURingOverflow forces ops through the overflow FIFO by submitting far
// more ops in one call than the tiny ring can hold.
func TestURingOverflow(t *testing.T) {
	s := newURingForTest(t, URingConfig{RingDepth: 4})
	if s == nil {
		return
	}
	defer func() { require.NoError(t, s.Close()) }()

	f := tempFile(t)
	fd := int(f.Fd())

	const blocks = 64
	const bs = 512
	want := make([]byte, blocks*bs)
	_, err := rand.NewChaCha8([32]byte{3}).Read(want)
	require.NoError(t, err)

	ops := make([]Op, blocks)
	for i := range ops {
		ops[i] = WriteOp(fd, want[i*bs:(i+1)*bs], int64(i*bs))
	}
	require.NoError(t, s.Submit(ops))
	for i := range ops {
		r := ops[i].Result()
		require.NoError(t, r.Err)
		require.Equal(t, bs, r.N)
	}

	got := make([]byte, blocks*bs)
	for i := range ops {
		ops[i] = ReadOp(fd, got[i*bs:(i+1)*bs], int64(i*bs))
	}
	require.NoError(t, s.Submit(ops))
	require.True(t, bytes.Equal(want, got))
}

// TestURingChainTooLong: a link chain longer than the ring can never be
// placed contiguously and must be rejected up front.
func TestURingChainTooLong(t *testing.T) {
	s := newURingForTest(t, URingConfig{RingDepth: 4})
	if s == nil {
		return
	}
	defer func() { require.NoError(t, s.Close()) }()

	f := tempFile(t)
	fd := int(f.Fd())
	buf := make([]byte, 8)
	ops := make([]Op, 6)
	for i := range ops {
		ops[i] = WriteOp(fd, buf, int64(i*8)).Linked()
	}
	ops[len(ops)-1] = WriteOp(fd, buf, 0)
	require.Error(t, s.Submit(ops))
}

func TestURingFixedBuffers(t *testing.T) {
	s := newURingForTest(t, URingConfig{})
	if s == nil {
		return
	}
	defer func() { require.NoError(t, s.Close()) }()

	pool, err := mempool.NewSlabPool(4<<20, 8<<10)
	require.NoError(t, err)
	defer pool.Close()
	if err := RegisterDMASlab(s, pool); err != nil {
		// Registration pins the slab and counts against RLIMIT_MEMLOCK.
		t.Skipf("cannot register DMA slab (RLIMIT_MEMLOCK?): %v", err)
	}

	f := tempFile(t)
	fd := int(f.Fd())

	wslot, err := pool.Acquire()
	require.NoError(t, err)
	defer wslot.Release()
	rslot, err := pool.Acquire()
	require.NoError(t, err)
	defer rslot.Release()

	_, err = rand.NewChaCha8([32]byte{9}).Read(wslot.Data)
	require.NoError(t, err)

	wr := []Op{WriteFixedOp(fd, wslot.Data, 0)}
	require.NoError(t, s.Submit(wr))
	require.NoError(t, wr[0].Result().Err)
	require.Equal(t, len(wslot.Data), wr[0].Result().N)

	rd := []Op{ReadFixedOp(fd, rslot.Data, 0)}
	require.NoError(t, s.Submit(rd))
	require.NoError(t, rd[0].Result().Err)
	require.True(t, bytes.Equal(wslot.Data, rslot.Data))

	// Re-registration replaces the previous slab.
	pool2, err := mempool.NewSlabPool(2<<20, 8<<10)
	require.NoError(t, err)
	defer pool2.Close()
	if err := RegisterDMASlab(s, pool2); err != nil {
		t.Skipf("cannot re-register DMA slab (RLIMIT_MEMLOCK?): %v", err)
	}
}

// RegisterDMASlab on a scheduler without fixed-buffer support is a no-op.
func TestRegisterDMASlabPosixNoop(t *testing.T) {
	pool, err := mempool.NewSlabPool(2<<20, 4<<10)
	require.NoError(t, err)
	defer pool.Close()
	s := NewPosixScheduler()
	defer s.Close()
	require.NoError(t, RegisterDMASlab(s, pool))
}

func TestURingSQPoll(t *testing.T) {
	// SQPOLL requires privileges on some kernels; skip if setup fails.
	s := newURingForTest(t, URingConfig{SQPOLL: true})
	if s == nil {
		return
	}
	defer func() { require.NoError(t, s.Close()) }()

	f := tempFile(t)
	fd := int(f.Fd())
	want := []byte("sqpoll roundtrip")
	wr := []Op{WriteOp(fd, want, 0)}
	require.NoError(t, s.Submit(wr))
	require.NoError(t, wr[0].Result().Err)

	got := make([]byte, len(want))
	rd := []Op{ReadOp(fd, got, 0)}
	require.NoError(t, s.Submit(rd))
	require.Equal(t, want, got)
}

// TestURingLeaderHandoff creates heavy goroutine churn on a small ring so
// leadership is handed off constantly, then verifies all data. Run with
// -race to exercise the synchronization protocol.
func TestURingLeaderHandoff(t *testing.T) {
	s := newURingForTest(t, URingConfig{RingDepth: 8})
	if s == nil {
		return
	}
	defer func() { require.NoError(t, s.Close()) }()

	const goroutines = 16
	const rounds = 100
	const bs = 1024

	f := tempFile(t)
	fd := int(f.Fd())
	require.NoError(t, f.Truncate(goroutines*rounds*bs))

	var wg sync.WaitGroup
	for gi := 0; gi < goroutines; gi++ {
		wg.Add(1)
		go func(gi int) {
			defer wg.Done()
			wbuf := make([]byte, bs)
			rbuf := make([]byte, bs)
			for round := 0; round < rounds; round++ {
				for i := range wbuf {
					wbuf[i] = byte(gi ^ round ^ i)
				}
				off := int64((gi*rounds + round) * bs)
				ops := []Op{WriteOp(fd, wbuf, off)}
				if err := s.Submit(ops); err != nil || ops[0].Result().Err != nil {
					t.Errorf("write: %v / %v", err, ops[0].Result().Err)
					return
				}
				ops[0] = ReadOp(fd, rbuf, off)
				if err := s.Submit(ops); err != nil || ops[0].Result().Err != nil {
					t.Errorf("read: %v / %v", err, ops[0].Result().Err)
					return
				}
				if !bytes.Equal(wbuf, rbuf) {
					t.Errorf("g%d round %d: mismatch", gi, round)
					return
				}
			}
		}(gi)
	}
	wg.Wait()
}
