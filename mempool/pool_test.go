package mempool_test

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/mempool"
)

// ─── Basic acquire / unpin ────────────────────────────────────────────────────

func TestMmapPool_AcquireUnpin(t *testing.T) {
	pool := mempool.NewMmapPool("test", 4096, 2)
	defer pool.Close()

	require.Equal(t, 2, pool.Capacity())
	require.Equal(t, int64(0), pool.Outstanding())

	buf := pool.Acquire()
	require.NotNil(t, buf)
	require.Equal(t, int64(1), pool.Outstanding())

	buf.Unpin()
	require.Equal(t, int64(0), pool.Outstanding())
}

func TestMmapPool_BytesAreAligned(t *testing.T) {
	pool := mempool.NewMmapPool("test", 4096, 1)
	defer pool.Close()

	buf := pool.Acquire()
	defer buf.Unpin()

	raw := buf.Bytes()
	require.True(t, align.IsAligned(raw), "pool buffer must be page-aligned")
	require.Equal(t, 4096, len(raw))
}

func TestMmapPool_AlignedBytes(t *testing.T) {
	pool := mempool.NewMmapPool("test", 1<<20, 1)
	defer pool.Close()

	buf := pool.Acquire()
	defer buf.Unpin()

	// Zero or negative → nil
	require.Nil(t, buf.AlignedBytes(0))
	require.Nil(t, buf.AlignedBytes(-1))

	// 1 byte of valid data → returns a full page (4096 bytes).
	ab := buf.AlignedBytes(1)
	require.Equal(t, 4096, len(ab))
	require.True(t, align.IsAligned(ab))

	// 4096 bytes → 4096
	require.Equal(t, 4096, len(buf.AlignedBytes(4096)))

	// 4097 bytes → 8192 (next page boundary)
	require.Equal(t, 8192, len(buf.AlignedBytes(4097)))
}

// ─── TryAcquire ──────────────────────────────────────────────────────────────

func TestMmapPool_TryAcquire_SucceedsWhileAvailable(t *testing.T) {
	pool := mempool.NewMmapPool("test", 4096, 2)
	defer pool.Close()

	b1, ok := pool.TryAcquire()
	require.True(t, ok)
	require.NotNil(t, b1)

	b2, ok := pool.TryAcquire()
	require.True(t, ok)
	require.NotNil(t, b2)

	// Pool is now empty.
	b3, ok := pool.TryAcquire()
	require.False(t, ok)
	require.Nil(t, b3)

	b1.Unpin()
	b2.Unpin()

	// After returning both, one slot is available again.
	b4, ok := pool.TryAcquire()
	require.True(t, ok)
	require.NotNil(t, b4)
	b4.Unpin()
}

// ─── Blocking backpressure ────────────────────────────────────────────────────

func TestMmapPool_Acquire_BlocksWhenEmpty(t *testing.T) {
	pool := mempool.NewMmapPool("test", 4096, 1)
	defer pool.Close()

	// Drain the pool.
	holder := pool.Acquire()

	// A second Acquire must block.
	acquired := make(chan *mempool.MmapBuffer, 1)
	go func() {
		acquired <- pool.Acquire()
	}()

	// Confirm it is blocked for a short window.
	select {
	case <-acquired:
		t.Fatal("Acquire should have blocked on empty pool")
	case <-time.After(50 * time.Millisecond):
		// Good — still waiting.
	}

	// Release the held buffer; the goroutine must unblock.
	holder.Unpin()

	select {
	case buf := <-acquired:
		require.NotNil(t, buf)
		buf.Unpin()
	case <-time.After(time.Second):
		t.Fatal("Acquire did not unblock after Unpin")
	}
}

// ─── Reference counting (TryInc) ─────────────────────────────────────────────

func TestMmapBuffer_TryInc_IncrementsRef(t *testing.T) {
	pool := mempool.NewMmapPool("test", 4096, 1)
	defer pool.Close()

	buf := pool.Acquire() // refCount = 1

	ok := buf.TryInc() // refCount = 2
	require.True(t, ok, "TryInc on live buffer must succeed")

	// Unpin twice to return to pool.
	buf.Unpin() // refCount = 1
	require.Equal(t, int64(1), pool.Outstanding(), "still pinned after first Unpin")

	buf.Unpin() // refCount = 0 → returned to pool
	require.Equal(t, int64(0), pool.Outstanding())
}

func TestMmapBuffer_TryInc_FailsAfterRelease(t *testing.T) {
	pool := mempool.NewMmapPool("test", 4096, 1)
	defer pool.Close()

	buf := pool.Acquire()
	buf.Unpin() // returned to pool — refCount is 0

	ok := buf.TryInc()
	require.False(t, ok, "TryInc on released buffer must fail")
}

func TestMmapBuffer_MultipleReaders(t *testing.T) {
	const readers = 8
	pool := mempool.NewMmapPool("test", 4096, 1)
	defer pool.Close()

	buf := pool.Acquire() // refCount = 1

	// Simulate multiple readers each acquiring a reference.
	for i := 0; i < readers; i++ {
		ok := buf.TryInc()
		require.True(t, ok)
	}

	// Primary writer releases its pin.
	buf.Unpin() // refCount = readers
	require.Equal(t, int64(1), pool.Outstanding())

	// Each reader releases.
	for i := 0; i < readers; i++ {
		buf.Unpin()
	}
	require.Equal(t, int64(0), pool.Outstanding())
}

// ─── ABA safety ──────────────────────────────────────────────────────────────

func TestMmapPool_ABA_FreshStruct(t *testing.T) {
	// Each Acquire must return a distinct *MmapBuffer even if the underlying
	// memory address is identical (same slot reused).
	pool := mempool.NewMmapPool("test", 4096, 1)
	defer pool.Close()

	first := pool.Acquire()
	firstPtr := first
	first.Unpin()

	second := pool.Acquire()
	defer second.Unpin()

	// Different struct pointers — ABA is prevented.
	require.NotSame(t, firstPtr, second,
		"each Acquire must produce a fresh *MmapBuffer")
}

// ─── Double-release behaviour ─────────────────────────────────────────────────

// TestMmapBuffer_DoubleUnpin_DefaultSilent verifies the default (permissive)
// behaviour: extra Unpins are silently ignored.
//
// This is the right default for production code where context-cancellation
// races can cause multiple goroutines to attempt to release the same buffer.
func TestMmapBuffer_DoubleUnpin_DefaultSilent(t *testing.T) {
	pool := mempool.NewMmapPool("test", 4096, 2)
	defer pool.Close()

	buf := pool.Acquire()
	buf.Unpin() // refCount → 0: resetAndRelease called, memory returned

	// Second Unpin drives refCount to -1; the == 0 guard prevents
	// resetAndRelease from being called again. Must not crash.
	require.NotPanics(t, func() { buf.Unpin() })
	require.Equal(t, int64(0), pool.Outstanding())
}

// TestMmapBuffer_DoubleUnpin_StrictMode verifies that SetPanicOnMisuse(true)
// causes the second Unpin to panic.
func TestMmapBuffer_DoubleUnpin_StrictMode(t *testing.T) {
	mempool.SetPanicOnMisuse(true)
	defer mempool.SetPanicOnMisuse(false) // restore for other tests

	pool := mempool.NewMmapPool("test", 4096, 1)
	defer pool.Close()

	buf := pool.Acquire()
	buf.Unpin()

	require.Panics(t, func() { buf.Unpin() })
}

// ─── Concurrent stress ────────────────────────────────────────────────────────

func TestMmapPool_Concurrent(t *testing.T) {
	const (
		capacity = 8
		workers  = 32
		iters    = 200
	)
	pool := mempool.NewMmapPool("stress", 4096, capacity)
	defer pool.Close()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				var buf *mempool.MmapBuffer
				// Mix Acquire and TryAcquire.
				if i%3 == 0 {
					b, ok := pool.TryAcquire()
					if !ok {
						continue
					}
					buf = b
				} else {
					buf = pool.Acquire()
				}
				// Write a recognisable pattern.
				raw := buf.Bytes()
				raw[0] = byte(i)
				buf.Unpin()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(0), pool.Outstanding())
}

// ─── Metrics ─────────────────────────────────────────────────────────────────

func TestMmapPool_OutstandingMetric(t *testing.T) {
	pool := mempool.NewMmapPool("test", 4096, 4)
	defer pool.Close()

	bufs := make([]*mempool.MmapBuffer, 4)
	for i := range bufs {
		bufs[i] = pool.Acquire()
		require.Equal(t, int64(i+1), pool.Outstanding())
	}
	for i := range bufs {
		bufs[i].Unpin()
		require.Equal(t, int64(len(bufs)-i-1), pool.Outstanding())
	}
}

// ─── GC safety (runtime.KeepAlive pattern) ───────────────────────────────────

func TestMmapBuffer_GCSafety(t *testing.T) {
	// Verify that a buffer pinned by TryInc is not reclaimed by GC while
	// a concurrent goroutine holds a TryInc reference.
	pool := mempool.NewMmapPool("gc", 4096, 1)
	defer pool.Close()

	buf := pool.Acquire()
	buf.TryInc() // reader

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Simulate a reader holding the buffer for a GC cycle.
		raw := buf.Bytes()
		for i := 0; i < 3; i++ {
			runtime.GC()
			time.Sleep(5 * time.Millisecond)
		}
		_ = raw[0] // access after GC
		buf.Unpin()
	}()

	buf.Unpin() // writer done
	<-done
}
