//go:build linux

package iosched

import (
	"os"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func TestURingDMARegistrationOwnership(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	_, err := NewURingScheduler(WithRingDepth(8), WithDMASlab(nil))
	require.ErrorContains(t, err, "nil DMA slab")
	pool, err := alloc.NewSlabPool(align.HugepageSize, align.BlockSize)
	require.NoError(t, err)
	s, err := NewURingScheduler(WithRingDepth(8), WithDMASlab(pool))
	require.NoError(t, err)

	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, s.Close())
		}
		pool.Close()
	})

	require.Same(t, pool, s.registeredPool)

	err = s.Close()
	closed = true
	require.NoError(t, err)
	require.Nil(t, s.registeredPool)
}

// A stream larger than the entire pool reuses one registered staging buffer.
func TestURingPooledBufferFixedIO(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	const chunkSize = 64 << 10
	pool, err := alloc.NewMmapPool(chunkSize, 1)
	require.NoError(t, err)
	defer pool.Close()
	backing, err := pool.TryAcquire()
	require.NoError(t, err)
	slab, err := alloc.NewSlabPoolFrom(backing, chunkSize)
	backing.Unpin() // The slab owns its own reference to the pooled mapping.
	require.NoError(t, err)
	defer slab.Close()
	s, err := NewURingScheduler(WithRingDepth(8), WithDMASlab(slab))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()

	f, err := os.CreateTemp(t.TempDir(), "fixed")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	buf, err := slab.TryAcquire()
	require.NoError(t, err)
	defer buf.Unpin()
	_, err = slab.TryAcquire()
	require.ErrorIs(t, err, alloc.ErrExhausted)

	payload := make([]byte, 3*chunkSize+100)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	for off := 0; off < len(payload); off += chunkSize {
		n := min(chunkSize, len(payload)-off)
		piece := buf.Bytes()[:int(align.PageAlign(int64(n)))]
		clear(piece)
		copy(piece, payload[off:off+n])
		ticket, err := s.Submit(WriteFixedOp(f, piece, int64(off)))
		require.NoError(t, err)
		written, err := ticket.Wait() // Complete I/O before reusing the buffer.
		require.NoError(t, err)
		require.Equal(t, len(piece), written)
	}
	for off := 0; off < len(payload); off += chunkSize {
		n := min(chunkSize, len(payload)-off)
		piece := buf.Bytes()[:int(align.PageAlign(int64(n)))]
		clear(piece)
		ticket, err := s.Submit(ReadFixedOp(f, piece, int64(off)))
		require.NoError(t, err)
		read, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, len(piece), read)
		require.Equal(t, payload[off:off+n], piece[:n])
	}
}
