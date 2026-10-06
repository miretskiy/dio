//go:build linux

package iosched

import (
	"bytes"
	"os"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/mempool"
	"github.com/stretchr/testify/require"
)

func TestURingDMARegistrationOwnership(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	_, err := NewURingScheduler(WithRingDepth(8), WithDMASlab(nil))
	require.ErrorContains(t, err, "nil DMA slab")
	pool, err := mempool.NewSlabPool(align.HugepageSize, align.BlockSize)
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

// TestURingAlignedBufferFixedIO writes an AlignedBuffer allocated from a
// registered SlabPool with a fixed-buffer write per chunk, and reads it back
// the same way.
func TestURingAlignedBufferFixedIO(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	slab, err := mempool.NewSlabPool(align.HugepageSize, 64<<10)
	require.NoError(t, err)
	s, err := NewURingScheduler(WithRingDepth(8), WithDMASlab(slab))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, s.Close())
		slab.Close()
	}()

	f, err := os.CreateTemp(t.TempDir(), "fixed")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	size := 3*slab.SlotSize() + 100
	src := mempool.NewAlignedBuffer(slab, size)
	defer src.Release()
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	_, err = src.Write(payload)
	require.NoError(t, err)

	// Each chunk is one slot of the registered slab, so each is a fixed I/O.
	transfer := int(align.PageAlign(int64(size)))
	var off int64
	for _, piece := range src.Slices(0, transfer, nil) {
		ticket, err := s.Submit(WriteFixedOp(f, piece, off))
		require.NoError(t, err)
		n, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, len(piece), n)
		off += int64(n)
	}

	dst := mempool.NewAlignedBuffer(slab, transfer)
	defer dst.Release()
	require.NoError(t, dst.Grow(transfer))
	off = 0
	for _, piece := range dst.Slices(0, transfer, nil) {
		ticket, err := s.Submit(ReadFixedOp(f, piece, off))
		require.NoError(t, err)
		n, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, len(piece), n)
		off += int64(n)
	}
	dst.SetLen(size)
	var got bytes.Buffer
	_, err = dst.WriteTo(&got)
	require.NoError(t, err)
	require.Equal(t, payload, got.Bytes())
}
