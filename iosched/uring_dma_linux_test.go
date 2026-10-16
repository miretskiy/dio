//go:build linux

package iosched

import (
	"os"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func TestURingDMABuffersCallerOwnership(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	buffer := alloc.NewBuffer(4096)
	released := false
	defer func() {
		if !released {
			buffer.Unpin()
		}
	}()
	s, err := NewURingScheduler(WithRingDepth(8), WithDMABuffers(buffer))
	require.NoError(t, err)
	closed := false
	defer func() {
		if !closed {
			require.NoError(t, s.Close())
		}
	}()
	require.Equal(t, 1, s.registeredBuffers.Len())
	require.Empty(t, s.config.dmaBuffers)
	err = s.Close()
	closed = true
	require.NoError(t, err)
	require.Nil(t, s.registeredBuffers)
	// Scheduler shutdown must neither release the caller's reference nor leave
	// behind an extra reference. The caller's one Unpin releases the buffer.
	require.True(t, buffer.TryInc())
	buffer.Unpin()
	buffer.Chunks()[0][0] = 42
	buffer.Unpin()
	released = true
	require.False(t, buffer.TryInc())
}

func TestURingFixedIOWithExplicitBuffers(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	for _, kind := range []string{"slab", "extent"} {
		t.Run(kind, func(t *testing.T) {
			const chunk = 4096
			var allocator alloc.Allocator
			var err error
			if kind == "slab" {
				allocator, err = alloc.NewSlabAllocator(7 * chunk)
			} else {
				allocator, err = alloc.NewExtentAllocator(chunk, 7)
			}
			require.NoError(t, err)
			defer allocator.Close()
			var buffers []*alloc.Buffer
			defer func() {
				for _, b := range buffers {
					b.Unpin()
				}
			}()
			held := make([]*alloc.Buffer, 7)
			defer func() {
				for _, b := range held {
					if b != nil {
						b.Unpin()
					}
				}
			}()
			for i := range held {
				held[i], err = allocator.TryAcquire(4096)
				require.NoError(t, err)
			}
			// Register individual allocations without including their neighbors.
			for _, i := range []int{0, 2, 4} {
				held[i].Unpin()
				held[i] = nil
			}
			multi, err := allocator.TryAcquire(3 * chunk)
			require.NoError(t, err)
			_, err = NewURingScheduler(WithRingDepth(8), WithDMABuffers(multi))
			multi.Unpin()
			require.ErrorContains(t, err, "DMA buffer 0 has 3 chunks; expected one")
			for range 3 {
				buffer, err := allocator.TryAcquire(chunk)
				require.NoError(t, err)
				buffers = append(buffers, buffer)
			}
			for range 3 {
				buffers = append(buffers, alloc.NewBuffer(chunk))
			}
			s, err := NewURingScheduler(WithRingDepth(8), WithDMABuffers(buffers...))
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close()) }()
			require.Equal(t, len(buffers), s.registeredBuffers.Len())
			f, err := os.CreateTemp(t.TempDir(), "explicit-buffers")
			require.NoError(t, err)
			defer f.Close()
			_, err = s.Submit(WriteFixedOp(f, held[1].Chunks()[0], 0))
			require.ErrorContains(t, err, "outside the registered DMA buffers")
			for i, buffer := range buffers {
				data := buffer.Chunks()[0]
				for j := range data {
					data[j] = byte(i + 1)
				}
				ticket, err := s.Submit(WriteFixedOp(f, data, int64(i*chunk)))
				require.NoError(t, err)
				n, err := ticket.Wait()
				require.NoError(t, err)
				require.Equal(t, chunk, n)
			}
			for i, buffer := range buffers {
				data := buffer.Chunks()[0]
				clear(data)
				ticket, err := s.Submit(ReadFixedOp(f, data, int64(i*chunk)))
				require.NoError(t, err)
				n, err := ticket.Wait()
				require.NoError(t, err)
				require.Equal(t, chunk, n)
				for _, v := range data {
					require.Equal(t, byte(i+1), v)
				}
			}
		})
	}
}

// A stream larger than the entire allocator reuses one caller-owned registered buffer.
func TestURingAllocatorBufferFixedIO(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	const chunkSize = 64 << 10
	allocator, err := alloc.NewExtentAllocator(chunkSize, 1)
	require.NoError(t, err)
	defer allocator.Close()
	buf, err := allocator.TryAcquire(chunkSize)
	require.NoError(t, err)
	defer buf.Unpin()
	s, err := NewURingScheduler(WithRingDepth(8), WithDMABuffers(buf))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	f, err := os.CreateTemp(t.TempDir(), "fixed")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	_, err = allocator.TryAcquire(chunkSize)
	require.ErrorIs(t, err, alloc.ErrExhausted)
	payload := make([]byte, 3*chunkSize+100)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	for off := 0; off < len(payload); off += chunkSize {
		n := min(chunkSize, len(payload)-off)
		piece := buf.Chunks()[0][:int(align.PageAlign(int64(n)))]
		clear(piece)
		copy(piece, payload[off:off+n])
		ticket, err := s.Submit(WriteFixedOp(f, piece, int64(off)))
		require.NoError(t, err)
		written, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, len(piece), written)
	}
	for off := 0; off < len(payload); off += chunkSize {
		n := min(chunkSize, len(payload)-off)
		piece := buf.Chunks()[0][:int(align.PageAlign(int64(n)))]
		clear(piece)
		ticket, err := s.Submit(ReadFixedOp(f, piece, int64(off)))
		require.NoError(t, err)
		read, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, len(piece), read)
		require.Equal(t, payload[off:off+n], piece[:n])
	}
}
