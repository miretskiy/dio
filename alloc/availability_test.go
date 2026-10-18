package alloc

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type measuredAllocator interface {
	Allocator
	Capacity() int64
	Available() int64
}

func TestAllocatorAvailability(t *testing.T) {
	for _, kind := range []string{"slab", "extent"} {
		for _, chunks := range []int{3, 65} {
			t.Run(fmt.Sprintf("%s/chunks%d", kind, chunks), func(t *testing.T) {
				const chunk = 8192
				var a measuredAllocator
				var err error
				if kind == "slab" {
					a, err = NewSlabAllocator(int64(chunks*chunk+4096), WithChunkSize(chunk))
				} else {
					a, err = NewExtentAllocator(chunk, chunks)
				}
				require.NoError(t, err)
				defer a.Close()
				capacity := int64(chunks * chunk)
				require.Equal(t, capacity, a.Capacity())
				require.Equal(t, capacity, a.Available())
				b, err := a.TryAcquire(chunk + 1)
				require.NoError(t, err)
				require.Equal(t, capacity-2*chunk, a.Available())
				require.True(t, b.TryInc())
				require.False(t, b.Unpin())
				require.Equal(t, capacity-2*chunk, a.Available())
				_, err = a.TryAcquire(capacity)
				require.ErrorIs(t, err, ErrExhausted)
				require.Equal(t, capacity-2*chunk, a.Available(), "failed partial claims are rolled back")
				require.True(t, b.Unpin())
				require.Equal(t, capacity, a.Available())
				b, err = a.TryAcquire(capacity)
				require.NoError(t, err)
				require.Zero(t, a.Available())
				require.True(t, b.Unpin())
				require.Equal(t, capacity, a.Available())
				a.Close()
				require.Zero(t, a.Capacity())
				require.Zero(t, a.Available())
			})
		}
	}
}

func TestSlabAvailabilityDuringConcurrentUse(t *testing.T) {
	a, err := NewSlabAllocator(128 * 4096)
	require.NoError(t, err)
	defer a.Close()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				b, err := a.TryAcquire(3 * 4096)
				if err != nil {
					t.Error(err)
					return
				}
				available := a.Available()
				if available < 0 || available > a.Capacity() || available%4096 != 0 {
					t.Errorf("invalid availability snapshot: %d", available)
				}
				b.Unpin()
			}
		})
	}
	wg.Wait()
	require.Equal(t, a.Capacity(), a.Available())
}
