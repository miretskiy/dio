package alloc

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArenaBatchCrossesPreferredBoundaryAndSavesOnce(t *testing.T) {
	a := newArena(7)
	a.cursors = []arenaCursor{{start: 2, end: 4}}
	c := &a.cursors[0]
	c.position.Store(3 | 6<<32)
	batch := a.startBatch()
	for _, span := range []int{3, 4, 5, 6, 0, 1, 2} {
		b, ok := batch.claim(a, spanPages)
		require.True(t, ok)
		require.Equal(t, span*spanPages, b.start)
		require.Equal(t, batch.initial, c.position.Load(), "claims do not publish intermediate cursor positions")
		home, spill := int(uint32(batch.position)), int(batch.position>>32)
		require.True(t, home >= c.start && home < c.end)
		require.True(t, spill < c.start || spill >= c.end)
	}
	_, ok := batch.claim(a, 1)
	require.False(t, ok)
	// A later claim must discover capacity freed behind its physical position.
	a.freePages(2*spanPages, spanPages)
	b, ok := batch.claim(a, spanPages)
	require.True(t, ok)
	require.Equal(t, 2*spanPages, b.start)
	require.True(t, batch.save())
	require.Equal(t, batch.position, c.position.Load())
}

func TestArenaBatchStartsAtRetainedOverflowHint(t *testing.T) {
	a := newArena(7)
	a.cursors = []arenaCursor{{start: 2, end: 4}}
	c := &a.cursors[0]
	c.position.Store(3 | 6<<32)
	a.words[2].Store(uint64(fullSpan))
	a.words[3].Store(uint64(fullSpan))
	batch := a.startBatch()
	b, ok := batch.claim(a, spanPages)
	require.True(t, ok)
	require.Equal(t, 6*spanPages, b.start)
	b, ok = batch.claim(a, spanPages)
	require.True(t, ok)
	require.Equal(t, 0, b.start)
	// Another request can advance the same hint while this batch is in flight.
	c.position.Store(2 | 5<<32)
	require.False(t, batch.save())
	require.Equal(t, uint64(2|5<<32), c.position.Load())
}

func TestArenaCursorDistributionAndProgress(t *testing.T) {
	a := newArena(65)
	for _, count := range []int{1, 2, 4, runtime.GOMAXPROCS(0), 2 * runtime.GOMAXPROCS(0), 4 * runtime.GOMAXPROCS(0), 65} {
		a.initCursors(count)
		require.Len(t, a.cursors, min(count, len(a.words)))
		covered, starts := make([]bool, len(a.words)), make(map[int]bool)
		for i := range a.cursors {
			c := &a.cursors[i]
			home, spill := int(uint32(c.position.Load())), int(c.position.Load()>>32)
			require.GreaterOrEqual(t, home, c.start)
			require.Less(t, home, c.end)
			require.False(t, starts[home], "starting positions must be distinct")
			starts[home] = true
			require.True(t, spill < c.start || spill >= c.end || c.end-c.start == len(a.words))
			for span := c.start; span < c.end; span++ {
				covered[span] = true
			}
		}
		for span, ok := range covered {
			require.True(t, ok, "span %d has no preferred shard", span)
		}
	}
	a.cursors = []arenaCursor{{start: 8, end: 16}}
	a.cursors[0].position.Store(8 | 16<<32)
	seen := make(map[int]bool)
	for i := range len(a.words) {
		b, ok := a.claim(spanPages)
		require.True(t, ok)
		span := b.start / spanPages
		require.False(t, seen[span])
		seen[span] = true
		if i < 8 {
			require.Equal(t, 8+i, span, "consume the preferred shard first")
		}
	}
	_, ok := a.claim(1)
	require.False(t, ok)
	a.freePages(10*spanPages, spanPages)
	b, ok := a.claim(spanPages)
	require.True(t, ok, "a new search finds pages freed in its home shard")
	require.Equal(t, 10*spanPages, b.start)
	require.Equal(t, uint32(11), uint32(a.cursors[0].position.Load()))
}

func TestArenaCursorRetainsPartialSpan(t *testing.T) {
	a := newArena(3)
	a.cursors = []arenaCursor{{end: 3}}
	for page := range spanPages {
		b, ok := a.claim(1)
		require.True(t, ok)
		require.Equal(t, page, b.start)
	}
	require.Equal(t, uint64(1), a.cursors[0].position.Load(), "retain the next span across requests")
	b, ok := a.claim(spanPages)
	require.True(t, ok)
	require.Equal(t, spanPages, b.start)
	require.Equal(t, uint64(2), a.cursors[0].position.Load())
}

func TestArenaConcurrentScansShareOnlyHints(t *testing.T) {
	a := newArena(3)
	a.cursors = []arenaCursor{{end: 3}}
	c := &a.cursors[0]
	initial := c.position.Load()
	b, first, ok := a.claimFrom(c, initial, spanPages)
	require.True(t, ok)
	require.Equal(t, 0, b.start)
	b, second, ok := a.claimFrom(c, initial, spanPages)
	require.True(t, ok)
	require.Equal(t, spanPages, b.start, "sharing a hint cannot share ownership")
	require.True(t, c.save(initial, second))
	require.False(t, c.save(initial, first), "another acquisition published to the same cursor")
	require.Equal(t, uint64(2), a.cursors[0].position.Load(), "a late save must not overwrite another scan's progress")
	b, ok = a.claim(spanPages)
	require.True(t, ok)
	require.Equal(t, 2*spanPages, b.start)
}

func TestArenaReleaseDoesNotChangeCursor(t *testing.T) {
	a := newArena(3)
	a.initCursors(1)
	c := &a.cursors[0]
	initial := c.position.Load()
	b, position, ok := a.claimFrom(c, initial, spanPages)
	require.True(t, ok)
	a.freePages(b.start, b.n)
	require.Equal(t, initial, c.position.Load())
	require.True(t, c.save(initial, position), "release cannot cause a cursor CAS failure")
}

// Exercise every placement of the preferred shard, both wrap points, and every
// possible pair of hints. A single free span must be reachable from all of them.
func TestArenaCursorOverflowCoverage(t *testing.T) {
	const spans = 7
	a := newArena(spans)
	for i := range a.words {
		a.words[i].Store(uint64(fullSpan))
	}
	for start := range spans {
		for end := start + 1; end <= spans; end++ {
			c := arenaCursor{start: start, end: end}
			for home := start; home < end; home++ {
				for spill := range spans {
					if spill >= start && spill < end && end-start != spans {
						continue
					}
					initial := uint64(home) | uint64(spill)<<32
					_, position, ok := a.claimFrom(&c, initial, spanPages)
					require.False(t, ok)
					require.Equal(t, initial, position, "exhaustion preserves both hints")
					for free := range spans {
						a.words[free].Store(uint64(emptySpan))
						b, position, ok := a.claimFrom(&c, initial, spanPages)
						require.True(t, ok, "home=[%d,%d) hints=(%d,%d) free=%d", start, end, home, spill, free)
						require.Equal(t, block{start: free * spanPages, n: spanPages}, b)
						nextHome, nextSpill := int(uint32(position)), int(position>>32)
						require.True(t, nextHome >= start && nextHome < end)
						if end-start != spans {
							require.True(t, nextSpill < start || nextSpill >= end)
							require.Less(t, nextSpill, spans)
						}
					}
				}
			}
		}
	}

	// Overflow retains a partial span, but a new claim still prefers home.
	a.cursors = []arenaCursor{{start: 2, end: 4}}
	c := &a.cursors[0]
	c.position.Store(3 | 6<<32)
	a.words[6].Store(uint64(emptySpan))
	b, ok := a.claim(1)
	require.True(t, ok)
	require.Equal(t, 6*spanPages, b.start)
	require.Equal(t, uint64(3|6<<32), c.position.Load())
	a.words[2].Store(uint64(emptySpan))
	b, ok = a.claim(spanPages)
	require.True(t, ok)
	require.Equal(t, 2*spanPages, b.start)
	require.Equal(t, uint64(3|6<<32), c.position.Load(), "home reuse preserves overflow progress")
}

func TestArenaWholeSpanConcurrentReuse(t *testing.T) {
	a := newArena(4)
	a.initCursors(1)
	owners := make([]atomic.Bool, len(a.words))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 2000 {
				b, ok := a.claim(spanPages)
				if !ok {
					runtime.Gosched()
					continue
				}
				owner := &owners[b.start/spanPages]
				if !owner.CompareAndSwap(false, true) {
					t.Error("span acquired while still owned")
					return
				}
				owner.Store(false)
				a.freePages(b.start, b.n)
			}
		})
	}
	wg.Wait()
	require.Equal(t, len(a.words)*spanPages, a.available())
}

// A single usable node must be found wherever the scan begins, including
// across wraparound and the end of a non-power-of-two arena.
func TestArenaScanFindsOnlyFreeNode(t *testing.T) {
	const spans = 65
	full := uint64(refWord(run(0, spanPages)))
	for n := 1; n <= spanPages; n++ {
		if n <= halfPages && n&(n-1) != 0 {
			continue
		}
		t.Run(fmt.Sprintf("pages%d", n), func(t *testing.T) {
			limit := 1 << levelOf(n)
			a := newArena(spans)
			for i := range a.words {
				a.words[i].Store(full)
			}
			_, ok := a.claim(limit)
			require.False(t, ok)
			start := spanPages - n
			before := uint64(refWord(run(0, spanPages) &^ run(start, n)))
			for i := range a.words {
				a.words[i].Store(before)
				b, ok := a.claim(limit)
				require.True(t, ok, "free node in span %d", i)
				require.Equal(t, block{start: i*spanPages + start, n: n}, b)
				require.Equal(t, full, a.words[i].Load(), "claim reserves the whole free range")
				a.freePages(b.start, b.n)
				require.Equal(t, before, a.words[i].Load())
				a.words[i].Store(full)
			}
			require.Zero(t, a.available())
		})
	}
}

// A whole-span request reserves both trees together.
func TestArenaWholeSpan(t *testing.T) {
	a := newArena(1)
	require.Equal(t, uint64(0xc000000000000000), a.words[0].Load())
	b, full, ok := a.claimNode(0, spanPages)
	require.True(t, ok)
	require.True(t, full)
	require.Equal(t, block{start: 0, n: spanPages}, b)
	require.Equal(t, uint64(0x3fffffffffffffff), a.words[0].Load())
	_, _, ok = a.claimNode(0, 1)
	require.False(t, ok, "the allocation owns every page")
	require.Zero(t, a.available())
	a.freePages(b.start, b.n)
	require.Equal(t, uint64(3)<<62, a.words[0].Load())
	require.Equal(t, spanPages, a.available())
}
