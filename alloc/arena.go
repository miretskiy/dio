package alloc

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// arena owns one contiguous region, one buddy-tree word per span, and lease
// state. A claim fits within one span; a lease may combine adjacent claims.
// Cursors spread searches across the arena without reserving any capacity:
// every search can use every span, and only the span-word CAS grants ownership.
type arena struct {
	region  []byte // whole spans
	words   []atomic.Uint64
	free    atomic.Int64 // free pages, updated with each successful claim/release
	leases  *leases      // one unit per page; an allocation uses its first page
	cursors []arenaCursor
}

// Hints in the same preferred shard share its bounds, but advance independently.
type arenaCursor struct {
	position   atomic.Uint64 // low 32 bits: home position; high 32 bits: overflow position
	start, end int           // immutable preferred half-open range of spans
}

// arenaBatch keeps a private search position for one Buffer growth. The first
// claim uses both shared hints; subsequent claims walk physical neighbors across
// shard boundaries. Only save publishes progress, once at the end of the batch.
type arenaBatch struct {
	cursor            *arenaCursor
	initial, position uint64
	next              int // -1 until the first claim
}

func (a *arena) startBatch() arenaBatch {
	c := &a.cursors[entryIndex(len(a.cursors))]
	position := c.position.Load()
	return arenaBatch{cursor: c, initial: position, position: position, next: -1}
}

func (s *arenaBatch) save() bool { return s.cursor.save(s.initial, s.position) }

func (s *arenaBatch) claim(a *arena, n int) (block, bool) {
	if s.next < 0 {
		b, position, ok := a.claimFrom(s.cursor, s.position, n)
		if ok {
			s.position = position
			s.next = (b.start + b.n) / spanPages
			if s.next == len(a.words) {
				s.next = 0
			}
		}
		return b, ok
	}
	// Each claim can still search the whole arena, including spans freed since
	// earlier claims. A full preferred shard is not retried between neighbors.
	for left := len(a.words); left > 0; left-- {
		i := s.next
		b, full, ok := a.claimNode(i, n)
		if full {
			s.next++
			if s.next == len(a.words) {
				s.next = 0
			}
		}
		if ok {
			s.record(i, full, len(a.words))
			b.start += i * spanPages
			return b, true
		}
	}
	return block{}, false
}

// record keeps the shared hints in their respective ranges even though the
// batch's physical walk can cross either boundary.
func (s *arenaBatch) record(i int, full bool, spans int) {
	c := s.cursor
	if i >= c.start && i < c.end {
		if full {
			i++
			if i == c.end {
				i = c.start
			}
		}
		s.position = s.position&0xffffffff00000000 | uint64(i)
	} else {
		if full {
			i++
			if i == c.start {
				i = c.end
			}
			if i == spans {
				i = 0
				if c.start == 0 {
					i = c.end
				}
			}
		}
		s.position = uint64(uint32(s.position)) | uint64(i)<<32
	}
}

// newArena creates metadata for spans. The caller sets region.
func newArena(spans int) *arena {
	a := &arena{words: make([]atomic.Uint64, spans)}
	a.free.Store(int64(spans * spanPages))
	for i := range a.words {
		a.words[i].Store(uint64(emptySpan))
	}
	a.leases = newLeases(spans*spanPages, a)
	a.initCursors(spans)
	return a
}

// initCursors spreads starting positions within GOMAXPROCS preferred shards.
// Call only while the arena is unused.
func (a *arena) initCursors(count int) {
	spans := len(a.words)
	count = min(count, spans)
	shards := min(count, runtime.GOMAXPROCS(0))
	a.cursors = make([]arenaCursor, count)
	for shard := range shards {
		start, end := shard*spans/shards, (shard+1)*spans/shards
		first, last := shard*count/shards, (shard+1)*count/shards
		for i := first; i < last; i++ {
			c := &a.cursors[i]
			c.start, c.end = start, end
			next := start + (i-first)*(end-start)/(last-first)
			c.position.Store(uint64(next) | uint64(end%spans)<<32)
		}
	}
}

// save publishes progress once per request. False means another scan changed
// this cursor, so its progress is retained. No update is needed if we stayed put.
func (c *arenaCursor) save(initial, position uint64) bool {
	return position == initial || c.position.CompareAndSwap(initial, position)
}

// claim takes the largest eligible block of at most n pages in the first nonfull
// span encountered; n must be a size class (1, 2, 4, 8, 16, or 32).
// The returned block is relative to the arena and may contain fewer than n pages.
func (a *arena) claim(n int) (block, bool) {
	c := &a.cursors[entryIndex(len(a.cursors))]
	initial := c.position.Load()
	b, position, ok := a.claimFrom(c, initial, n)
	c.save(initial, position)
	return b, ok
}

// claimFrom returns one claim and the updated cursor position. Search the
// preferred shard first, then the rest of the arena, visiting each span at most
// once. Keep a nonfull span as the next hint. Each request checks its home shard
// again so locally freed capacity is reused; the overflow hint survives too.
func (a *arena) claimFrom(c *arenaCursor, position uint64, n int) (block, uint64, bool) {
	next, spill := int(uint32(position)), int(position>>32)
	for left := c.end - c.start; left > 0; left-- {
		i := next
		b, full, ok := a.claimNode(i, n)
		if full {
			next++
			if next == c.end {
				next = c.start
			}
		}
		if ok {
			b.start += i * spanPages
			return b, uint64(next) | uint64(spill)<<32, true
		}
	}
	for left := len(a.words) - (c.end - c.start); left > 0; left-- {
		i := spill
		b, full, ok := a.claimNode(i, n)
		if full {
			spill++
			if spill == c.start {
				spill = c.end
			}
			if spill == len(a.words) {
				spill = 0
				if c.start == 0 {
					spill = c.end
				}
			}
		}
		if ok {
			b.start += i * spanPages
			return b, uint64(next) | uint64(spill)<<32, true
		}
	}
	return block{}, position, false
}

// entryIndex selects a cursor, never a fresh span position. It is usually
// stable at one call site, but stack movement can change it; it is not a CPU
// identifier. Progress lives in the atomic cursor even when callers change.
func entryIndex(n int) int {
	var local byte
	h := uint64(uintptr(unsafe.Pointer(&local))) * 0x9E3779B97F4A7C15
	return int(h >> 32 * uint64(n) >> 32)
}

// claimNode reserves the largest eligible contiguous block of at most n pages in span i.
// n must be a size class; the returned block is relative to the span. full
// reports the resulting span state and is also true when the claim fails.
func (a *arena) claimNode(i, n int) (b block, full, ok bool) {
	word := &a.words[i]
	for {
		w := spanWord(word.Load())
		// Full-span scans need no buddy-tree work or call to reserve.
		if w == fullSpan {
			return block{}, true, false
		}
		next, b, ok := w.reserve(n)
		if !ok {
			return block{}, true, false
		}
		if word.CompareAndSwap(uint64(w), uint64(next)) {
			a.free.Add(-int64(b.n))
			return b, next == fullSpan, true
		}
	}
}

// freeSpan returns one span's portion of a lease. Partial releases must preserve
// other owners' pages; a whole-span release owns every bit and needs only a store.
func (a *arena) freeSpan(i, start, n int) {
	word := &a.words[i]
	if n == spanPages {
		word.Store(uint64(emptySpan))
		a.free.Add(int64(n))
		return
	}
	b := block{start: start, n: n}
	for {
		w := spanWord(word.Load())
		if word.CompareAndSwap(uint64(w), uint64(w.release(b))) {
			a.free.Add(int64(n))
			return
		}
	}
}

// freePages returns a lease, which may combine claims across span boundaries.
func (a *arena) freePages(page, n int) {
	for n > 0 {
		start := page % spanPages
		pages := min(n, spanPages-start)
		a.freeSpan(page/spanPages, start, pages)
		page += pages
		n -= pages
	}
}

// available returns a snapshot of the free pages.
func (a *arena) available() int {
	// A claim/release updates its span word before the counter. Concurrent
	// snapshots may briefly include one side of those updates; keep them in range.
	return int(max(0, min(int64(len(a.words)*spanPages), a.free.Load())))
}
