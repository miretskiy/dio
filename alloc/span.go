package alloc

import (
	"context"
	"errors"
	"math/bits"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/miretskiy/dio/v2/align"
)

// ErrTooLarge means a request exceeds the largest supported allocation.
var ErrTooLarge = errors.New("alloc: allocation too large")

// SpanAllocator lends variable-size, page-aligned, contiguous memory from one
// region. Small requests claim a slot with a single CAS.
//
// The region is divided into 256 KiB spans of 64 pages. A request of up to
// 128 KiB is rounded up to a size class and served from a span assigned to
// that class, cut into equal slots. A larger request takes up to 64 whole
// contiguous spans. Spans are assigned to classes on demand and returned when
// empty:
//
//	region:  span 0    span 1    span 2              span n-2  span n-1
//	        +---------+---------+---------+--   --+---------+---------+
//	        | class 0 | class 5 |  free   |  ...  |   large run (2)   |
//	        +---------+---------+---------+--   --+---------+---------+
//	         class spans: lowest first   -->   <--   large runs: from the top
//
//	span 1 (class 5: 8-page slots, 8 per span):
//	        page 0    8         16        24                  56       63
//	        +---------+---------+---------+--   --+---------+
//	        | slot 0  | slot 1  | slot 2  |  ...  | slot 7  |
//	        +---------+---------+---------+--   --+---------+
//
// Each span has a 64-bit slot word, kept outside the region. All ones marks a
// span that is free or part of a large run:
//
//	+-------+------------------------------------------+
//	| class | one bit per slot: 1 = taken (or unused)  |
//	+-------+------------------------------------------+
//	 4 bits   60 bits
//
// Region-wide bitmaps have one bit per span, 64 spans per word. They change
// when a span changes state and are scanned in searches, so they are packed
// densely:
//
//	inUse                the span is in use (in a class or a large run)
//	nonFull (per class)  the span is in this class and has a free slot
//
// Acquiring a slot of a class tries, in order:
//  1. one of the class's active spans, cached spans picked per goroutine;
//  2. the lowest span in nonFull;
//  3. the lowest free span, which joins the class.
type SpanAllocator struct {
	region        []byte  // whole spans
	mapped        []byte  // the region, when the allocator mapped it
	leases        *leases // one unit per page; an allocation uses its first page
	spans         []spanState
	inUse         bitmap // per span: in a class or a large run
	classes       [len(spanClassPages)]sizeClass
	largeHints    []largeHint // per goroutine slot: inUse word to search first
	retryInterval time.Duration
	noZero        bool
}

// SpanSize is a SpanAllocator's span: regions are used in whole spans, and
// requests above 128 KiB round up to whole spans.
const SpanSize = spanBytes

const (
	spanPages     = 64
	spanBytes     = spanPages * align.BlockSize
	maxSmallPages = 32
	maxLargeSpans = 64 // A run spans at most two inUse words.
	tagShift      = 60
	tagMask       = uint64(0xF) << tagShift
	slotMask      = ^tagMask
)

// spanClassPages lists slot sizes in pages, chosen so slots fill a span:
var spanClassPages = [...]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32}

// Class tags are 4 bits, and the all-ones tag marks spans outside any class.
const _ = uint(15 - len(spanClassPages))

// pagesToClass maps a page count of 1..32 to the smallest class that fits.
var pagesToClass = func() (t [maxSmallPages + 1]uint8) {
	c := 0
	for p := 1; p <= maxSmallPages; p++ {
		for spanClassPages[c] < p {
			c++
		}
		t[p] = uint8(c)
	}
	return t
}()

// bitmap is a dense atomic bitmap, 64 bits per word.
type bitmap []atomic.Uint64

func newBitmap(bits int) bitmap { return make(bitmap, (bits+63)/64) }

func (b bitmap) has(i int) bool { return b[i/64].Load()&(1<<(i%64)) != 0 }

// set sets bit i and reports whether its word was zero before.
func (b bitmap) set(i int) (wasZero bool) { return b[i/64].Or(1<<(i%64)) == 0 }

// clear clears bit i and reports whether its word is now zero.
func (b bitmap) clear(i int) (nowZero bool) {
	m := uint64(1) << (i % 64)
	return b[i/64].And(^m)&^m == 0
}

// wordHas reports whether the word holding bit i has any bit set.
func (b bitmap) wordHas(i int) bool { return b[i/64].Load() != 0 }

// spanState fills a cache line: concurrent acquisitions and releases on
// different spans write their slot words independently.
type spanState struct {
	slots atomic.Uint64
	held  atomic.Int32 // active entries caching this span
	_     [52]byte
}

// largeHint remembers the inUse word where a goroutine slot last claimed a
// large run. It fills a cache line for the same reason as activeSpan.
type largeHint struct {
	word atomic.Int32
	_    [60]byte
}

// activeSpan caches a span that acquisitions of its class try first. Each
// class has GOMAXPROCS entries, the most goroutines that can run at once,
// spreading concurrent acquisitions across spans. An acquisition picks its
// entry with entryIndex. An entry fills a cache line so replacing it leaves the
// others cached in every core. Unmeasured: entries change only when a span fills.
type activeSpan struct {
	span atomic.Int32 // span index + 1; zero means none
	_    [60]byte
}

type sizeClass struct {
	pages        int
	tag          uint64
	empty        uint64 // slot word with every slot free
	nonFull      bitmap // per span: in this class, with a free slot; see clearNonFull
	nonFullIndex bitmap // per nonFull word: has a bit set; bounds the search
	active       []activeSpan
}

// NewSpanAllocator creates an allocator over size bytes rounded up to whole
// 256 KiB spans, mapped during construction. WithBacking instead supplies a
// page-aligned region of at least one span, in which case size is ignored.
// WithWarmup commits all pages.
func NewSpanAllocator(size int64, opts ...Option) (*SpanAllocator, error) {
	cfg := allocatorConfig{retryInterval: 100 * time.Millisecond}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.chunkSize != 0 {
		return nil, errors.New("alloc: WithChunkSize requires NewSlabAllocator")
	}
	var mapped []byte
	region := cfg.backing
	if region == nil {
		// Round size up to whole spans: ceil(size / spanBytes) * spanBytes.
		mapped = align.AllocAligned((size + spanBytes - 1) / spanBytes * spanBytes)
		region = mapped
	} else if len(region) < spanBytes || !align.IsAligned(region) {
		return nil, errors.New("alloc: backing must be page-aligned and at least 256 KiB")
	}
	n := len(region) / spanBytes
	region = region[: n*spanBytes : n*spanBytes]
	if cfg.warmup {
		warmup(region, align.BlockSize)
	}
	a := &SpanAllocator{
		region:        region,
		mapped:        mapped,
		spans:         make([]spanState, n),
		inUse:         newBitmap(n),
		largeHints:    make([]largeHint, runtime.GOMAXPROCS(0)),
		retryInterval: cfg.retryInterval,
		noZero:        cfg.noZero,
	}
	for i := range a.spans {
		a.spans[i].slots.Store(^uint64(0))
	}
	// Spread first searches over the top words, one per goroutine slot.
	for i := range a.largeHints {
		a.largeHints[i].word.Store(int32(len(a.inUse) - 1 - i%len(a.inUse)))
	}
	// inUse holds 64 spans per word. Mark bits past the last span as in use
	// so claimSpans never returns a span that does not exist.
	if tail := n % 64; tail != 0 {
		a.inUse[len(a.inUse)-1].Store(^uint64(0) << tail)
	}
	for c := range a.classes {
		cl := &a.classes[c]
		cl.pages = spanClassPages[c]
		cl.tag = uint64(c) << tagShift
		cl.empty = cl.tag | slotMask&(^uint64(0)<<min(tagShift, spanPages/cl.pages))
		cl.nonFull = newBitmap(n)
		cl.nonFullIndex = newBitmap(len(cl.nonFull))
		cl.active = make([]activeSpan, runtime.GOMAXPROCS(0))
	}
	a.leases = newLeases(n*spanPages, func(page int, data []byte) {
		a.release(page, len(data)/align.BlockSize)
	}, cfg.releaseHook)
	return a, nil
}

// TryAcquire returns memory of at least size bytes, rounded up to the size
// class or to whole spans, zeroed unless built WithoutZeroing. It returns
// ErrExhausted instead of waiting, and ErrTooLarge above MaxSize. Size must be
// positive.
func (a *SpanAllocator) TryAcquire(size int64) (Memory, error) {
	if size > a.MaxSize() {
		return Memory{}, ErrTooLarge
	}
	pages := int((size + align.BlockSize - 1) / align.BlockSize)
	page, n, ok := a.tryAcquire(pages)
	if !ok {
		return Memory{}, ErrExhausted
	}
	off := page * align.BlockSize
	data := a.region[off : off+n*align.BlockSize]
	if !a.noZero {
		clear(data)
	}
	return a.leases.lease(page, data), nil
}

// Acquire tries immediately, then retries every 100 ms (or WithRetryInterval)
// when exhausted. Context cancellation interrupts the wait.
func (a *SpanAllocator) Acquire(ctx context.Context, size int64) (Memory, error) {
	if m, err := a.TryAcquire(size); err != ErrExhausted {
		return m, err
	}
	timer := time.NewTimer(a.retryInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return Memory{}, ctx.Err()
		case <-timer.C:
			if m, err := a.TryAcquire(size); err != ErrExhausted {
				return m, err
			}
			timer.Reset(a.retryInterval)
		}
	}
}

// Capacity reports the bytes in whole spans. It must not race Close.
func (a *SpanAllocator) Capacity() int64 { return int64(len(a.spans)) * spanBytes }

// MaxSize reports the largest request the allocator can ever serve: 64 spans
// (16 MiB), or Capacity if smaller. It needs that many contiguous free spans,
// so fragmentation can keep such a request waiting.
func (a *SpanAllocator) MaxSize() int64 {
	return int64(min(maxLargeSpans, len(a.spans))) * spanBytes
}

// Available reports a snapshot of free bytes: unassigned and idle spans plus
// free slots in other class spans. It scans all spans and must not race Close.
func (a *SpanAllocator) Available() int64 {
	var pages int
	for i := range a.spans {
		w := a.spans[i].slots.Load()
		if w == ^uint64(0) {
			if !a.inUse.has(i) {
				pages += spanPages
			}
			continue
		}
		cl := &a.classes[w>>tagShift]
		if w == cl.empty { // Idle: reclaimable as a whole span.
			pages += spanPages
			continue
		}
		pages += bits.OnesCount64(^w&slotMask) * cl.pages
	}
	return int64(pages) * align.BlockSize
}

// Close unmaps the region if the allocator mapped it. It must not race other
// use; the caller must release borrowed memory first. Repeated closes are harmless.
func (a *SpanAllocator) Close() {
	if a.mapped != nil {
		align.FreeAligned(a.mapped)
	}
	a.region, a.mapped = nil, nil
}

// tryAcquire returns the first page and page count of a new allocation.
func (a *SpanAllocator) tryAcquire(pages int) (page, n int, ok bool) {
	if pages <= maxSmallPages {
		c := int(pagesToClass[pages])
		page, ok = a.acquireSlot(c)
		return page, a.classes[c].pages, ok
	}
	k := (pages + spanPages - 1) / spanPages
	s, ok := a.acquireSpans(k, true)
	return s * spanPages, k * spanPages, ok
}

// release frees n pages from page; n is the class or run size tryAcquire returned.
func (a *SpanAllocator) release(page, n int) {
	if n <= maxSmallPages {
		a.releaseSlot(page)
		return
	}
	a.releaseSpans(page/spanPages, n/spanPages)
}

// releaseSpans frees the run of k spans from s, which may continue into the
// next inUse word. runMask drops the bits above the first word.
func (a *SpanAllocator) releaseSpans(s, k int) {
	a.inUse[s/64].And(^runMask(k, s%64))
	if over := s%64 + k - 64; over > 0 {
		a.inUse[s/64+1].And(^runMask(over, 0))
	}
}

// acquireSlot claims a slot of class c: active span, then non-full span, then
// fresh span. When the active span's CAS loses a race, it takes a fresh span
// directly, so contended entries move to separate spans. When inUse is full,
// it retries contended non-full spans until it claims a slot.
func (a *SpanAllocator) acquireSlot(c int) (int, bool) {
	cl := &a.classes[c]
	entry := &cl.active[entryIndex(len(cl.active))]
	busy := false
	if s := int(entry.span.Load()) - 1; s >= 0 {
		page, ok, contended := a.claimSlot(s, c, false)
		if ok {
			return page, true
		}
		busy = contended
	}
	if !busy {
		if page, ok := a.claimNonFull(entry, c, false); ok {
			return page, true
		}
	}
	if page, ok := a.claimFresh(entry, c); ok {
		return page, true
	}
	// No free span: wait out contention on any non-full span.
	return a.claimNonFull(entry, c, true)
}

// claimNonFull claims a slot in a span with its nonFull bit set, lowest first.
// Without persist, it skips spans whose CAS loses a race.
func (a *SpanAllocator) claimNonFull(entry *activeSpan, c int, persist bool) (int, bool) {
	cl := &a.classes[c]
	for si := range cl.nonFullIndex {
		for sum := cl.nonFullIndex[si].Load(); sum != 0; sum &= sum - 1 {
			w := si*64 + bits.TrailingZeros64(sum)
			for hint := cl.nonFull[w].Load(); hint != 0; hint &= hint - 1 {
				s := w*64 + bits.TrailingZeros64(hint)
				if page, ok, _ := a.claimSlot(s, c, persist); ok {
					a.setActive(entry, s)
					return page, true
				}
			}
		}
	}
	return 0, false
}

// claimFresh assigns a free span to class c, claims its first slot, and makes
// it entry's span.
func (a *SpanAllocator) claimFresh(entry *activeSpan, c int) (int, bool) {
	cl := &a.classes[c]
	s, ok := a.acquireSpans(1, false)
	if !ok {
		return 0, false
	}
	// Publishing the slot word makes the span visible to claimers of class c.
	a.spans[s].slots.Store(cl.empty | 1)
	if cl.empty|1|tagMask != ^uint64(0) {
		a.setNonFull(c, s)
	}
	a.setActive(entry, s)
	return s * spanPages, true
}

// claimSlot claims a free slot in span s if it belongs to class c. The CAS
// compares the class bits too, so it fails once the span has moved to another
// class. Without persist, a lost CAS race returns contended instead of retrying.
func (a *SpanAllocator) claimSlot(s, c int, persist bool) (page int, ok, contended bool) {
	cl := &a.classes[c]
	word := &a.spans[s].slots
	for {
		w := word.Load()
		free := ^w & slotMask
		if w&tagMask != cl.tag || free == 0 {
			a.clearNonFull(c, s)
			return 0, false, contended
		}
		i := bits.TrailingZeros64(free)
		bit := uint64(1) << i
		if word.CompareAndSwap(w, w|bit) {
			if free == bit {
				a.clearNonFull(c, s)
			}
			return s*spanPages + i*cl.pages, true, contended
		}
		contended = true
		if !persist {
			return 0, false, true
		}
	}
}

// releaseSlot frees the slot starting at page. The caller owns the slot, so
// the span's class is stable until the slot bit is cleared.
func (a *SpanAllocator) releaseSlot(page int) {
	s := page / spanPages
	word := &a.spans[s].slots
	c := int(word.Load() >> tagShift)
	cl := &a.classes[c]
	bit := uint64(1) << (page % spanPages / cl.pages)
	old := word.And(^bit)
	if old|tagMask == ^uint64(0) {
		a.setNonFull(c, s)
	}
	if old&^bit == cl.empty {
		a.retireIfIdle(s)
	}
}

// retireIfIdle returns span s to inUse if it is empty in its current class and
// no active entry holds it. The CAS fails if a slot was claimed meanwhile.
func (a *SpanAllocator) retireIfIdle(s int) bool {
	sp := &a.spans[s]
	if sp.held.Load() != 0 {
		return false
	}
	w := sp.slots.Load()
	c := int(w >> tagShift)
	if c >= len(a.classes) || w != a.classes[c].empty || !sp.slots.CompareAndSwap(w, ^uint64(0)) {
		return false
	}
	a.clearNonFullBit(c, s)
	a.inUse.clear(s)
	return true
}

// setActive makes s entry's span. The caller holds a slot in s, so s cannot be
// retired meanwhile. The span it replaces may have emptied while the entry
// held it.
func (a *SpanAllocator) setActive(entry *activeSpan, s int) {
	old := int(entry.span.Swap(int32(s+1))) - 1
	if old == s {
		return
	}
	a.spans[s].held.Add(1)
	if old >= 0 && a.spans[old].held.Add(-1) == 0 {
		a.retireIfIdle(old)
	}
}

// entryIndex picks an active entry from the address of a stack variable. Each
// goroutine has its own stack, so concurrent callers spread across entries,
// and a goroutine keeps returning to the same entry, span, and cache lines.
func entryIndex(n int) int {
	var local byte
	h := uint64(uintptr(unsafe.Pointer(&local))) * 0x9E3779B97F4A7C15
	return int(h >> 32 % uint64(n))
}

// reclaimIdle releases active entries holding empty spans, or spans that have
// moved to another class, and retires those spans. It runs only when inUse has
// no room, and reports whether any span was retired.
func (a *SpanAllocator) reclaimIdle() bool {
	retired := false
	for c := range a.classes {
		cl := &a.classes[c]
		for i := range cl.active {
			entry := &cl.active[i].span
			s := int(entry.Load()) - 1
			if s < 0 {
				continue
			}
			w := a.spans[s].slots.Load()
			if w&tagMask == cl.tag && w != cl.empty || !entry.CompareAndSwap(int32(s+1), 0) {
				continue
			}
			if a.spans[s].held.Add(-1) == 0 {
				retired = a.retireIfIdle(s) || retired
			}
		}
	}
	return retired
}

// setNonFull sets span s's nonFull bit, and its word's nonFullIndex bit when
// the word was empty.
func (a *SpanAllocator) setNonFull(c, s int) {
	cl := &a.classes[c]
	if !cl.nonFull.has(s) && cl.nonFull.set(s) {
		cl.nonFullIndex.set(s / 64)
	}
}

// clearNonFull clears the hint, then re-checks the span: a concurrent release
// either sees the cleared bit and sets it again, or this re-check sees its slot.
func (a *SpanAllocator) clearNonFull(c, s int) {
	if !a.clearNonFullBit(c, s) {
		return
	}
	if w := a.spans[s].slots.Load(); w&tagMask == a.classes[c].tag && w|tagMask != ^uint64(0) {
		a.setNonFull(c, s)
	}
}

// clearNonFullBit clears span s's nonFull bit and reports whether it was set.
// When the nonFull word becomes zero, its nonFullIndex bit is cleared and
// re-checked the same way: a concurrent setter either sees a zero word and
// sets the index bit, or this re-check sees its bit.
func (a *SpanAllocator) clearNonFullBit(c, s int) bool {
	cl := &a.classes[c]
	if !cl.nonFull.has(s) {
		return false
	}
	if cl.nonFull.clear(s) {
		cl.nonFullIndex.clear(s / 64)
		if cl.nonFull.wordHas(s) {
			cl.nonFullIndex.set(s / 64)
		}
	}
	return true
}

// acquireSpans claims k contiguous spans, reclaiming idle cached spans once if
// the bitmap has no room.
func (a *SpanAllocator) acquireSpans(k int, top bool) (int, bool) {
	if s := a.claimSpans(k, top); s >= 0 {
		return s, true
	}
	if a.reclaimIdle() {
		if s := a.claimSpans(k, top); s >= 0 {
			return s, true
		}
	}
	return 0, false
}

// claimSpans claims k <= 64 contiguous free spans. Class spans search upward
// from the bottom; large runs search downward from the goroutine's hint,
// wrapping around. Each inUse word is tried alone, then with the next word.
func (a *SpanAllocator) claimSpans(k int, top bool) int {
	n := len(a.inUse)
	var hint *largeHint
	start := 0
	if top {
		// Start where this goroutine's last large claim succeeded: concurrent
		// claims stay on separate words, and a word with room is found first.
		hint = &a.largeHints[entryIndex(len(a.largeHints))]
		start = int(hint.word.Load())
	}
	for j := range n {
		i := start + j
		if top {
			if i = start - j; i < 0 {
				i += n
			}
		}
		s := -1
		for {
			w := a.inUse[i].Load()
			starts := runStarts(^w, k)
			if starts == 0 {
				break
			}
			bit := bits.TrailingZeros64(starts)
			if top {
				bit = bits.Len64(starts) - 1
			}
			if a.inUse[i].CompareAndSwap(w, w|runMask(k, bit)) {
				s = i*64 + bit
				break
			}
		}
		// A run across words needs the top span of word i free.
		if s < 0 && k > 1 && i+1 < n && a.inUse[i].Load()>>63 == 0 {
			s = a.claimAcross(i, k)
		}
		if s >= 0 {
			if hint != nil && i != start {
				hint.word.Store(int32(i))
			}
			return s
		}
	}
	return -1
}

// claimAcross claims k spans that cross from the top of inUse word i into the
// bottom of word i+1, taking as many as fit from word i+1:
//
//	word i:   ...used | below free     word i+1:   above free | used...
//
// It reserves word i first, then word i+1, and clears its word i bits if word
// i+1 was taken meanwhile. Reserved bits stay unavailable to other claims
// until then, so the run is returned only when both words are held.
func (a *SpanAllocator) claimAcross(i, k int) int {
	lo, hi := &a.inUse[i], &a.inUse[i+1]
	for {
		w := lo.Load()
		above := min(bits.TrailingZeros64(hi.Load()), k-1)
		below := k - above
		if above == 0 || below > bits.LeadingZeros64(w) {
			return -1
		}
		lowMask, highMask := runMask(below, 64-below), runMask(above, 0)
		if !lo.CompareAndSwap(w, w|lowMask) {
			continue
		}
		for {
			v := hi.Load()
			if v&highMask != 0 {
				lo.And(^lowMask)
				break
			}
			if hi.CompareAndSwap(v, v|highMask) {
				return i*64 + 64 - below
			}
		}
	}
}

// runStarts returns a mask whose bit i is set iff bits i..i+k-1 of free are set.
func runStarts(free uint64, k int) uint64 {
	for have := 1; have < k; {
		step := min(have, k-have)
		free &= free >> step
		have += step
	}
	return free
}

// runMask returns k set bits starting at bit start, for 1 <= k <= 64.
func runMask(k, start int) uint64 { return ^uint64(0) >> (64 - k) << start }
