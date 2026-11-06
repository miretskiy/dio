package alloc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"
)

func run(start, n int) uint64 { return (uint64(1)<<n - 1) << start }

// refWord builds the specified two-tree encoding from page occupancy, without
// using production node masks or tree operations. A node is set if any leaf
// in its range is occupied. Enumerate page ranges breadth-first, from root
// to leaves, independently of the production claim masks and heap arithmetic.
func refWord(pages uint64) spanWord {
	var w uint64
	for half := range 2 {
		bit := half * 31
		for block := 16; block >= 1; block /= 2 {
			for start := 0; start < 16; start += block {
				occupied := false
				for page := start; page < start+block; page++ {
					occupied = occupied || pages&(1<<(half*16+page)) != 0
				}
				if occupied {
					w |= 1 << bit
				}
				bit++
			}
		}
		if pages&(0xffff<<(half*16)) == 0 {
			w |= 1 << (62 + half)
		}
	}
	return spanWord(w)
}

// refFit searches largest-first in a plain page bitmap, independently of the
// production masks and bit scans. Above 16 pages, try every length and start;
// smaller allocations retain buddy sizes and alignment.
func refFit(pages uint64, n int) (block, bool) {
	if n > halfPages {
		for size := n; size > halfPages; size-- {
			for start := 0; start+size <= spanPages; start++ {
				if pages&run(start, size) == 0 {
					return block{start: start, n: size}, true
				}
			}
		}
		n = halfPages
	}
	for ; n >= 1; n /= 2 {
		for start := 0; start < spanPages; start += n {
			if pages&run(start, n) == 0 {
				return block{start: start, n: n}, true
			}
		}
	}
	return block{}, false
}

func TestSpanWordSingleOccupiedPage(t *testing.T) {
	for page := range spanPages {
		t.Run(fmt.Sprint(page), func(t *testing.T) {
			w := refWord(1 << page)
			next, b, ok := w.reserve(spanPages)
			require.True(t, ok)
			want := block{start: page + 1, n: spanPages - page - 1}
			if page >= halfPages {
				want = block{start: 0, n: page}
			}
			require.Equal(t, want, b)
			require.Equal(t, refWord(1<<page|run(b.start, b.n)), next)
			require.Equal(t, spanPages-1-b.n, next.freePages())
			require.Equal(t, w, next.release(b))
		})
	}
}

// Exercise every possible large range, including free space on both sides.
// Release neighbors before the large block as well as after it: coalescing
// must use the current word, not the occupancy when the block was reserved.
func TestSpanWordLargeRangeRelease(t *testing.T) {
	for n := halfPages + 1; n <= spanPages; n++ {
		for start := 0; start+n <= spanPages; start++ {
			t.Run(fmt.Sprintf("start%d/pages%d", start, n), func(t *testing.T) {
				occupied := run(0, spanPages) &^ run(start, n)
				before := refWord(occupied)
				next, b, ok := before.reserve(spanPages)
				require.True(t, ok)
				require.Equal(t, block{start: start, n: n}, b)
				require.Equal(t, fullSpan, next)
				require.Equal(t, before, next.release(b))

				for page := range spanPages {
					if occupied&(1<<page) == 0 {
						continue
					}
					next = next.release(block{start: page, n: 1})
					occupied &^= 1 << page
					require.Equal(t, refWord(occupied|run(start, n)), next)
					require.Equal(t, refWord(occupied), next.release(b))
				}
				require.Equal(t, emptySpan, next.release(b))
			})
		}
	}
}

// Every large extent with every possible occupancy of all outside pages.
// This also models neighbors changing between reservation and release.
func TestSpanWordLargeExtentsExhaustive(t *testing.T) {
	checked := 0
	for n := halfPages + 1; n <= spanPages; n++ {
		for start := 0; start+n <= spanPages; start++ {
			extent := run(start, n)
			outside := run(0, spanPages) &^ extent
			// Enumerate every subset of the outside pages.
			for occupied := outside; ; occupied = (occupied - 1) & outside {
				before := refWord(occupied)
				want := refWord(occupied | extent)
				next, b := before.reserveLarge(start, n)
				if next != want || b != (block{start: start, n: n}) {
					t.Fatalf("reserveLarge(%032b, %d, %d) = %064b, %+v; want %064b", occupied, start, n, next, b, want)
				}
				if got := next.release(b); got != before {
					t.Fatalf("release(%032b, %+v) = %064b; want %064b", occupied|extent, b, got, before)
				}
				checked++
				if occupied == 0 {
					break
				}
			}
		}
	}
	if checked != 983_041 {
		t.Fatalf("checked %d cases, want 983041", checked)
	}
}

// Merging can produce an interval that is neither a buddy block nor a half
// edge. Check every occupied interval against every outside pattern in both
// halves, including independent retained neighbors on both sides.
func TestSpanWordReleaseMergedRanges(t *testing.T) {
	for half := range 2 {
		for n := 1; n <= halfPages; n++ {
			for start := 0; start+n <= halfPages; start++ {
				mask := run(start, n)
				outside := run(0, halfPages) &^ mask
				for occupied := outside; ; occupied = (occupied - 1) & outside {
					remaining := occupied<<(half*halfPages) | uint64(0xa55a)<<((1-half)*halfPages)
					b := block{start: half*halfPages + start, n: n}
					before := refWord(remaining | run(b.start, b.n))
					if got, want := before.release(b), refWord(remaining); got != want {
						t.Fatalf("release(%032b, %+v) = %064b, want %064b", remaining, b, got, want)
					}
					if occupied == 0 {
						break
					}
				}
			}
		}
	}
	for start := range spanPages {
		for n := 1; start+n <= spanPages; n++ {
			b := block{start: start, n: n}
			outside := run(0, spanPages) &^ run(start, n)
			for _, pattern := range []uint64{0, outside, outside & 0xaaaaaaaa, outside & 0x55555555} {
				require.Equal(t, refWord(pattern), refWord(pattern|run(start, n)).release(b))
			}
		}
	}
}

func TestSpanWordLargeRequestFallback(t *testing.T) {
	for _, test := range []struct {
		name  string
		pages uint64
		want  block
	}{
		{"A-full", run(0, 16), block{16, 16}},
		{"B-full", run(16, 16), block{0, 16}},
		{"boundary-occupied", run(15, 2), block{0, 8}},
		{"separate-free-ends", run(8, 16), block{0, 8}},
		{"crossing-only-16", run(0, 8) | run(24, 8), block{8, 8}},
		{"same-size-prefers-A", run(0, 14) | run(18, 14), block{14, 2}},
		{"alternating", 0xaaaaaaaa, block{0, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := refWord(test.pages)
			next, b, ok := w.reserve(spanPages)
			require.True(t, ok)
			require.Equal(t, test.want, b)
			require.Equal(t, refWord(test.pages|run(b.start, b.n)), next)
			require.Equal(t, w, next.release(b))
		})
	}
}

// Every half state, beside an empty, full, identical, or complementary half.
// Empty flags must not affect free-page accounting.
func TestSpanWordReserve(t *testing.T) {
	for p := range uint64(1 << halfPages) {
		for _, other := range []uint64{0, 0xFFFF, p, p ^ 0xFFFF} {
			for _, pages := range []uint64{p | other<<halfPages, other | p<<halfPages} {
				w := refWord(pages)
				if got, want := w.freePages(), spanPages-bits.OnesCount64(pages); got != want {
					t.Fatalf("freePages(%032b) = %d, want %d", pages, got, want)
				}
				for n := 1; n <= spanPages; n *= 2 {
					next, b, ok := w.reserve(n)
					wantBlock, wantOK := refFit(pages, n)
					if b != wantBlock || ok != wantOK {
						t.Fatalf("reserve(%032b, %d) = %+v, %v; want %+v, %v", pages, n, b, ok, wantBlock, wantOK)
					}
					var want spanWord
					if wantOK {
						want = refWord(pages | run(wantBlock.start, wantBlock.n))
					}
					if next != want {
						t.Fatalf("reserve(%032b, %d) left %064b, want %064b", pages, n, next, want)
					}
					if ok && next.release(b) != w {
						t.Fatalf("release after reserve(%032b, %d) did not restore the word", pages, n)
					}
				}
			}
		}
	}
}

// The nearest occupied buddy determines which shared ancestors survive.
// Include multiple occupied buddies and a whole subtree being released.
func TestSpanWordReleaseSharedAncestors(t *testing.T) {
	for _, test := range []struct {
		name      string
		n         int
		neighbors uint64
	}{
		{"leaf/neighbor-4K", 1, 1 << 5},
		{"leaf/neighbor-8K", 1, 1 << 6},
		{"leaf/neighbor-16K", 1, 1 << 0},
		{"leaf/neighbor-32K", 1, 1 << 8},
		{"leaf/several-buddies", 1, 1<<0 | 1<<5 | 1<<6 | 1<<8},
		{"leaf/empty-half", 1, 0},
		{"subtree/neighbor-16K", 4, 1 << 0},
		{"subtree/neighbor-32K", 4, 1 << 8},
		{"subtree/several-buddies", 4, 1<<0 | 1<<8},
		{"subtree/empty-half", 4, 0},
	} {
		for half := range 2 {
			t.Run(fmt.Sprintf("half%d/%s", half, test.name), func(t *testing.T) {
				b := block{start: half*halfPages + 4, n: test.n}
				// Keep one page in the other half occupied throughout.
				remaining := test.neighbors<<(half*halfPages) | 1<<((1-half)*halfPages)
				w := refWord(remaining | run(b.start, b.n))
				require.Equal(t, refWord(remaining), w.release(b))
			})
		}
	}
}

// Release every fully occupied buddy block in every half occupancy pattern,
// including blocks that reserve would not choose first. The other half stays
// partially occupied to catch accidental changes across the tree boundary.
func TestSpanWordRelease(t *testing.T) {
	for p := range uint64(1 << halfPages) {
		for half := range 2 {
			shift := half * halfPages
			pages := p<<shift | uint64(0xa55a)<<((1-half)*halfPages)
			w := refWord(pages)
			for n := 1; n <= halfPages; n *= 2 {
				for start := 0; start < halfPages; start += n {
					mask := run(start, n)
					if p&mask != mask {
						continue
					}
					b := block{start: shift + start, n: n}
					want := refWord(pages &^ (mask << shift))
					if got := w.release(b); got != want {
						t.Fatalf("release(%032b, %+v) = %064b, want %064b", pages, b, got, want)
					}
				}
			}
		}
	}
}

// Matches of a snapshot's empty-span pattern must start at word boundaries,
// including when two nonempty spans have adjacent empty inner halves.
func TestSpanWordEmptyPattern(t *testing.T) {
	for _, test := range []struct {
		name  string
		order binary.ByteOrder
	}{
		{"little", binary.LittleEndian},
		{"big", binary.BigEndian},
	} {
		t.Run(test.name, func(t *testing.T) {
			empty := uint64(refWord(0))
			var pattern [24]byte
			for i := 0; i < len(pattern); i += 8 {
				test.order.PutUint64(pattern[i:], empty)
			}
			var data [64]byte
			for pages := uint64(1); pages < 1<<16; pages++ {
				left, right := uint64(refWord(pages)), uint64(refWord(pages<<16))
				// Both occupied includes words with a zero high byte, e.g. pages 0 and 16.
				both := uint64(refWord(pages | pages<<16))
				for _, word := range []uint64{left, right, both} {
					lowestByte := bits.TrailingZeros64(word) &^ 7
					if byte(word>>lowestByte) == 0xc0 {
						t.Fatalf("nonempty word %016x has 0xc0 as its lowest nonzero byte", word)
					}
				}
				for i, word := range [...]uint64{both, left, right, empty, empty, right, left, both} {
					test.order.PutUint64(data[i*8:], word)
				}
				for _, search := range []struct {
					n       int
					offsets []int
				}{
					{8, []int{24, 32}},
					{16, []int{24}},
					{24, nil},
				} {
					cursor, found := 0, 0
					for {
						at := bytes.Index(data[cursor:], pattern[:search.n])
						if at < 0 {
							break
						}
						at += cursor
						if found >= len(search.offsets) || at != search.offsets[found] {
							t.Fatalf("pages %016b, %d-byte match at %d; want offsets %v", pages, search.n, at, search.offsets)
						}
						found++
						cursor = at + 1 // Include overlapping and unaligned matches.
					}
					if found != len(search.offsets) {
						t.Fatalf("pages %016b, %d-byte search found %d matches; want %v", pages, search.n, found, search.offsets)
					}
				}
			}
		})
	}
}
