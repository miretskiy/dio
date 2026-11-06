package alloc

import (
	"math/bits"

	"github.com/miretskiy/dio/v2/align"
)

// A span holds 32 pages of 4 KiB each (align.BlockSize), for 128 KiB total.
const (
	spanPages = 32
	spanBytes = spanPages * align.BlockSize
)

// A span has two 64 KiB halves, A and B. Each half serves allocations of
// 4, 8, 16, 32, or 64 KiB. A binary buddy tree describes those possible blocks:
// the root represents the whole half, its children represent two 32 KiB
// blocks, and each split halves the block size down to 4 KiB pages.
// This gives sixteen leaves and 1 + 2 + 4 + 8 + 16 = 31 nodes per tree.
//
// "Buddies" are the two equal-sized children of the same parent block.
// When both are free, they can merge back into that parent, recursively.
// Adjacent blocks with different parents are not buddies and cannot merge.
const (
	halfPages = 16
	treeBits  = 31
)

// spanWord represents each tree node with one bit. A set bit means some or
// all pages in the node's block are occupied; a clear bit means the entire
// block is free. Each half uses heap indices 1..31: node 1 is the root,
// node i has children 2*i and 2*i+1, parent i/2, and buddy i^1 (except the root).
// Node i occupies bit i-1 in A, or bit 31+i-1 in B. A covers pages 0..15;
// B covers pages 16..31. Nodes at a level follow increasing page addresses.
//
//	level:       0       1       2       3       4
//	block size:  4 KiB   8 KiB  16 KiB  32 KiB  64 KiB
//	heap nodes: 16..31   8..15   4..7    2..3    1
//	bits in A:  15..30   7..14   3..6    1..2    0
//	bits in B:  46..61  38..45  34..37  32..33  31
//
// Bit 62 means A is empty; bit 63 means B is empty. The trees and empty flags
// are published together with one CAS by the arena.
//
//	memory:      A [0 ... 15] | B [16 ... 31]
//	leaf fields: A [15 ... 0] | B [31 ... 16]  (each high bit first)
//
// A's leading zeros count its free suffix; B's trailing zeros count its free
// prefix. Together they give the free run crossing A/B, where every run over
// 16 pages must lie. uint16(w>>15) and uint16(w>>46) isolate these leaf fields,
// excluding parent bits and empty flags. Smaller runs need a buddy search.
type spanWord uint64

// An empty span has just its empty flags set. A fully occupied span has all
// tree bits set and both empty flags clear. One 128 KiB allocation and a full
// set of smaller allocations share that encoding; Memory records the extent.
//
// The high byte can be zero. Instead, consider the lowest nonzero byte:
// if A is occupied, bit 0 makes the low byte odd; if only B is occupied,
// bit 31 makes its lowest nonzero byte 0x80. Only an empty span has 0xc0
// as its lowest nonzero byte, in the high byte of the word.
// Thus an unaligned little-endian match of seven zeros followed by 0xc0
// would require the next word to begin with zeros and then 0xc0 before its
// high byte, which no valid word does. Big endian reverses this argument
// using the preceding word's suffix. In a consistent snapshot, one or more
// repeated empty-span patterns can therefore match only at word boundaries.
const (
	emptySpan = spanWord(0xc000000000000000)
	fullSpan  = spanWord(0x3fffffffffffffff)
)

// block identifies a contiguous interval in pages. reserve returns an aligned
// buddy block of 1, 2, 4, 8, or 16 pages, or a run of 17..32 pages crossing the
// half boundary. release also accepts intervals formed by merging adjacent
// claims. Span operations use a span-relative start; arena claims translate
// it to an arena-relative start.
type block struct {
	start int
	n     int
}

// reserve claims a contiguous block of at most n pages. A whole-span request
// first tries the free run crossing the half boundary when it exceeds 16 pages.
// Otherwise, it chooses the largest free buddy block, then the lowest starting
// page on ties. The caller must round the limit to a size class: 1, 2, 4, 8,
// 16, or 32 pages. TryAcquire validates this before entering the arena scan.
// A full span returns zero values and false. The returned block records the
// actual page count. The caller publishes next by CAS only on success.
func (w spanWord) reserve(n int) (next spanWord, b block, ok bool) {
	if w == fullSpan {
		return 0, block{}, false
	}
	if n == spanPages {
		if w == emptySpan {
			return fullSpan, block{start: 0, n: spanPages}, true
		}
		// A run longer than one half must cross the A/B boundary.
		tail := bits.LeadingZeros16(uint16(w >> 15))
		head := bits.TrailingZeros16(uint16(w >> 46))
		if pages := tail + head; pages > halfPages {
			next, b = w.reserveLarge(halfPages-tail, pages)
			return next, b, true
		}
		n = halfPages
	}
	if w != emptySpan {
		// Folding finds the largest free size class. Search that class in
		// the original word below so A's lower addresses take priority over B.
		free := ^w & fullSpan
		bit := bits.TrailingZeros64(uint64(free | free>>treeBits))
		n = min(n, claimTab[bit].block.n)
	}

	level := bits.TrailingZeros(uint(n))
	free := ^w & buddyTab[level]
	claim := &claimTab[bits.TrailingZeros64(uint64(free))]
	next = w | claim.setMask
	next &^= claim.emptyFlag
	return next, claim.block, true
}

// release returns b, which must identify a live allocation.
func (w spanWord) release(b block) spanWord {
	if b.n == spanPages {
		return emptySpan
	}
	if b.n > halfPages {
		return w.releaseLarge(b)
	}
	if b.n&(b.n-1) != 0 || b.start&(b.n-1) != 0 {
		return w.releaseSmallRange(b)
	}
	level := bits.TrailingZeros(uint(b.n))
	node := (halfPages + b.start%halfPages) >> level
	return w.releaseBuddy(b.start/halfPages*treeBits + node - 1)
}

// Merging can produce a small interval that is not an aligned buddy block.
func (w spanWord) releaseSmallRange(b block) spanWord {
	if b.start >= halfPages {
		return w.releaseHalfRange(1, b.start-halfPages, b.n)
	}
	pages := min(b.n, halfPages-b.start)
	w = w.releaseHalfRange(0, b.start, pages)
	if pages < b.n {
		w = w.releaseHalfRange(1, 0, b.n-pages)
	}
	return w
}

// Intersecting a suffix and prefix describes any interval in one half. Unlike
// an edge, an interior interval can have live neighbors on both sides, so both
// outside ancestor paths must survive.
func (w spanWord) releaseHalfRange(half, start, pages int) spanWord {
	suffix := &edgeTab[halfPages-start].suffix
	prefix := &edgeTab[start+pages].prefix
	shift := half * treeBits
	setMask := spanWord(suffix.setMask&prefix.setMask) << shift
	keep := w.outsideAncestors(spanWord(suffix.buddyMask)<<shift) |
		w.outsideAncestors(spanWord(prefix.buddyMask)<<shift)
	w &^= setMask &^ keep
	if keep == 0 {
		w |= 1 << (62 + half)
	}
	return w
}

func (w spanWord) outsideAncestors(mask spanWord) spanWord {
	occupied := w & mask
	if occupied == 0 {
		return 0
	}
	return claimTab[bits.Len64(uint64(occupied))-1].setMask
}

// releaseMasks releases a buddy block, prefix, or suffix within one half.
// setMask includes the released pages and every node above them; buddyMask
// selects the maximal outside subtrees whose parents touch the release.
// Their shared ancestor paths are nested for these supported shapes, so the
// deepest occupied outside subtree determines all ancestors that must remain.
func (w spanWord) releaseMasks(setMask, buddyMask, emptyFlag spanWord) spanWord {
	occupied := w & buddyMask
	if occupied == 0 {
		return (w &^ setMask) | emptyFlag
	}
	buddy := bits.Len64(uint64(occupied)) - 1
	keep := claimTab[buddy].setMask
	return w &^ (setMask &^ keep)
}

// releaseBuddy clears one buddy subtree, preserving shared ancestors.
func (w spanWord) releaseBuddy(bit int) spanWord {
	claim := &claimTab[bit]
	return w.releaseMasks(claim.setMask, claim.buddyMask, claim.emptyFlag)
}

// reserveLarge claims an A suffix and B prefix, covering 17..32 pages.
// Keep this out of reserve: inlining regressed full-span scans on arm64.
//
//go:noinline
func (w spanWord) reserveLarge(start, pages int) (spanWord, block) {
	aMasks := &edgeTab[halfPages-start].suffix
	bMasks := &edgeTab[start+pages-halfPages].prefix
	setMask := spanWord(aMasks.setMask) | spanWord(bMasks.setMask)<<treeBits
	return (w | setMask) &^ emptySpan, block{start: start, n: pages}
}

// releaseLarge releases each half once, including whole-span allocations.
func (w spanWord) releaseLarge(b block) spanWord {
	aMasks := &edgeTab[halfPages-b.start].suffix
	bMasks := &edgeTab[b.start+b.n-halfPages].prefix
	w = w.releaseMasks(spanWord(aMasks.setMask), spanWord(aMasks.buddyMask), 1<<62)
	return w.releaseMasks(spanWord(bMasks.setMask)<<treeBits, spanWord(bMasks.buddyMask)<<treeBits, 1<<63)
}

// freePages counts unoccupied leaves (4 KiB pages).
func (w spanWord) freePages() int {
	return spanPages - bits.OnesCount64(uint64(w&buddyTab[0]))
}

// levelOf rounds a positive page count up to its buddy level: 1 -> 0,
// 2 -> 1, 3..4 -> 2, through 17..32 -> 5 (a whole span).
func levelOf(n int) int { return bits.Len(uint(n - 1)) }

// buddyTab selects candidate node bits across both trees, indexed by buddy level.
//
// Binary masks in both tables read from high to low: empty flags (B then A),
// tree B, tree A. Within each tree, leaves come first and the root last.
// R means root (level 4, 64 KiB): the first R is Rb at bit 31, the second Ra
// at bit 0. L3, L2, L1, L0 name the 32, 16, 8, and 4 KiB levels.
var buddyTab = [5]spanWord{
	// 4 KiB (1 page)
	0b00_1111111111111111_00000000_0000_00_0_1111111111111111_00000000_0000_00_0,
	// 8 KiB (2 pages)
	0b00_0000000000000000_11111111_0000_00_0_0000000000000000_11111111_0000_00_0,
	// 16 KiB (4 pages)
	0b00_0000000000000000_00000000_1111_00_0_0000000000000000_00000000_1111_00_0,
	// 32 KiB (8 pages)
	0b00_0000000000000000_00000000_0000_11_0_0000000000000000_00000000_0000_11_0,
	// 64 KiB (16 pages)
	0b00_0000000000000000_00000000_0000_00_1_0000000000000000_00000000_0000_00_1,
}

// claimTab is indexed by the selected bit in spanWord. Each tree bit
// identifies one block, so all size classes share this table. Bits 62 and
// 63 are empty flags, not tree nodes; their entries are unused.
//
// setMask includes the node, its descendants, and its ancestors. buddyMask
// includes the sibling of the node and of each ancestor below the root; the
// two roots have no buddies. emptyFlag is cleared on reserve and restored
// when release empties that half.
// block gives the starting page and page count within the whole span.
var claimTab = [64]struct {
	setMask   spanWord
	buddyMask spanWord
	emptyFlag spanWord
	block     block
}{
	// A: 64 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	0: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_1111111111111111_11111111_1111_11_1,
		buddyMask: 0,
		emptyFlag: 1 << 62,
		block:     block{start: 0, n: 16},
	},

	// A: 32 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	1: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000011111111_00001111_0011_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00000000_0000_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 0, n: 8},
	},
	2: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_1111111100000000_11110000_1100_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00000000_0000_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 8, n: 8},
	},

	// A: 16 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	3: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000000001111_00000011_0001_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00000000_0010_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 0, n: 4},
	},
	4: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000011110000_00001100_0010_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00000000_0001_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 4, n: 4},
	},
	5: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000111100000000_00110000_0100_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00000000_1000_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 8, n: 4},
	},
	6: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_1111000000000000_11000000_1000_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00000000_0100_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 12, n: 4},
	},

	// A: 8 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	7: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000000000011_00000001_0001_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00000010_0010_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 0, n: 2},
	},
	8: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000000001100_00000010_0001_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00000001_0010_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 2, n: 2},
	},
	9: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000000110000_00000100_0010_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00001000_0001_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 4, n: 2},
	},
	10: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000011000000_00001000_0010_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00000100_0001_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 6, n: 2},
	},
	11: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000001100000000_00010000_0100_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00100000_1000_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 8, n: 2},
	},
	12: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000110000000000_00100000_0100_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_00010000_1000_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 10, n: 2},
	},
	13: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0011000000000000_01000000_1000_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_10000000_0100_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 12, n: 2},
	},
	14: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_1100000000000000_10000000_1000_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000000_01000000_0100_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 14, n: 2},
	},

	// A: 4 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	15: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000000000001_00000001_0001_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000010_00000010_0010_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 0, n: 1},
	},
	16: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000000000010_00000001_0001_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000001_00000010_0010_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 1, n: 1},
	},
	17: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000000000100_00000010_0001_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000001000_00000001_0010_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 2, n: 1},
	},
	18: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000000001000_00000010_0001_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000000100_00000001_0010_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 3, n: 1},
	},
	19: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000000010000_00000100_0010_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000100000_00001000_0001_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 4, n: 1},
	},
	20: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000000100000_00000100_0010_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000000010000_00001000_0001_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 5, n: 1},
	},
	21: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000001000000_00001000_0010_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000010000000_00000100_0001_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 6, n: 1},
	},
	22: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000010000000_00001000_0010_01_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000001000000_00000100_0001_10_0,
		emptyFlag: 1 << 62,
		block:     block{start: 7, n: 1},
	},

	// A: 4 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	23: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000000100000000_00010000_0100_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000001000000000_00100000_1000_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 8, n: 1},
	},
	24: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000001000000000_00010000_0100_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000000100000000_00100000_1000_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 9, n: 1},
	},
	25: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000010000000000_00100000_0100_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000100000000000_00010000_1000_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 10, n: 1},
	},
	26: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0000100000000000_00100000_0100_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0000010000000000_00010000_1000_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 11, n: 1},
	},
	27: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0001000000000000_01000000_1000_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0010000000000000_10000000_0100_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 12, n: 1},
	},
	28: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0010000000000000_01000000_1000_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0001000000000000_10000000_0100_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 13, n: 1},
	},
	29: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_0100000000000000_10000000_1000_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_1000000000000000_01000000_0100_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 14, n: 1},
	},
	30: {
		setMask:   0b00_0000000000000000_00000000_0000_00_0_1000000000000000_10000000_1000_10_1,
		buddyMask: 0b00_0000000000000000_00000000_0000_00_0_0100000000000000_01000000_0100_01_0,
		emptyFlag: 1 << 62,
		block:     block{start: 15, n: 1},
	},

	// B: 64 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	31: {
		setMask:   0b00_1111111111111111_11111111_1111_11_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0,
		emptyFlag: 1 << 63,
		block:     block{start: 16, n: 16},
	},

	// B: 32 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	32: {
		setMask:   0b00_0000000011111111_00001111_0011_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00000000_0000_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 16, n: 8},
	},
	33: {
		setMask:   0b00_1111111100000000_11110000_1100_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00000000_0000_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 24, n: 8},
	},

	// B: 16 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	34: {
		setMask:   0b00_0000000000001111_00000011_0001_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00000000_0010_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 16, n: 4},
	},
	35: {
		setMask:   0b00_0000000011110000_00001100_0010_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00000000_0001_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 20, n: 4},
	},
	36: {
		setMask:   0b00_0000111100000000_00110000_0100_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00000000_1000_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 24, n: 4},
	},
	37: {
		setMask:   0b00_1111000000000000_11000000_1000_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00000000_0100_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 28, n: 4},
	},

	// B: 8 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	38: {
		setMask:   0b00_0000000000000011_00000001_0001_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00000010_0010_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 16, n: 2},
	},
	39: {
		setMask:   0b00_0000000000001100_00000010_0001_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00000001_0010_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 18, n: 2},
	},
	40: {
		setMask:   0b00_0000000000110000_00000100_0010_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00001000_0001_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 20, n: 2},
	},
	41: {
		setMask:   0b00_0000000011000000_00001000_0010_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00000100_0001_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 22, n: 2},
	},
	42: {
		setMask:   0b00_0000001100000000_00010000_0100_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00100000_1000_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 24, n: 2},
	},
	43: {
		setMask:   0b00_0000110000000000_00100000_0100_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_00010000_1000_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 26, n: 2},
	},
	44: {
		setMask:   0b00_0011000000000000_01000000_1000_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_10000000_0100_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 28, n: 2},
	},
	45: {
		setMask:   0b00_1100000000000000_10000000_1000_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000000_01000000_0100_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 30, n: 2},
	},

	// B: 4 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	46: {
		setMask:   0b00_0000000000000001_00000001_0001_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000010_00000010_0010_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 16, n: 1},
	},
	47: {
		setMask:   0b00_0000000000000010_00000001_0001_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000001_00000010_0010_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 17, n: 1},
	},
	48: {
		setMask:   0b00_0000000000000100_00000010_0001_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000001000_00000001_0010_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 18, n: 1},
	},
	49: {
		setMask:   0b00_0000000000001000_00000010_0001_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000000100_00000001_0010_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 19, n: 1},
	},
	50: {
		setMask:   0b00_0000000000010000_00000100_0010_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000100000_00001000_0001_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 20, n: 1},
	},
	51: {
		setMask:   0b00_0000000000100000_00000100_0010_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000000010000_00001000_0001_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 21, n: 1},
	},
	52: {
		setMask:   0b00_0000000001000000_00001000_0010_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000010000000_00000100_0001_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 22, n: 1},
	},
	53: {
		setMask:   0b00_0000000010000000_00001000_0010_01_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000001000000_00000100_0001_10_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 23, n: 1},
	},

	// B: 4 KiB blocks.
	//    EE|         B: leaves to root         |         A: leaves to root
	//    EE_       L0       _   L1   _ L2 _L3_R_       L0       _   L1   _ L2 _L3_R
	54: {
		setMask:   0b00_0000000100000000_00010000_0100_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000001000000000_00100000_1000_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 24, n: 1},
	},
	55: {
		setMask:   0b00_0000001000000000_00010000_0100_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000000100000000_00100000_1000_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 25, n: 1},
	},
	56: {
		setMask:   0b00_0000010000000000_00100000_0100_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000100000000000_00010000_1000_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 26, n: 1},
	},
	57: {
		setMask:   0b00_0000100000000000_00100000_0100_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0000010000000000_00010000_1000_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 27, n: 1},
	},
	58: {
		setMask:   0b00_0001000000000000_01000000_1000_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0010000000000000_10000000_0100_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 28, n: 1},
	},
	59: {
		setMask:   0b00_0010000000000000_01000000_1000_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0001000000000000_10000000_0100_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 29, n: 1},
	},
	60: {
		setMask:   0b00_0100000000000000_10000000_1000_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_1000000000000000_01000000_0100_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 30, n: 1},
	},
	61: {
		setMask:   0b00_1000000000000000_10000000_1000_10_1_0000000000000000_00000000_0000_00_0,
		buddyMask: 0b00_0100000000000000_01000000_0100_01_0_0000000000000000_00000000_0000_00_0,
		emptyFlag: 1 << 63,
		block:     block{start: 31, n: 1},
	},
}

// edgeMasks describes a prefix or suffix in one 31-bit tree. Both masks
// use positions relative to A; shift by treeBits when applying them to B.
type edgeMasks struct {
	setMask   uint32
	buddyMask uint32
}

// edgeTab[n] describes the first n and last n pages of a half. Its
// 17 entries occupy 272 bytes. Binary fields are L0_L1_L2_L3_R.
// These are static geometry masks; current occupancy stays in spanWord.
var edgeTab = [halfPages + 1]struct {
	prefix edgeMasks
	suffix edgeMasks
}{
	0: {},
	1: {
		prefix: edgeMasks{
			setMask:   0b0000000000000001_00000001_0001_01_1,
			buddyMask: 0b0000000000000010_00000010_0010_10_0,
		},
		suffix: edgeMasks{
			setMask:   0b1000000000000000_10000000_1000_10_1,
			buddyMask: 0b0100000000000000_01000000_0100_01_0,
		},
	},
	2: {
		prefix: edgeMasks{
			setMask:   0b0000000000000011_00000001_0001_01_1,
			buddyMask: 0b0000000000000000_00000010_0010_10_0,
		},
		suffix: edgeMasks{
			setMask:   0b1100000000000000_10000000_1000_10_1,
			buddyMask: 0b0000000000000000_01000000_0100_01_0,
		},
	},
	3: {
		prefix: edgeMasks{
			setMask:   0b0000000000000111_00000011_0001_01_1,
			buddyMask: 0b0000000000001000_00000000_0010_10_0,
		},
		suffix: edgeMasks{
			setMask:   0b1110000000000000_11000000_1000_10_1,
			buddyMask: 0b0001000000000000_00000000_0100_01_0,
		},
	},
	4: {
		prefix: edgeMasks{
			setMask:   0b0000000000001111_00000011_0001_01_1,
			buddyMask: 0b0000000000000000_00000000_0010_10_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111000000000000_11000000_1000_10_1,
			buddyMask: 0b0000000000000000_00000000_0100_01_0,
		},
	},
	5: {
		prefix: edgeMasks{
			setMask:   0b0000000000011111_00000111_0011_01_1,
			buddyMask: 0b0000000000100000_00001000_0000_10_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111100000000000_11100000_1100_10_1,
			buddyMask: 0b0000010000000000_00010000_0000_01_0,
		},
	},
	6: {
		prefix: edgeMasks{
			setMask:   0b0000000000111111_00000111_0011_01_1,
			buddyMask: 0b0000000000000000_00001000_0000_10_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111110000000000_11100000_1100_10_1,
			buddyMask: 0b0000000000000000_00010000_0000_01_0,
		},
	},
	7: {
		prefix: edgeMasks{
			setMask:   0b0000000001111111_00001111_0011_01_1,
			buddyMask: 0b0000000010000000_00000000_0000_10_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111111000000000_11110000_1100_10_1,
			buddyMask: 0b0000000100000000_00000000_0000_01_0,
		},
	},
	8: {
		prefix: edgeMasks{
			setMask:   0b0000000011111111_00001111_0011_01_1,
			buddyMask: 0b0000000000000000_00000000_0000_10_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111111100000000_11110000_1100_10_1,
			buddyMask: 0b0000000000000000_00000000_0000_01_0,
		},
	},
	9: {
		prefix: edgeMasks{
			setMask:   0b0000000111111111_00011111_0111_11_1,
			buddyMask: 0b0000001000000000_00100000_1000_00_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111111110000000_11111000_1110_11_1,
			buddyMask: 0b0000000001000000_00000100_0001_00_0,
		},
	},
	10: {
		prefix: edgeMasks{
			setMask:   0b0000001111111111_00011111_0111_11_1,
			buddyMask: 0b0000000000000000_00100000_1000_00_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111111111000000_11111000_1110_11_1,
			buddyMask: 0b0000000000000000_00000100_0001_00_0,
		},
	},
	11: {
		prefix: edgeMasks{
			setMask:   0b0000011111111111_00111111_0111_11_1,
			buddyMask: 0b0000100000000000_00000000_1000_00_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111111111100000_11111100_1110_11_1,
			buddyMask: 0b0000000000010000_00000000_0001_00_0,
		},
	},
	12: {
		prefix: edgeMasks{
			setMask:   0b0000111111111111_00111111_0111_11_1,
			buddyMask: 0b0000000000000000_00000000_1000_00_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111111111110000_11111100_1110_11_1,
			buddyMask: 0b0000000000000000_00000000_0001_00_0,
		},
	},
	13: {
		prefix: edgeMasks{
			setMask:   0b0001111111111111_01111111_1111_11_1,
			buddyMask: 0b0010000000000000_10000000_0000_00_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111111111111000_11111110_1111_11_1,
			buddyMask: 0b0000000000000100_00000001_0000_00_0,
		},
	},
	14: {
		prefix: edgeMasks{
			setMask:   0b0011111111111111_01111111_1111_11_1,
			buddyMask: 0b0000000000000000_10000000_0000_00_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111111111111100_11111110_1111_11_1,
			buddyMask: 0b0000000000000000_00000001_0000_00_0,
		},
	},
	15: {
		prefix: edgeMasks{
			setMask:   0b0111111111111111_11111111_1111_11_1,
			buddyMask: 0b1000000000000000_00000000_0000_00_0,
		},
		suffix: edgeMasks{
			setMask:   0b1111111111111110_11111111_1111_11_1,
			buddyMask: 0b0000000000000001_00000000_0000_00_0,
		},
	},
	16: {
		prefix: edgeMasks{
			setMask:   0b1111111111111111_11111111_1111_11_1,
			buddyMask: 0,
		},
		suffix: edgeMasks{
			setMask:   0b1111111111111111_11111111_1111_11_1,
			buddyMask: 0,
		},
	},
}
