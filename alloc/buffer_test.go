package alloc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

const chunk = align.BlockSize

// limitedAllocator returns a one-span allocator with only free pages left.
func limitedAllocator(t *testing.T, free int) *Allocator {
	t.Helper()
	a := newAllocator(t, spanBytes)
	for range spanPages - free {
		m, err := a.TryAcquire(chunk)
		require.NoError(t, err)
		t.Cleanup(func() { m.Unpin() })
	}
	return a
}

func TestBufferMatchesBytesBuffer(t *testing.T) {
	a := newAllocator(t, 64*spanBytes)
	b := NewAllocatingBuffer(a)
	var reference bytes.Buffer
	rng := rand.New(rand.NewPCG(7, 11))
	for step := 0; step < 1000; step++ {
		switch rng.IntN(6) {
		case 0, 1:
			p := make([]byte, rng.IntN(3*chunk))
			for i := range p {
				p[i] = byte(rng.Uint32())
			}
			n, err := b.Write(p)
			wantN, wantErr := reference.Write(p)
			require.Equal(t, wantN, n)
			require.Equal(t, wantErr, err)
		case 2:
			got, want := make([]byte, rng.IntN(2*chunk)), make([]byte, 2*chunk)
			want = want[:len(got)]
			n, err := b.Read(got)
			wantN, wantErr := reference.Read(want)
			require.Equal(t, wantN, n)
			require.Equal(t, wantErr, err)
			require.Equal(t, want[:wantN], got[:n])
		case 3:
			p := bytes.Repeat([]byte("abc"), rng.IntN(chunk))
			n, err := b.ReadFrom(iotest.HalfReader(bytes.NewReader(p)))
			wantN, wantErr := reference.ReadFrom(bytes.NewReader(p))
			require.Equal(t, wantN, n)
			require.Equal(t, wantErr, err)
		case 4:
			var got, want bytes.Buffer
			n, err := b.WriteTo(&got)
			wantN, wantErr := reference.WriteTo(&want)
			require.Equal(t, wantN, n)
			require.Equal(t, wantErr, err)
			require.Equal(t, want.Bytes(), got.Bytes())
		case 5:
			b.Reset()
			reference.Reset()
		}
		require.Equal(t, reference.Len(), b.Len())
	}
	b.Release()
	require.Equal(t, a.Capacity(), a.Available())
}

func TestBufferGrowthAccounting(t *testing.T) {
	a := newAllocator(t, spanBytes)
	b := NewAllocatingBuffer(a)
	defer b.Release()
	_, err := b.Write([]byte("prefix"))
	require.NoError(t, err)
	first := b.RawAllocations()[0]
	require.Len(t, first.Bytes(), chunk, "the final chunk rounds only the remaining demand")
	// Growth appends chunks; existing bytes never move.
	require.NoError(t, b.Grow(4*chunk))
	memories := b.RawAllocations()
	require.Len(t, memories, 2)
	require.Equal(t, first, memories[0])
	require.Equal(t, 5*chunk, b.Cap())
	require.ErrorIs(t, b.Grow(spanBytes), ErrExhausted)
	require.Equal(t, spanBytes, b.Cap(), "chunks acquired before the failure are retained")
	got, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Equal(t, "prefix", string(got))
}

// A large stream needs enough free chunks, not a contiguous run of spans.
// Live neighbors must survive both successful writes and exhausted growth.
func TestBufferLargeWriteAcrossScatteredSpans(t *testing.T) {
	a := newAllocator(t, 6*spanBytes)
	var held [6]Memory
	for range held {
		m, err := a.TryAcquire(MaxAllocationSize)
		require.NoError(t, err)
		span := a.pageOf(m) / spanPages
		held[span] = m
		for j := range m.Bytes() {
			m.Bytes()[j] = byte(span + 1)
		}
	}
	for i := 0; i < len(held); i += 2 {
		held[i].Unpin()
	}
	defer func() {
		for i := 1; i < len(held); i += 2 {
			held[i].Unpin()
		}
	}()
	b := NewAllocatingBuffer(a)
	defer b.Release()
	payload := bytes.Repeat([]byte("abcdefg"), 3*spanBytes/7)
	n, err := b.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.Len(t, b.RawAllocations(), 3)
	seen := make(map[int]bool)
	for _, m := range b.RawAllocations() {
		require.Len(t, m.Bytes(), spanBytes)
		span := a.pageOf(m) / spanPages
		require.Zero(t, span%2, "only alternating spans were freed")
		require.False(t, seen[span], "chunks must not overlap")
		seen[span] = true
	}
	n, err = b.Write(make([]byte, spanBytes))
	require.ErrorIs(t, err, ErrExhausted)
	require.Zero(t, n)
	got, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	for i := 1; i < len(held); i += 2 {
		require.Equal(t, bytes.Repeat([]byte{byte(i + 1)}, spanBytes), held[i].Bytes())
	}
}

// A partial span and two empty spans provide 320 KiB, separated by an occupied
// gap. Stream operations must cross the resulting unequal Memory boundaries.
func TestBufferVariableMemories(t *testing.T) {
	a := newAllocator(t, 4*spanBytes)
	held, err := a.TryAcquire(64 << 10)
	require.NoError(t, err)
	defer held.Unpin()
	copy(held.Bytes(), bytes.Repeat([]byte{0xa5}, len(held.Bytes())))
	// Keep a full span occupied so coalescing cannot turn all free storage
	// into one Memory; stream cursors must still cross unequal boundaries.
	holeSpan := (a.pageOf(held)/spanPages + 2) % 4
	hole, _, ok := a.arena.claimNode(holeSpan, spanPages)
	require.True(t, ok)
	hole.start += holeSpan * spanPages
	holeMemory := a.arena.leases.lease(hole.start, a.arena.region[hole.start*chunk:(hole.start+hole.n)*chunk])
	defer holeMemory.Unpin()
	b := NewAllocatingBuffer(a)
	defer b.Release()
	payload := make([]byte, (320<<10)-73)
	for i := range payload {
		payload[i] = byte(i*31 + i/4096)
	}
	n, err := b.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	var sizes []int
	for _, m := range b.RawAllocations() {
		sizes = append(sizes, len(m.Bytes()))
	}
	require.GreaterOrEqual(t, len(sizes), 2)
	var capacity int
	for _, size := range sizes {
		capacity += size
	}
	require.Equal(t, 320<<10, capacity)
	require.Equal(t, 320<<10, b.Cap())
	require.Zero(t, a.Available())

	// Read past the first Memory, then stop a WriteTo within the next one.
	prefix := make([]byte, sizes[0]+17)
	n, err = b.Read(prefix)
	require.NoError(t, err)
	require.Equal(t, payload[:n], prefix)
	var got bytes.Buffer
	got.Write(prefix)
	written, err := b.WriteTo(writeFunc(func(p []byte) (int, error) { return got.Write(p[:23]) }))
	require.EqualValues(t, 23, written)
	require.ErrorIs(t, err, io.ErrShortWrite)
	_, err = b.WriteTo(&got)
	require.NoError(t, err)
	require.Equal(t, payload, got.Bytes())
	require.Zero(t, b.Len())

	// Draining reset both cursors; reuse the same Memories for streaming input.
	read, err := b.ReadFrom(iotest.HalfReader(bytes.NewReader(payload)))
	require.NoError(t, err)
	require.EqualValues(t, len(payload), read)
	n, err = b.Write(make([]byte, 74))
	require.ErrorIs(t, err, ErrExhausted)
	require.Zero(t, n)
	got.Reset()
	_, err = b.WriteTo(&got)
	require.NoError(t, err)
	require.Equal(t, payload, got.Bytes())
	require.Equal(t, bytes.Repeat([]byte{0xa5}, len(held.Bytes())), held.Bytes())
	b.Release()
	require.EqualValues(t, 320<<10, a.Available())
}

func TestBufferDrainReusesMemory(t *testing.T) {
	b := NewAllocatingBuffer(newAllocator(t, spanBytes))
	defer b.Release()
	for range 10 {
		_, err := b.Write([]byte("abcdef"))
		require.NoError(t, err)
		got, err := io.ReadAll(b)
		require.NoError(t, err)
		require.Equal(t, "abcdef", string(got))
	}
	require.Len(t, b.RawAllocations(), 1, "a drained buffer writes from the start")
}

func TestBufferMergesAcrossGrowthAndPinsCombinedRange(t *testing.T) {
	a := newAllocator(t, 4*spanBytes)
	a.arena.cursors = []arenaCursor{{end: len(a.arena.words)}}
	b := NewAllocatingBuffer(a)
	require.NoError(t, b.Grow(spanBytes+chunk))
	require.Len(t, b.mem, 1, "adjacent span claims share one lease")
	address := &b.mem[0].Bytes()[0]
	unpinned := b.RawAllocations()[0]
	require.NoError(t, b.Grow(2*spanBytes+chunk))
	require.Len(t, b.mem, 1, "RawAllocations does not prevent merging")
	require.Equal(t, address, &b.mem[0].Bytes()[0])
	require.False(t, unpinned.TryInc(), "merging retires unpinned copies")

	raw := b.RawAllocations()
	require.Equal(t, len(raw), cap(raw))
	first := raw[0]
	require.True(t, first.TryInc())
	b.Release()
	require.Equal(t, a.Capacity()-int64(len(first.Bytes())), a.Available())
	require.True(t, first.Unpin())
	a.requireEmpty(t)
	require.False(t, first.TryInc())
}

func TestBufferCursorsAfterExtendingLastMemory(t *testing.T) {
	a := newAllocator(t, 2*spanBytes)
	a.arena.cursors = []arenaCursor{{end: len(a.arena.words)}}
	b := NewAllocatingBuffer(a)
	defer b.Release()
	prefix := bytes.Repeat([]byte("a"), chunk)
	_, err := b.Write(prefix)
	require.NoError(t, err)
	_, err = b.Write([]byte("tail"))
	require.NoError(t, err)
	require.Len(t, b.mem, 1, "growth extends the chunk at the cached write boundary")
	got := make([]byte, chunk)
	_, err = io.ReadFull(b, got)
	require.NoError(t, err)
	require.Equal(t, prefix, got)
	require.NoError(t, b.Grow(2*chunk))
	_, err = b.Write([]byte("more"))
	require.NoError(t, err)
	got, err = io.ReadAll(b)
	require.NoError(t, err)
	require.Equal(t, "tailmore", string(got))
	require.Zero(t, b.Len())
}

func TestBufferMergesUnalignedInteriorAndCrossSpanRanges(t *testing.T) {
	for _, test := range []struct{ start, pages int }{{1, 2}, {5, 7}, {15, 2}, {17, 3}, {31, 35}} {
		t.Run(fmt.Sprintf("start%d/pages%d", test.start, test.pages), func(t *testing.T) {
			a := newAllocator(t, 3*spanBytes)
			a.arena.cursors = []arenaCursor{{end: len(a.arena.words)}}
			var held []Memory
			for range 3 * spanPages {
				m, err := a.TryAcquire(chunk)
				require.NoError(t, err)
				held = append(held, m)
			}
			for i, m := range held {
				if i >= test.start && i < test.start+test.pages {
					m.Unpin()
				} else {
					m.Bytes()[0] = 0xa5
					defer m.Unpin()
				}
			}
			b := NewAllocatingBuffer(a)
			require.NoError(t, b.Grow(test.pages*chunk))
			// Buddy size preference can visit subranges out of address order;
			// every adjacent pair in allocation order must still be merged.
			for i := 1; i < len(b.mem); i++ {
				prev, next := b.mem[i-1], b.mem[i]
				require.NotEqual(t, int(prev.unit)+len(prev.data)/chunk, int(next.unit))
			}
			for _, m := range b.mem {
				require.Equal(t, make([]byte, len(m.Bytes())), m.Bytes())
			}
			b.Release()
			require.EqualValues(t, test.pages*chunk, a.Available())
			for i, m := range held {
				if i < test.start || i >= test.start+test.pages {
					require.Equal(t, byte(0xa5), m.Bytes()[0])
				}
			}
		})
	}
}

func TestBufferConcurrentMergedLeases(t *testing.T) {
	a := newAllocator(t, 256<<20, WithoutZeroing())
	owners := make([]atomic.Int32, a.Capacity()/chunk)
	var wg sync.WaitGroup
	for worker := range 32 {
		wg.Go(func() {
			id := int32(worker + 1)
			buf := NewAllocatingBuffer(a)
			defer buf.Release()
			mark := func(m Memory, from, to int32) {
				for page := int(m.unit); page < int(m.unit)+len(m.data)/chunk; page++ {
					if !owners[page].CompareAndSwap(from, to) {
						t.Errorf("page %d ownership: got %d, expected %d", page, owners[page].Load(), from)
					}
				}
			}
			for round := range 64 {
				size := []int{3 * chunk, spanBytes + chunk, 1 << 20, 4 << 20}[(worker+round)%4]
				if err := buf.Grow(size); err != nil {
					t.Error(err)
					return
				}
				mem := buf.RawAllocations()
				first := mem[0]
				for _, m := range mem {
					mark(m, 0, id)
					m.Bytes()[0] = byte(id)
					m.Bytes()[len(m.Bytes())-1] = byte(id)
				}
				if !first.TryInc() {
					t.Error("could not pin a live merged lease")
					return
				}
				for _, m := range mem[1:] {
					mark(m, id, 0)
				}
				buf.Release()
				if first.Bytes()[0] != byte(id) || first.Bytes()[len(first.Bytes())-1] != byte(id) {
					t.Error("pinned storage was overwritten after buffer release")
				}
				mark(first, id, 0)
				first.Unpin()
				if first.TryInc() {
					t.Error("released merged handle remained live")
				}
			}
		})
	}
	wg.Wait()
	a.requireEmpty(t)
}

func TestBufferWriteAllocationFailure(t *testing.T) {
	b := NewAllocatingBuffer(limitedAllocator(t, 2))
	defer b.Release()
	_, err := b.Write([]byte("abc"))
	require.NoError(t, err)
	n, err := b.Write(make([]byte, 2*chunk))
	require.ErrorIs(t, err, ErrExhausted)
	require.Zero(t, n, "a failed write writes nothing")
	require.Equal(t, 3, b.Len())
	require.Equal(t, 2*chunk, b.Cap(), "memory acquired before the failure is retained")
	_, err = b.Write([]byte("next"))
	require.NoError(t, err, "allocation failure does not poison the buffer")
	got, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Equal(t, "abcnext", string(got))
}

func TestBufferRelease(t *testing.T) {
	a := newAllocator(t, 2*spanBytes)
	b := NewAllocatingBuffer(a)
	_, err := b.Write(bytes.Repeat([]byte("x"), spanBytes+chunk+1))
	require.NoError(t, err)
	require.NotEmpty(t, b.RawAllocations())
	b.Reset()
	require.Equal(t, a.Capacity()-spanBytes-2*chunk, a.Available(), "Reset retains all allocations")
	_, err = b.Write([]byte("reused"))
	require.NoError(t, err)
	first := b.RawAllocations()[0]
	require.True(t, first.TryInc())
	b.Release()
	require.Equal(t, a.Capacity()-int64(len(first.Bytes())), a.Available(), "an independent pin survives Release")
	require.Zero(t, b.Len())
	require.Zero(t, b.Cap())
	require.Equal(t, "reusedxx", string(first.Bytes()[:8]))
	first.Unpin()
	require.Equal(t, a.Capacity(), a.Available())
	_, err = b.Write([]byte("again"))
	require.NoError(t, err, "Release leaves a reusable empty buffer")
	b.Release()
	require.Equal(t, a.Capacity(), a.Available())
}

type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(p []byte) (int, error) { return f(p) }

func TestBufferWriteToPreservesSuffix(t *testing.T) {
	a := newAllocator(t, spanBytes)
	for _, failure := range []error{nil, io.ErrClosedPipe} {
		b := NewAllocatingBuffer(a)
		_, err := b.Write([]byte("abcdefghijk"))
		require.NoError(t, err)
		n, err := b.WriteTo(writeFunc(func(p []byte) (int, error) { return 2, failure }))
		require.EqualValues(t, 2, n)
		if failure == nil {
			require.ErrorIs(t, err, io.ErrShortWrite)
		} else {
			require.ErrorIs(t, err, failure)
		}
		got, err := io.ReadAll(b)
		require.NoError(t, err)
		require.Equal(t, "cdefghijk", string(got))
		b.Release()
		require.Equal(t, a.Capacity(), a.Available())
	}
}

func TestBufferReadFromSourceErrors(t *testing.T) {
	a := newAllocator(t, spanBytes)
	failure := errors.New("source failed")
	for _, sourceError := range []error{io.EOF, failure} {
		b := NewAllocatingBuffer(a)
		source := iotest.DataErrReader(io.MultiReader(bytes.NewBufferString("abcdefghijk"), iotest.ErrReader(sourceError)))
		n, err := b.ReadFrom(source)
		require.EqualValues(t, 11, n)
		if sourceError == io.EOF {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, failure)
		}
		got, err := io.ReadAll(b)
		require.NoError(t, err)
		require.Equal(t, "abcdefghijk", string(got))
		b.Release()
		require.Equal(t, a.Capacity(), a.Available())
	}
}

func TestBufferReadFromAllocationFailure(t *testing.T) {
	b := NewAllocatingBuffer(limitedAllocator(t, 1))
	defer b.Release()
	p := bytes.Repeat([]byte("abcd"), chunk/4+1)
	source := bytes.NewBuffer(p)
	n, err := b.ReadFrom(source)
	require.ErrorIs(t, err, ErrExhausted)
	require.EqualValues(t, chunk, n)
	require.Equal(t, "abcd", source.String(), "failed growth consumes no source bytes")
	got, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Equal(t, p[:chunk], got)
}
