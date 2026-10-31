package alloc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"testing"
	"testing/iotest"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

// goAlloc lends Go heap memory of a fixed size. Small units expose Memory
// boundaries independently of page alignment.
type goAlloc struct {
	size  int
	limit int // live allocations; zero means unlimited
	calls int
	live  int
}

var _ Allocator = (*goAlloc)(nil)

func (a *goAlloc) TryAcquire() (Memory, error) {
	if a.limit > 0 && a.live == a.limit {
		return Memory{}, ErrExhausted
	}
	a.calls++
	a.live++
	return newMemory(make([]byte, a.size), func() { a.live-- }), nil
}

func (a *goAlloc) Acquire(context.Context) (Memory, error) { return a.TryAcquire() }
func (a *goAlloc) Close()                                  {}

func TestBufferMatchesBytesBuffer(t *testing.T) {
	a := &goAlloc{size: 5}
	b := NewAllocatingBuffer(a)
	var reference bytes.Buffer
	rng := rand.New(rand.NewPCG(7, 11))
	for step := 0; step < 1000; step++ {
		switch rng.IntN(6) {
		case 0, 1:
			p := make([]byte, rng.IntN(31))
			for i := range p {
				p[i] = byte(rng.Uint32())
			}
			n, err := b.Write(p)
			wantN, wantErr := reference.Write(p)
			require.Equal(t, wantN, n)
			require.Equal(t, wantErr, err)
		case 2:
			got, want := make([]byte, rng.IntN(23)), make([]byte, 23)
			want = want[:len(got)]
			n, err := b.Read(got)
			wantN, wantErr := reference.Read(want)
			require.Equal(t, wantN, n)
			require.Equal(t, wantErr, err)
			require.Equal(t, want[:wantN], got[:n])
		case 3:
			p := bytes.Repeat([]byte("abc"), rng.IntN(7))
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
	require.Zero(t, a.live)
}

func TestBufferAllocatorUnits(t *testing.T) {
	for _, kind := range []string{"slab", "extent"} {
		t.Run(kind, func(t *testing.T) {
			var pool Allocator
			var err error
			if kind == "slab" {
				pool, err = NewSlabAllocator(6*align.BlockSize, WithChunkSize(2*align.BlockSize))
			} else {
				pool, err = NewExtentAllocator(2*align.BlockSize, 3)
			}
			require.NoError(t, err)
			defer pool.Close()
			b := NewAllocatingBuffer(pool)
			defer b.Release()
			_, err = b.Write([]byte("prefix"))
			require.NoError(t, err)
			first := b.RawAllocations()[0]
			require.Len(t, first.Bytes(), 2*align.BlockSize)
			// Growth appends units; existing bytes never move.
			require.NoError(t, b.Grow(4*align.BlockSize))
			memories := b.RawAllocations()
			require.Len(t, memories, 3)
			require.Equal(t, first, memories[0])
			require.Equal(t, 6*align.BlockSize, b.Cap())
			require.ErrorIs(t, b.Grow(6*align.BlockSize), ErrExhausted)
			got, err := io.ReadAll(b)
			require.NoError(t, err)
			require.Equal(t, "prefix", string(got))
		})
	}
}

func TestBufferSpansUnits(t *testing.T) {
	a := &goAlloc{size: 5}
	b := NewAllocatingBuffer(a)
	defer b.Release()
	_, err := b.Write([]byte("abcdefghijkl"))
	require.NoError(t, err)
	require.Equal(t, 3, a.calls)
	require.Equal(t, "abcde", string(b.RawAllocations()[0].Bytes()))
	require.Equal(t, "fghij", string(b.RawAllocations()[1].Bytes()))
	got := make([]byte, 7)
	n, err := b.Read(got)
	require.NoError(t, err)
	require.Equal(t, "abcdefg", string(got[:n]))
	require.Equal(t, 5, b.Len())
}

func TestBufferDrainReusesMemory(t *testing.T) {
	a := &goAlloc{size: 8}
	b := NewAllocatingBuffer(a)
	defer b.Release()
	for range 10 {
		_, err := b.Write([]byte("abcdef"))
		require.NoError(t, err)
		got, err := io.ReadAll(b)
		require.NoError(t, err)
		require.Equal(t, "abcdef", string(got))
	}
	require.Equal(t, 1, a.calls, "a drained buffer writes from the start")
}

func TestBufferWriteAllocationFailure(t *testing.T) {
	a := &goAlloc{size: 8, limit: 2}
	b := NewAllocatingBuffer(a)
	defer b.Release()
	_, err := b.Write([]byte("abc"))
	require.NoError(t, err)
	n, err := b.Write(make([]byte, 20))
	require.ErrorIs(t, err, ErrExhausted)
	require.Zero(t, n, "a failed write writes nothing")
	require.Equal(t, 3, b.Len())
	require.Equal(t, 16, b.Cap(), "memory acquired before the failure is retained")
	_, err = b.Write([]byte("next"))
	require.NoError(t, err, "allocation failure does not poison the buffer")
	got, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Equal(t, "abcnext", string(got))
}

func TestBufferRelease(t *testing.T) {
	a := &goAlloc{size: 8}
	b := NewAllocatingBuffer(a)
	_, err := b.Write(bytes.Repeat([]byte("x"), 17))
	require.NoError(t, err)
	require.Len(t, b.RawAllocations(), 3)
	b.Reset()
	require.Equal(t, 3, a.live, "Reset retains all allocations")
	_, err = b.Write([]byte("reused"))
	require.NoError(t, err)
	first := b.RawAllocations()[0]
	require.True(t, first.TryInc())
	b.Release()
	require.Equal(t, 1, a.live, "an independent pin survives Release")
	require.Zero(t, b.Len())
	require.Zero(t, b.Cap())
	require.Equal(t, "reusedxx", string(first.Bytes()))
	first.Unpin()
	require.Zero(t, a.live)
	_, err = b.Write([]byte("again"))
	require.NoError(t, err, "Release leaves a reusable empty buffer")
	b.Release()
	require.Zero(t, a.live)
}

type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(p []byte) (int, error) { return f(p) }

func TestBufferWriteToPreservesSuffix(t *testing.T) {
	for _, failure := range []error{nil, io.ErrClosedPipe} {
		a := &goAlloc{size: 8}
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
		require.Zero(t, a.live)
	}
}

func TestBufferReadFromSourceErrors(t *testing.T) {
	failure := errors.New("source failed")
	for _, sourceError := range []error{io.EOF, failure} {
		a := &goAlloc{size: 8}
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
		require.Zero(t, a.live)
	}
}

func TestBufferReadFromAllocationFailure(t *testing.T) {
	a := &goAlloc{size: 8, limit: 1}
	b := NewAllocatingBuffer(a)
	defer b.Release()
	source := bytes.NewBufferString("abcdefghijkl")
	n, err := b.ReadFrom(source)
	require.ErrorIs(t, err, ErrExhausted)
	require.EqualValues(t, 8, n)
	require.Equal(t, "ijkl", source.String(), "failed growth consumes no source bytes")
	got, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Equal(t, "abcdefgh", string(got))
}
