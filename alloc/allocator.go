// Package alloc provides allocators of page-aligned, reference-counted Buffers.
// ExtentAllocator lends whole backing allocations. SlabAllocator subdivides one
// buffer. NewBuffer maps standalone storage. Memory is accessed through Buffer.
package alloc

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/miretskiy/dio/v2/align"
)

// ErrExhausted means there is insufficient free capacity for an allocation.
var ErrExhausted = errors.New("alloc: allocator exhausted")

// ErrClosed means acquisition was attempted after the allocator was closed.
var ErrClosed = errors.New("alloc: allocator closed")

// Allocator lends buffers with one reference. Release each acquired reference
// with Buffer.Unpin. Acquisition and release are safe for concurrent use.
// Sizes must be positive and are rounded up to whole chunks (whole extents for
// ExtentAllocator). The returned chunks need not be contiguous.
type Allocator interface {
	// Acquire waits for capacity or context cancellation.
	Acquire(ctx context.Context, size int64) (*Buffer, error)
	// TryAcquire returns ErrExhausted instead of waiting for a borrower.
	TryAcquire(size int64) (*Buffer, error)
	// Close releases retained storage. It must not race acquisitions or Unpin.
	// The caller must finish using and releasing borrowed buffers first.
	// Repeated closes are harmless. Do not acquire after Close.
	Close()
}

var (
	_ Allocator = (*ExtentAllocator)(nil)
	_ Allocator = (*SlabAllocator)(nil)
)

type allocatorConfig struct {
	chunkSize     int64
	backing       *Buffer
	retryInterval time.Duration
	warmup        bool
}

// Option configures an allocator at construction time.
type Option func(*allocatorConfig)

// WithChunkSize sets the page-rounded object size of a SlabAllocator.
// The default is 4 KiB chunks.
func WithChunkSize(size int64) Option {
	return func(c *allocatorConfig) { c.chunkSize = size }
}

// WithBackingBuffer supplies a live single-chunk Buffer for a SlabAllocator. Nil
// uses the default mapping behavior. The allocator takes its own reference and
// releases it on Close. The caller remains responsible for its own reference.
// Other allocators reject non-nil backing.
func WithBackingBuffer(backing *Buffer) Option {
	return func(c *allocatorConfig) { c.backing = backing }
}

// WithRetryInterval sets the delay between SlabAllocator Acquire
// retries when exhausted. The default is 100 ms. Cancellation interrupts the wait.
func WithRetryInterval(interval time.Duration) Option {
	return func(c *allocatorConfig) { c.retryInterval = interval }
}

// WithWarmup commits all backing memory during allocator construction.
// It zeroes one byte per page. By default, pages are faulted in when first used.
func WithWarmup() Option {
	return func(c *allocatorConfig) { c.warmup = true }
}

// Use at most eight workers, with at least 64 MiB per worker. Partition
// boundaries follow stride so each page is touched by exactly one worker.
func warmup(data []byte, stride int) {
	workers := min(8, runtime.GOMAXPROCS(0), max(1, len(data)/(64<<20)))
	if workers == 1 {
		align.Warmup(data, stride)
		return
	}
	partitionSize := (len(data) + workers - 1) / workers
	partitionSize = (partitionSize + stride - 1) / stride * stride
	var wg sync.WaitGroup
	for start := 0; start < len(data); start += partitionSize {
		wg.Go(func() { align.Warmup(data[start:min(start+partitionSize, len(data))], stride) })
	}
	wg.Wait()
}
