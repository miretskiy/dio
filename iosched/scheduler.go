// Package iosched provides pluggable I/O scheduling for positioned reads and writes.
//
// Two implementations are provided:
//   - [PwriteScheduler]: synchronous pread/pwrite, zero overhead (default).
//   - [URingScheduler]: asynchronous io_uring with sliding-window batching
//     and optional SQPOLL (Linux only).
//
// Use [IOUringAvailable] to probe kernel support at runtime, then pick the
// best available backend with [NewDefaultScheduler].
package iosched

import (
	"io"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// IOScheduler abstracts positioned reads and writes for pluggable I/O backends.
//
// Implementations must be safe for concurrent use by multiple goroutines.
// Buffers passed to ReadAt/WriteAt must remain valid until the call returns.
type IOScheduler interface {
	// ReadAt performs a positioned read of len(buf) bytes from fd at offset.
	ReadAt(fd int, buf []byte, offset int64) (int, error)

	// WriteAt performs a positioned write of len(buf) bytes to fd at offset.
	WriteAt(fd int, buf []byte, offset int64) (int, error)

	// Stats returns a snapshot of scheduler statistics.
	Stats() Stats

	io.Closer
}

// URingConfig configures the io_uring scheduler.
type URingConfig struct {
	// RingDepth is the number of SQ/CQ entries. Must be a power of two.
	// Zero uses the default (256).
	RingDepth uint32

	// SQPOLL enables IORING_SETUP_SQPOLL. The kernel spawns a polling thread
	// that submits SQEs without a syscall on the submission path, at the cost
	// of one CPU core. The kernel thread sleeps after ~1 s of idle.
	SQPOLL bool
}

// Stats reports I/O scheduler statistics.
type Stats struct {
	// ReadLatency is a snapshot of read latency distribution (nanoseconds).
	// May be nil if the scheduler was not constructed via its constructor.
	ReadLatency *hdrhistogram.Histogram

	// WriteLatency is a snapshot of write latency distribution (nanoseconds).
	WriteLatency *hdrhistogram.Histogram

	// Batching stats (non-zero for URingScheduler only).
	Batches  int64   // number of SubmitAndWait calls
	Requests int64   // total requests submitted
	MaxBatch int64   // largest single batch observed
	AvgBatch float64 // average batch size
}

// ioLatency tracks per-operation latency using a mutex-protected HDR histogram.
// Embedded by both PwriteScheduler and URingScheduler; initialized by
// initLatency. The mutex cost (~50 ns) is negligible relative to disk I/O.
type ioLatency struct {
	mu        sync.Mutex
	readHist  *hdrhistogram.Histogram
	writeHist *hdrhistogram.Histogram
}

func (l *ioLatency) initLatency() {
	l.readHist = hdrhistogram.New(1_000, 10_000_000_000, 3)
	l.writeHist = hdrhistogram.New(1_000, 10_000_000_000, 3)
}

func (l *ioLatency) readSnapshot() *hdrhistogram.Histogram {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.readHist == nil {
		return nil
	}
	return hdrhistogram.Import(l.readHist.Export())
}

func (l *ioLatency) writeSnapshot() *hdrhistogram.Histogram {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writeHist == nil {
		return nil
	}
	return hdrhistogram.Import(l.writeHist.Export())
}

func (l *ioLatency) recordRead(start time.Time) {
	if l.readHist == nil {
		return
	}
	ns := time.Since(start).Nanoseconds()
	l.mu.Lock()
	l.readHist.RecordValue(ns)
	l.mu.Unlock()
}

func (l *ioLatency) recordWrite(start time.Time) {
	if l.writeHist == nil {
		return
	}
	ns := time.Since(start).Nanoseconds()
	l.mu.Lock()
	l.writeHist.RecordValue(ns)
	l.mu.Unlock()
}
