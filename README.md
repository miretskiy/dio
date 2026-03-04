# dio — Direct I/O primitives for Go

`dio` provides production-grade, bare-metal I/O primitives for Go programs
that need to bypass the Linux page cache: page-aligned memory pooling,
`io_uring`/`pread`/`pwrite` scheduling, and OS-level file-descriptor wrappers
that correctly manage GC lifetime across raw syscalls.

Extracted from an internal storage engine and battle-tested on NVMe hardware.

```
go get github.com/miretskiy/dio
```

Requires **Go 1.25+**. Fully functional on Linux and Darwin; graceful no-ops
on other POSIX platforms.

---

## Packages

| Package | Purpose |
|---|---|
| [`dio/align`](#dioalign) | Page-aligned memory allocation and O_DIRECT arithmetic |
| [`dio/mempool`](#diomempool) | Fixed-capacity pool of pre-warmed mmap buffers |
| [`dio/sys`](#diosys) | OS file-descriptor primitives: Fallocate, Fdatasync, PunchHole, … |
| [`dio/iosched`](#dioiosched) | Pluggable I/O scheduler: synchronous pread/pwrite or async io_uring |

---

## dio/align

Alignment helpers for O_DIRECT I/O. All O_DIRECT operations on Linux require
the buffer address, transfer size, and file offset to be multiples of the
filesystem block size (4 KiB on most systems).

```go
import "github.com/miretskiy/dio/align"

// Round a size up to the next 4 KiB boundary.
aligned := align.PageAlign(4097) // → 8192

// Compute aligned parameters for a byte range — useful before a pread.
off, length := align.AlignRange(offset, int(size))
buf := align.AllocAligned(int(length))
defer align.FreeAligned(buf)
n, err := f.ReadAt(buf, off)
data := buf[offset-off : offset-off+size] // trim lead padding

// Check whether a slice is page-aligned (required before O_DIRECT write).
if !align.IsAligned(buf) {
    return errors.New("buffer not page-aligned")
}

// Grow a reusable aligned buffer without reallocating if already large enough.
buf = align.GrowAligned(buf, newSize) // old buf freed if too small
```

### AllocAligned / FreeAligned

`AllocAligned` uses `mmap(MAP_ANON)` — guaranteed page-aligned, pre-warmed
to commit physical RAM. **Not managed by the GC**: you must call `FreeAligned`
when done.

```go
buf := align.AllocAligned(1 << 20) // 1 MiB, page-aligned
defer align.FreeAligned(buf)

// buf is safe to pass to O_DIRECT read/write
```

---

## dio/mempool

A fixed-capacity pool of pre-allocated, page-aligned mmap buffers.
All memory is committed at construction time — the hot path never
touches the kernel allocator.

### Basic usage

```go
import "github.com/miretskiy/dio/mempool"

// 32 × 1 MiB slabs, pre-allocated and pre-warmed.
pool := mempool.NewMmapPool("writes", 1<<20, 32)
defer pool.Close()

// Acquire blocks until a buffer is available (token-bucket backpressure).
buf := pool.Acquire()

// Write data into it, then submit to the I/O path.
copy(buf.Bytes(), payload)
submitWrite(buf.AlignedBytes(int64(len(payload)))) // page-aligned prefix

// Release when done — returns the raw memory to the pool.
buf.Unpin()
```

### Non-blocking acquire

```go
buf, ok := pool.TryAcquire()
if !ok {
    // Pool is currently empty; handle backpressure explicitly.
    metrics.PoolExhausted.Inc()
    buf = pool.Acquire() // block
}
defer buf.Unpin()
```

### Sharing a buffer across concurrent readers

The pool uses reference counting so multiple goroutines can read from
the same buffer simultaneously.

```go
buf := pool.Acquire() // refCount = 1

for _, reader := range readers {
    if !buf.TryInc() { // refCount++; returns false if already released
        break
    }
    go func(b *mempool.MmapBuffer) {
        defer b.Unpin()
        process(b.Bytes())
    }(buf)
}

buf.Unpin() // writer releases its pin; last reader will return to pool
```

ABA safety: each `Acquire` wraps the raw memory in a **fresh** `*MmapBuffer`
struct, so stale pointers held by racing readers remain distinct even when
the underlying memory is reused.

### Unpooled (one-off) buffers

For allocations that exceed the pool's slab size:

```go
buf := mempool.NewMmapBuffer(int64(largeSize))
defer buf.Unpin() // Munmaps directly — no pool involved
```

### Strict misuse detection

```go
// In init() or TestMain — panics on double-release or other misuse.
mempool.SetPanicOnMisuse(true)
```

---

## dio/sys

OS-level file-descriptor primitives. Every function that issues a raw
syscall accepts `*os.File` and calls `runtime.KeepAlive` after the syscall
to prevent the GC from finalizing the file while the kernel holds its fd.

### Creating and pre-allocating files

```go
import "github.com/miretskiy/dio/sys"

// Open with O_DIRECT | O_DSYNC — bypasses page cache, syncs data on write.
f, err := sys.CreateDirect(path, sys.FlDirectIO|sys.FlDSync)
if err != nil {
    return err
}
defer f.Close()

// Pre-allocate contiguous disk space to reduce fragmentation.
if err := sys.Fallocate(f, 512<<20 /* 512 MiB */); err != nil {
    return err
}
```

### Writing and syncing

```go
buf := align.AllocAligned(blockSize)
defer align.FreeAligned(buf)
fillBuffer(buf)

if _, err := f.Write(buf); err != nil {
    return err
}

// On Darwin, O_DSYNC is unavailable at open time — explicit sync required.
if err := sys.SyncFile(f, sys.FlDSync); err != nil {
    return err
}
```

### Punching holes (sparse files)

```go
// Reclaim disk space occupied by a deleted record.
// The range is aligned inward to block boundaries so adjacent data is safe.
reclaimed, err := sys.PunchHole(f, recordOffset, recordSize)
if err != nil {
    return err
}
log.Printf("reclaimed %d bytes", reclaimed)
```

### Fadvise

```go
// Tell the kernel we will read sequentially — enables aggressive read-ahead.
sys.Fadvise(f, 0, fileSize, sys.FadvSequential)

// After reading, release the pages from the page cache.
sys.Fadvise(f, 0, fileSize, sys.FadvDontNeed)
```

### Platform constants

```go
if sys.RequiresAlignment {
    // Linux O_DIRECT: buffer address, size, and offset must all be 4 KiB-aligned.
}
if sys.UseFadvise {
    // Linux: posix_fadvise is effective.
    // Darwin: F_RDAHEAD is coarser; this is false.
}
```

---

## dio/iosched

Pluggable positioned I/O scheduler. The interface is identical on all
platforms; the implementation is swapped at construction time.

```go
import "github.com/miretskiy/dio/iosched"

// Pick the best available backend automatically:
//   Linux + io_uring kernel support → URingScheduler
//   Otherwise                       → PwriteScheduler
sched, err := iosched.NewDefaultScheduler()
if err != nil {
    return err
}
defer sched.Close()

f, _ := os.Open(path)
defer f.Close()

buf := make([]byte, blockSize)
n, err := sched.ReadAt(int(f.Fd()), buf, offset)

n, err = sched.WriteAt(int(f.Fd()), buf[:n], offset)
```

### PwriteScheduler (synchronous)

Zero-overhead baseline: each call maps directly to one `pread(2)` or
`pwrite(2)` syscall.

```go
sched, err := iosched.NewPwriteScheduler()
```

### URingScheduler (async, Linux only)

Sliding-window io_uring scheduler. A single coordinator goroutine owns
the ring and continuously fills free SQ slots from a pending queue,
calling `SubmitAndWait(1)` to drain completions. The ring stays full,
maximising NVMe queue depth.

```go
if !iosched.IOUringAvailable {
    log.Println("io_uring not available, falling back to pwrite")
}

sched, err := iosched.NewURingScheduler(iosched.URingConfig{
    RingDepth: 256,  // SQ/CQ entries, must be power of two
    SQPOLL:    false, // true burns one CPU core for kernel-side polling
})
```

### Concurrent reads — batching in action

```go
var wg sync.WaitGroup
for _, req := range requests {
    wg.Add(1)
    go func(r Request) {
        defer wg.Done()
        buf := make([]byte, r.Size)
        n, err := sched.ReadAt(int(f.Fd()), buf, r.Offset)
        r.Done(buf[:n], err)
    }(req)
}
wg.Wait()
// URingScheduler coalesces concurrent goroutine submissions into batches,
// submitting them together with a single io_uring_enter syscall.
```

### Stats

```go
stats := sched.Stats()
fmt.Printf("batches=%d requests=%d avg_batch=%.1f\n",
    stats.Batches, stats.Requests, stats.AvgBatch)
if stats.ReadLatency != nil {
    fmt.Printf("p99 read latency: %dµs\n",
        stats.ReadLatency.ValueAtQuantile(99)/1000)
}
```

---

## KeepAlive contract

Every `sys` function that extracts a raw fd from `*os.File` and passes it
to a kernel syscall follows this pattern:

```go
fd := int(f.Fd())        // extract once before any loop
var err error
for {
    err = unix.Fdatasync(fd)
    if err != unix.EINTR { break }
}
runtime.KeepAlive(f)     // single KeepAlive after the loop
return err
```

`f.Fd()` returns a `uintptr`. Once extracted, the Go runtime no longer
considers the integer a reference to `f`, so the GC could finalize `f`
(closing the fd) while the kernel is blocked in the syscall. `KeepAlive`
extends `f`'s lifetime to cover the entire loop.

---

## Testing

```bash
# Unit tests (Darwin/Linux)
go test ./...

# Race detector
go test -race ./...

# Linux-only tests (hole punch, io_uring) — run on an NVMe volume
TMPDIR=/instance_storage go test ./... -v
```

---

## License

MIT
