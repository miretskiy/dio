# dio

`dio` is a collection of low-level storage primitives for Go: aligned memory,
fixed-size buffer pools, portable file syscalls, and synchronous or io_uring
I/O scheduling.

The module requires Go 1.25.11 or newer. The `align`, `mempool`, `sys`, and POSIX
scheduler APIs work on Linux and Darwin. io_uring support is Linux-only.

```sh
go get github.com/miretskiy/dio
```

## Packages

| Package | Purpose |
| --- | --- |
| `align` | Page-aligned allocation and range arithmetic |
| `mempool` | Page-aligned fixed-slot and reference-counted buffer pools |
| `sys` | Portable file allocation, syncing, direct I/O, and hole punching |
| `iosched` | A common operation/ticket API over POSIX and io_uring |
| `ringo` | Lifetime-safe low-level io_uring operations and resource ownership |

Most applications should use `iosched`. `ringo` is the lower-level Linux API
used to implement it; unlike a raw SQE binding, it owns every kernel-visible Go
object from `Push` through final completion reaping.

## Aligned memory

Linux `O_DIRECT` generally requires the buffer address, transfer length, and
file offset to be aligned. The `align` package centralizes those calculations
and owns aligned mmap allocations.

```go
buf := align.AllocAligned(1 << 20)
defer align.FreeAligned(buf)

alignedOffset, alignedLength := align.AlignRange(offset, length)
_ = alignedOffset
_ = alignedLength
```

`GrowAligned` preserves an allocation when it is already large enough and
replaces it otherwise. Memory returned by `AllocAligned` is not managed by the
garbage collector and must be released with `FreeAligned`.

## Buffer pools

`SlabPool` owns one contiguous aligned mapping divided into fixed-size slots.
`Acquire` is non-blocking and returns `ErrSlabExhausted` when every slot is in
use.

```go
pool, err := mempool.NewSlabPool(64<<20, 1<<20)
if err != nil {
    return err
}
defer pool.Close()

slot, err := pool.Acquire()
if err != nil {
    return err
}
defer slot.Release()

copy(slot.Data, payload)
```

A slab can be registered as one io_uring fixed buffer. The caller retains
ownership and must close the scheduler before closing the pool.

```go
sched, err := iosched.NewURingScheduler(
    iosched.WithRingDepth(256),
    iosched.WithDMASlab(pool),
)
if err != nil {
    return err
}
defer sched.Close()

ticket, err := sched.Submit(iosched.WriteFixedOp(f, slot.Data, offset))
```

`MmapPool` is the blocking, reference-counted alternative. Each `Acquire`
returns a fresh `MmapBuffer`; `TryInc` adds a reference and every reference must
eventually call `Unpin`.

```go
pool := mempool.NewMmapPool("writes", 1<<20, 32)
defer pool.Close()

buf := pool.Acquire()
defer buf.Unpin()
copy(buf.Bytes(), payload)
```

## File operations

The `sys` package keeps `*os.File` live across raw syscalls and normalizes the
platform differences around direct I/O and syncing.

```go
f, err := sys.CreateDirect(path, sys.FlDirectIO)
if err != nil {
    return err
}
defer f.Close()

if err := sys.Fallocate(f, 512<<20); err != nil {
    return err
}
if err := sys.Fdatasync(f); err != nil {
    return err
}
```

Other operations include `OpenDirect`, `PunchHole`, `Fadvise`,
`CopyFileRange`, `PreadAligned`, and `Ftruncate`. Unsupported optimizations are
implemented as portable fallbacks or documented no-ops; use
`sys.RequiresAlignment` and `sys.UseFadvise` when behavior matters to the
caller.

## I/O scheduling

`Scheduler` accepts an `Op` and returns a `Ticket`. `URingScheduler.Submit` is a
non-blocking handoff to a coordinator goroutine. `POSIXScheduler.Submit` runs
the operation synchronously, so its returned ticket is already complete.

```go
sched := iosched.NewDefaultScheduler(iosched.WithVFiles(64))
defer sched.Close()

ticket, err := sched.Submit(iosched.ReadOp(f, buf, offset))
if err != nil {
    return err // not accepted
}
ticket.Wait()
if err := ticket.Error(); err != nil {
    return err // execution failed
}
n := ticket.N()
```

`NewDefaultScheduler` chooses io_uring when the running Linux kernel provides
the required features and falls back to POSIX otherwise. To require io_uring,
construct it directly with the same options:

```go
sched, err := iosched.NewURingScheduler(
    iosched.WithRingDepth(256),
    iosched.WithVFiles(64),
    iosched.WithCoalescing(true),
)
```

`WithSQPOLL` enables kernel submission-queue polling. It can require additional
privileges and dedicates a kernel thread, so it should be an explicit choice.

The scheduler does not provide application-level backpressure. Callers that
need an in-flight limit should own the semaphore, queue, or buffer pool that
defines it. Submitted buffers must remain valid until the ticket completes.

### Operations and ordering

Positioned scalar and vectored reads/writes, `fsync`, `fdatasync`, file
allocation, open, and close are represented as `Op` values. `Link` stops the
remaining chain after an operation error; `HardLink` continues it.

```go
op := iosched.WriteOp(f, payload, offset).
    Link(iosched.FdatasyncOp(f))
ticket, err := sched.Submit(op)
```

Reads may complete with a short count and no error. Writes are not retried; a
short write returns its count and `io.ErrShortWrite`, following `io.Writer`
semantics. A caller that wants retry policy can resubmit the remaining suffix at
`offset + int64(ticket.N())`.

Standalone writes can request durability directly:

```go
ticket, err := sched.Submit(iosched.WriteOp(f, payload, offset).Durable())
```

The ticket completes after `fdatasync`. `Durable` is deliberately rejected in
a linked chain; use an explicit `FdatasyncOp` there so ordering is visible.

Separate submissions are otherwise unordered. The io_uring coordinator does
provide file-lifecycle barriers: a virtual open holds later work for its slot,
and close waits for previously accepted work on the file. Racing `Submit`
calls do not create an application-visible order.

### Virtual files

Virtual operations name a scheduler-owned slot instead of passing `*os.File`.
On io_uring these are registered-file slots; POSIX emulates the same API with a
userspace table. Configure the io_uring table with `WithVFiles`.

```go
const slot = uint32(0)

open := iosched.VOpenatOp(
    unix.AT_FDCWD,
    path,
    unix.O_CREATE|unix.O_RDWR,
    0o600,
    slot,
).Link(
    iosched.VFallocateOp(slot, fileSize),
    iosched.VWriteOp(slot, firstBlock, 0),
)

ticket, err := sched.Submit(open)
```

Wait for a close-containing ticket before reusing its slot. Contiguous,
standalone writes accepted next to one another may be coalesced into a single
`writev`; each original submission still receives its own ticket and count.

## Low-level io_uring

Ringo exposes typed operations without exposing SQEs, CQEs, raw `user_data`, or
manual completion-queue advancement:

```go
ring, err := ringo.New(ringo.WithDepth(256))
if err != nil {
    return err
}
defer ring.Close()

handle, err := ring.Push(ringo.Read(f, buf, offset))
if err != nil {
    return err
}
_, err = ring.SubmitAndWait(1)
if err != nil {
    return err
}
for completion := range ring.Reap() {
    if completion.Handle != handle {
        continue
    }
    return completion.Err
}
```

`Push` permanently consumes the Op; discard every copy and use only its
Handle. Referenced files, buffers, copied paths, and iovecs remain retained
through the final CQE; multishot completions remain retained while their
`More` flag is set. Fixed-file and fixed-buffer registrations remain owned
until `Ring.Close`. Retention does not prevent access through another slice or
pointer alias: callers must not modify memory the kernel may read or access
memory the kernel may write while an operation is active. Callers also
serialize calls on the same ring, avoid explicitly closing retained files,
submit pushed work, reap completions, and handle short I/O.

## Validation

```sh
./scripts/check.sh
```

The script requires `staticcheck` in `PATH` and runs tests, `go vet`,
Staticcheck, and the scheduler race tests. On non-Linux hosts it also compiles
and analyzes the Linux build. The io_uring runtime tests still require Linux;
cross-compiling checks the Linux code but does not exercise the kernel path.

## License

Apache 2.0. See `LICENSE`.
