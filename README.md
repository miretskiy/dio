# dio

`dio` provides direct I/O and io_uring building blocks for Go. Its Linux-only
`ringo` package is a lifetime-safe, policy-free io_uring interface: it exposes
typed operations while retaining every Go object the kernel can access through
the final completion. For applications that want scheduling rather than direct
ring control, `iosched` provides a higher-level operation and ticket API with
io_uring and synchronous POSIX backends.

The supporting packages provide aligned memory, fixed-size buffer pools,
portable file operations, and a targeted TLS read-buffer optimization.

The module requires Go 1.25.11 or newer. The `align`, `alloc`, `sys`, and POSIX
scheduler APIs work on Linux and Darwin. io_uring support is Linux-only.

```sh
go get github.com/miretskiy/dio/v2
```

## Choose an I/O layer

- Use [`ringo`](https://pkg.go.dev/github.com/miretskiy/dio/v2/ringo) for direct,
  low-level io_uring control without raw SQEs, CQEs, `user_data`, or a background
  goroutine. The caller owns submission, reaping, synchronization, and policy.
- Use [`iosched`](https://pkg.go.dev/github.com/miretskiy/dio/v2/iosched) for a
  scheduler that coordinates submissions and exposes completion tickets. It
  selects io_uring when supported and otherwise falls back to synchronous POSIX
  I/O.

## Upgrading from v1

Version 2 adds `/v2` to every import path. It replaces the raw `giouring`
package with `ringo`, whose ownership-oriented API is intentionally not a
drop-in replacement. Existing `align`, `sys`, `netbuf`, and
`iosched` imports must also include `/v2`. The former `mempool` package is
now `alloc`; its pools return owned `*alloc.Buffer` values.

## Packages

| Package | Purpose |
| --- | --- |
| [`ringo`](https://pkg.go.dev/github.com/miretskiy/dio/v2/ringo) | Lifetime-safe low-level io_uring operations and resource ownership |
| [`iosched`](https://pkg.go.dev/github.com/miretskiy/dio/v2/iosched) | A common operation/ticket API over POSIX and io_uring |
| [`align`](https://pkg.go.dev/github.com/miretskiy/dio/v2/align) | Page-aligned allocation and range arithmetic |
| [`alloc`](https://pkg.go.dev/github.com/miretskiy/dio/v2/alloc) | Owned aligned buffers and bounded mmap/slab pools |
| [`sys`](https://pkg.go.dev/github.com/miretskiy/dio/v2/sys) | Portable file allocation, syncing, direct I/O, and hole punching |
| [`netbuf`](https://pkg.go.dev/github.com/miretskiy/dio/v2/netbuf) | TLS read-buffer pre-sizing for golang/go#47672 |

## Ringo: lifetime-safe io_uring

Ringo exposes typed operations without exposing SQEs, CQEs, raw `user_data`, or
manual completion-queue advancement:

```go
ring, err := ringo.New(ringo.WithDepth(256))
if err != nil {
    return err
}
defer ring.Close()

handle, err := ring.Push(ringo.Read(ringo.FileFD(f), buf, offset))
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
until `Ring.Close`. `Close` always closes the ring and never waits, but reap
every pushed operation first: closing with work still pending reports
`ringo.ErrPending` and permanently retains the ring and those operands, because
the kernel may still be using them and never reports when it stops.
Retention does not prevent access through another slice or pointer alias:
callers must not modify memory the kernel may read or access memory the kernel
may write while an operation is active. Callers also serialize calls on the
same ring, avoid explicitly closing retained files, submit pushed work, reap
completions, and handle short I/O.

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
copies the contents into a larger mapping otherwise. `AllocAligned` and
`GrowAligned` take `int64` sizes and panic on allocation failure.
Mapped memory is not managed by the garbage collector and must be released with `FreeAligned`, passing the
original base and capacity (a zero-length reslice is fine).

## Owned buffers and pools

`alloc.Buffer` owns a reference to aligned memory. `Bytes()` exposes a view;
keep the reference alive through every use of that view, including pending I/O.
`TryInc()` acquires another reference and `Unpin()` releases one. The last
reference returns pooled memory or unmaps a standalone allocation. Released
buffer objects cannot be pinned again, even after their storage is reused.
Extra `Unpin` calls after release panic.

`alloc.NewBuffer(size)` creates a standalone mapping. Allocation failures panic;
there is no automatic fallback from an exhausted pool to standalone memory.
Pool constructors return errors for invalid configuration.

`MmapPool` lends whole slabs with a fixed capacity. `Acquire(ctx)` waits for a
slab or context cancellation; `TryAcquire()` returns `ErrExhausted` instead of
waiting. Both return `ErrClosed` after shutdown.

```go
pool, err := alloc.NewMmapPool(1<<20, 32, alloc.WithLazyInitSlabs(32))
if err != nil {
    return err
}
defer pool.Close()

buf, err := pool.Acquire(ctx)
if err != nil {
    return err
}
defer buf.Unpin()
copy(buf.Bytes(), payload)
```

`WithLazyInitSlabs(n)` defers mapping up to n slabs. Returned mappings are reused
before mapping unused slots. Pass `context.Background()` to wait indefinitely.
Cancellation observed before reservation returns `ctx.Err()`; it does not
interrupt mapping or revoke an acquired buffer.

`SlabPool` divides a contiguous backing buffer into fixed-size slots. Its
`TryAcquire()` returns an owned `*Buffer` or `ErrExhausted`; it never waits for
another borrower. `NewSlabPool(totalSize, slotSize)` creates the backing memory.
`NewSlabPoolFrom(backing, slotSize)` instead retains a reference to an existing
`*Buffer`, including one acquired from a `MmapPool`. The caller keeps its own
reference; closing the slab pool releases the pool's reference.

A complete slab can be registered as one io_uring fixed buffer. `RawData()`
provides a view for registration, not an allocation to free or a way to access
borrowed slots.

```go
slab, err := alloc.NewSlabPool(64<<20, 1<<20)
if err != nil {
    return err
}
defer slab.Close()

sched, err := iosched.NewURingScheduler(
    iosched.WithRingDepth(256),
    iosched.WithDMASlab(slab),
)
if err != nil {
    return err
}
defer sched.Close() // unregister before closing the slab

buf, err := slab.TryAcquire()
if err != nil {
    return err
}
defer buf.Unpin()

ticket, err := sched.Submit(iosched.WriteFixedOp(f, buf.Bytes(), offset))
if err != nil {
    return err
}
_, err = ticket.Wait() // finish I/O before Unpin or reuse
```

Pools must outlive their acquired buffers. Finish acquisitions and `Unpin`
calls before `Close`; shutdown must not race pool use. Close a registered
scheduler before its slab pool, and a slab pool before its backing mmap pool.

Streaming writers belong to the caller. Reuse a bounded number of buffers after
their I/O completes instead of accumulating an entire stream in memory. A cache
can retain a buffer reference for a resident value while readers hold their own
references. Synchronize lookup-and-pin with removal; remove an entry before
dropping the cache reference. Allocation does not decide cache admission or
whether an entry is still current.

The former `MmapBuffer` is now `alloc.Buffer`, constructed by `alloc.NewBuffer`
or pool acquisition. `NewBorrowedBuffer`, `AlignedBuffer`, the `Allocator`
interface, and raw-slice `Alloc`/`Free` pool APIs are removed. `SlabPool.Acquire`
is now `TryAcquire`; `NewSlabPoolFrom` takes an owned buffer rather than a slice.

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
n, err := ticket.Wait()
if err != nil {
    return err // execution failed
}
_ = n
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

Each io_uring coordinator runs on a dedicated OS thread. `WithCoordinatorCPU(n)`
requests affinity to CPU `n`; the default leaves affinity unchanged. Affinity
setup is best effort: failures are logged and the loop continues. Pinning does
not reserve a CPU or set kernel-worker or interrupt affinity. The thread exits
with the coordinator so restricted affinity cannot leak into unrelated Go work.

The scheduler does not provide application-level backpressure: `Submit` never
blocks or refuses work for lack of capacity. Callers that need to bound what
they submit should own the semaphore, queue, or buffer pool that defines it.
Submitted buffers must remain valid until the ticket completes.

### In-flight budget

The io_uring backend does bound what it hands to the device. Queueing more than
the device needs to reach its ceiling adds only latency (by Little's law, the
device queue's latency is what is in flight divided by throughput), and a deep
queue of writes makes every read wait behind it. So reads and writes each keep
at most a latency goal of device time in flight; the rest waits in the
scheduler, oldest first within each class, and a read never waits for the write
budget. An operation costs `max(bytes / bandwidth, 1 / IOPS)` at its class's
limits; a linked chain costs its first operation.

| Option | Default |
|---|---|
| `WithReadBandwidth(bytesPerSecond)` | 2.56 GB/s |
| `WithReadIOPS(iops)` | 500k |
| `WithWriteBandwidth(bytesPerSecond)` | 1.22 GB/s |
| `WithWriteIOPS(iops)` | 83k (writes into freshly allocated space) |
| `WithLatencyGoal(d)` | 1.5 ms |
| `WithoutIOBudget()` | — places work as soon as it fits in the ring |

The defaults were measured on an AWS m7gd.8xlarge instance store. On another
disk, measure them with `scripts/disk-model.sh DIR` (fio, about 10 minutes):
take the 4 KiB IOPS and the 1 MiB bandwidth of each class, and a latency goal at
least as large as the in-flight time at which the depth sweep reaches its
ceiling. Leave margin above that knee: the scheduler takes some time to place
the next operation after one completes, which fio does not. On the m7gd disk
the knee was 0.86 ms, yet a 1 ms goal left the device 3% idle under a mixed
read/write load, while 1.5 ms matched the throughput of no budget at a seventh
of its device latency. The script also checks the two assumptions the model makes: that IOPS
and bandwidth are independent limits, and that reads and writes reach their
ceilings together. Limits set too low leave a faster disk underused; too high,
they let the device queue grow.

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
`offset + int64(n)`, where `n` is the count `Ticket.Wait` returned.

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
    unix.O_CREAT|unix.O_RDWR,
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
Durable writes on the same file whose writes complete before the next
`fdatasync` is issued share it (group commit), whether or not they were
coalesced.

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
