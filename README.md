# dio

`dio` provides direct I/O and io_uring building blocks for Go. Its Linux-only
`ringo` package is a lifetime-safe, policy-free io_uring interface: it exposes
typed operations while retaining every Go object the kernel can access through
the final completion. For applications that want scheduling rather than direct
ring control, `iosched` provides a higher-level operation and ticket API with
io_uring and synchronous POSIX backends.

The supporting packages provide aligned memory, fixed-size buffer allocators,
reusable handle-based lists, portable file operations, and a targeted TLS
read-buffer optimization.

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
now `alloc`; its allocators return `alloc.Memory` lease handles.

## Packages

| Package | Purpose |
| --- | --- |
| [`ringo`](https://pkg.go.dev/github.com/miretskiy/dio/v2/ringo) | Lifetime-safe low-level io_uring operations and resource ownership |
| [`iosched`](https://pkg.go.dev/github.com/miretskiy/dio/v2/iosched) | A common operation/ticket API over POSIX and io_uring |
| [`align`](https://pkg.go.dev/github.com/miretskiy/dio/v2/align) | Page-aligned allocation and range arithmetic |
| [`alloc`](https://pkg.go.dev/github.com/miretskiy/dio/v2/alloc) | Owned aligned buffers and extent and slab allocators |
| [`collections`](https://pkg.go.dev/github.com/miretskiy/dio/v2/collections) | Lists with reusable handles and a concurrent sharded map with per-shard state |
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
Mapped memory is not managed by the garbage collector. Pass the original
allocation to `FreeAligned` when finished.

## Owned memory and allocators

`alloc.Memory` owns page-aligned storage for direct I/O. Use `alloc.NewMemory(size)`
for a standalone allocation, or choose an allocator for reusable storage:

| Allocator | Purpose |
| --- | --- |
| `ExtentAllocator` | Borrow whole allocations from a bounded pool |
| `SlabAllocator` | Subdivide one region into fixed-size chunks |

Both implement `Allocator`: `Acquire(ctx)` waits for capacity, `TryAcquire()`
returns `ErrExhausted` when none is free, and `Close()` releases retained storage.
Each acquisition returns one contiguous, zeroed `Memory` of the allocator's unit
size: one chunk or one extent. Backing is mapped up front; pages are committed on
first use unless `WithWarmup()` is supplied. `WithoutZeroing()` skips zeroing
for callers that overwrite every byte, such as direct-I/O reads. The slab
allocator defaults to 4 KiB chunks; `WithChunkSize` changes the size.
`WithBacking(region)` subdivides an existing page-aligned region instead of
mapping one; the caller keeps it valid until the allocator is closed and its
memory released.

```go
allocator, err := alloc.NewSlabAllocator(
    64<<20, // one 64 MiB slab
    alloc.WithChunkSize(128<<10),
)
if err != nil {
    return err
}
defer allocator.Close()

mem, err := allocator.Acquire(ctx)
if err != nil {
    return err
}
defer mem.Unpin()
copy(mem.Bytes(), payload)
```

`Memory` is a small value naming one lease; copies name the same lease, and
acquiring memory allocates nothing on the Go heap. Each lease carries a
generation, checked by every operation, so a handle kept past its lease fails
`TryInc` and panics on `Unpin` even after the memory is lent again.
Access the storage through `Memory.Bytes()`, which includes allocator rounding.
Release memory with `Unpin()` after use, including pending I/O. It returns
`true` only when this call released the final reference and returned storage to
its owner. Callers may ignore the result; method values used as `func()` callbacks
need a wrapper such as `func() { mem.Unpin() }`.
`TryInc()` retains a reference when sharing memory.
`WithReleaseHook(func(data []byte))` runs a callback on every final `Unpin`,
before the memory returns to the allocator. This lets callers account for
storage until the last borrower releases it.

`alloc.Buffer` is a byte stream in the spirit of `bytes.Buffer`, growing by
acquiring allocator units rather than reallocating. It supports `Read`, `Write`,
`ReadFrom`, and `WriteTo`. Growth never moves existing bytes and returns allocation
errors rather than waiting for capacity; a failed `Write` writes nothing.
Once every byte has been read, the buffer reuses its memory from the start.
`Reset()` discards contents while retaining storage; `Release()` also drops the
buffer's references. `Len()` counts unread bytes; `Cap()` counts held bytes.

```go
buffer := alloc.NewAllocatingBuffer(allocator)
defer buffer.Release()
if _, err := buffer.ReadFrom(source); err != nil {
    return err
}
_, err = buffer.WriteTo(destination)
```

`RawAllocations()` exposes the buffer's `Memory`; obtain an extra reference with
`TryInc()` before retaining one independently. Reference counts protect lifetime,
not contents: finish reading shared bytes before the buffer reuses them.

Both concrete allocators expose `Capacity()` and `Available()` byte snapshots.
Free capacity excludes memory still pinned by readers, writers, or I/O after a
cache has dropped its reference; it becomes available on the final release. Snapshots
do not reserve memory; acquisition remains authoritative. Slab availability scans
its bitmaps, avoiding a shared accounting counter on each allocation/release.
Return all acquired memory before closing its allocator. See the
[`alloc` API documentation](https://pkg.go.dev/github.com/miretskiy/dio/v2/alloc)
for configuration and ownership details.

For fixed-buffer I/O, pass regions to the scheduler with
`iosched.WithDMABuffers(regions...)`, for example `mem.Bytes()`. The caller keeps
them mapped until the scheduler closes.

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

`Submit` validates the whole chain and checks the shared rejection state before
enqueueing. When a coordinator has closed admission, Submit returns `ErrRejected`
immediately, with no ticket or executed operations. The caller retains its buffers and descriptors and decides whether
to retry. Admitted buffers must remain valid until the ticket completes.

### Shared disk admission

`DiskPolicy` holds the device model, the admission limits and the atomic
counters. Construct it with `NewDiskPolicy(...DiskPolicyOption)` and share one
pointer across the rings serving a device with `WithDiskPolicy(policy)`. Device
and admission options exist only on `NewDiskPolicy`; ring depth, CPU and file
table settings are scheduler options. Without a policy, each scheduler uses its
own `NewDiskPolicy()`.

Every data operation has a modeled service time, `max(bytes / bandwidth, 1 / IOPS)`;
a linked chain sums its operations. Both admission and dispatch are expressed in
this one unit, per class (reads, writes), so a limit covers bytes and operation
count at once and reads directly as time.

Admission: a coordinator adds a submission's modeled time to its class's
backlog when it accepts the submission and removes it when the ticket
completes, including any durable sync. Reaching `WithWriteBacklogLimit(d)`
(or `WithReadBacklogLimit(d)`) closes that class's admission, so an admitted
write queues for at most about `d`. Submit only observes a shared rejection
flag; it does no accounting or rollback. Staged requests can overshoot the
limit before a coordinator closes admission.

Separately, the coordinator reserves modeled service time before dispatch and
returns it as each I/O completes. Work that does not fit queues without being
rejected, in order within its class; unbudgeted reads can pass queued writes.
A coordinator always dispatches the next chain of a class when it has none of
that class in flight itself, whatever its size and whatever other rings hold.
So a blocked ring always has its own completion coming to wake it: there are
no timers, polling or cross-ring notifications, every wait is bounded by the
ring's own I/O, and the shared cap is exceeded by at most one chain per ring.

Non-positive values never fail: a non-positive rate keeps its default, and a
non-positive limit or latency goal disables it.

| Option | Default |
|---|---|
| `WithReadBandwidth(bytesPerSecond)` | 2.56 GB/s |
| `WithReadIOPS(iops)` | 500k |
| `WithWriteBandwidth(bytesPerSecond)` | 1.22 GB/s |
| `WithWriteIOPS(iops)` | 83k |
| `WithLatencyGoal(d)` | 1.5 ms per class |
| `WithReadBacklogLimit(d)`, `WithWriteBacklogLimit(d)` | disabled |
| `WithWriteBacklogRecovery(target)` | negative (timed recovery disabled) |

`WithoutIOBudget()` disables both dispatch caps; `WithoutReadIOBudget()`
disables only the read cap. Without timed recovery a backlog limit closes
admission until coordinators observe the backlog below it again.
`MustAdmit()` exempts a submission from every throttle: it is never rejected,
never waits for the service-time allowance, and is not charged, so it is
excluded from `Stats()`.

With `WithWriteBacklogRecovery(target)`, reaching the write limit instead
closes write admission for `backlog - target`: the modeled time to drain to
target. For a one-second limit and a 200 ms target the pause is 800 ms. Submit
compares the clock with the deadline only while admission is closed, so the
pause ends on time even when every coordinator is idle, with no timer. Active
pauses are not extended; a backlog still at the limit after expiry starts
another. Reads and `MustAdmit` continue. Zero targets a full drain. The model
is an estimate: a device also serving reads drains writes more slowly than its
write rating, so more than target typically remains at expiry. Keep the
dispatch cap enabled to limit device queue pressure while admitted work drains.

The default device rates were measured on an AWS m7gd.8xlarge instance store.
Use `scripts/disk-model.sh DIR` to measure another device. Service time is a
model, not measured latency or a bandwidth rate limiter. `DiskPolicy.Stats()`
reports each class's backlog (counted while a backlog limit is set) and
dispatched service time (counted while a dispatch cap is set) across its rings.
Staged submissions are not yet counted.

Accounting and release operate on prepared submissions inside the coordinator.
`SubmitNotify` carries its callback into preparation; only the prepared submission
owns the runtime callback. `completion` contains the ticket result and signal.
`Op` holds its followers in one flat slice, allocated once per `Link` or
`HardLink` call and never modified afterwards.

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

The ticket completes after `fdatasync`. The final write of a linked chain can
also be durable, sharing its sync with other durable writes on the same file:

```go
op := iosched.WriteOp(dataFile, payload, dataOffset).
    Link(iosched.WriteOp(indexFile, header, indexOffset).Durable())
ticket, err := sched.Submit(op)
```

Here the data write must succeed before the index write runs, and the ticket
waits for the index file's sync. This does not sync the data file.

A durable write may also be followed by other operations. The scheduler then
inserts an `fdatasync` right after the write, linked the way the write links to
its successor, so the following operations run after the flush:

```go
op := iosched.VWriteOp(slot, payload, 0).Durable().
    HardLink(iosched.VCloseOp(slot))
```

The inserted sync occupies one of the chain's eight entries and reports into
the write's `Result`. Call `Durable` on the write before linking; on a chain it
marks the root. A chain holds at most eight operations on every backend; the
io_uring scheduler raises a smaller ring depth to fit one beside its doorbell.

Separate submissions are otherwise unordered. The one barrier is `DrainOp`,
which waits for previously accepted work on a regular file. Virtual-file work is
ordered only by links and completion waits. Racing `Submit` calls do not create
an application-visible order.

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

Wait for a close-containing ticket before reusing its slot. `AllocateVFD` on a
root `VOpenatOp` picks a free slot; its followers name the same slot with
`AllocateVFD`, and `Ticket.Wait` returns the index. A successful virtual close
frees an allocated slot. Hard-link the close (`open.Link(write.HardLink(close))`)
so it runs even when the write fails; with a soft link a failed write cancels
the close and the caller must submit `VCloseOp(index)` itself. Contiguous,
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
