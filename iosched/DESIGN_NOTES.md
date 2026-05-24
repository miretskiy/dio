# iosched design notes

Decisions and non-decisions, captured so we don't relitigate them.

## Coordinator model

A single coordinator goroutine owns the io_uring ring exclusively. Producers
call `Submit(Op)`, which copies the root `Op` into a pooled `Ticket`, pushes
that ticket onto an intrusive lock-free MPSC Treiber stack, and rings a
buffered(1) doorbell with a non-blocking send.

The submit path has no mutex and allocates no wrapper nodes. The only
intentional allocation on the linked-op path is `Op.Link(next)`, which stores
`next` behind `Op.linked`. Single-op submissions stay as value copies in the
pooled ticket.

The coordinator drains the stack with `stagingHead.Swap(nil)`, reverses the
intrusive list to restore FIFO-ish order for the current wave, appends it to a
single local ticket list, places as many complete tickets as fit into free
SQEs, then calls `SubmitAndWait(1)` and reaps CQEs. A linked chain is
all-or-nothing: if it is larger than ring depth, the ticket fails immediately;
if it merely does not fit in the currently free SQE slots, it stays on the
coordinator-local list until completions free space.

`Close` is not an admission barrier. Callers must stop submitting before
closing the scheduler. During close, the coordinator fails tickets that have
not yet been placed and continues reaping already-submitted SQEs so the kernel
is no longer using their buffers/files before the ring exits.

## Ticket lifetime

`Submit` returns a `*Ticket`. Callers wait with `Ticket.Wait`, inspect
`ticket.Op.Result` or `ticket.Error()`, then call `Ticket.Release`. The
intended usage is:

```go
ticket, err := sched.Submit(op)
if err != nil {
	return err
}
defer ticket.Release()
ticket.Wait()
return ticket.Error()
```

`Release` is nil-safe and idempotent. It also guards the pool against abandoned
tickets: if `pending > 0`, it only clears `t.sched` and deliberately does not
return the ticket to the pool or clear `Op.f`, because the kernel may still be
using the file descriptor and buffers reachable from the op chain.

`Ticket.Error` returns the first non-nil error in linked-chain order. It does
not use `errors.Join`; this matches io_uring's fail-fast behavior where later
linked SQEs often report `ECANCELED` after an earlier failure.

## Backpressure

The scheduler does not implement application-level backpressure. `Submit` is a
non-blocking handoff into the lock-free staging stack; once the scheduler is
open, it accepts the ticket and lets the coordinator pace placement into the
kernel ring.

This is deliberate policy separation. The right cap depends on payload size,
memory budget, request class, and caller retry semantics. A caller doing 1 MiB
writes may need a very different concurrency limit than a caller doing 4 KiB
reads. If a program needs admission control, wrap the scheduler with a caller
owned semaphore, token bucket, queue, or retry policy.

The scheduler only owns the mechanism: enqueue tickets cheaply, keep the ring
fed, and complete tickets when the kernel reports CQEs.

## We do not implement: `BatchDelay` / opportunistic coalescing

We considered an opt-in `BatchDelay time.Duration` knob where the coordinator
waits briefly after the first submission so more work can arrive before
issuing the syscall. Benchmarks showed this is net-negative for the current
serial-per-goroutine workload: the coordinator waits while the kernel is idle.

If a future workload genuinely needs deliberate coalescing, design it around
that workload rather than adding a timer to the hot path.

## We do not enable `SQPOLL` by default

`URingConfig.SQPOLL` is exposed but defaults to false. SQPOLL can be slower at
low concurrency because the kernel poller sleeps and must be woken; it is a
workload-specific knob, not a default.

## Things we explicitly punt on

- Multiple coordinators or sharded rings. Single ring ownership is simpler and
  enough for one local device.
- Adaptive batching. The tradeoff should be explicit, not hidden in a feedback
  loop.
- Generic tracing or group-commit durability wrappers. The scheduler stays
  focused on core io_uring mechanics.
