# iosched design notes

Decisions and non-decisions, captured so we don't relitigate them.

## Coordinator model

A single coordinator goroutine owns the io_uring ring exclusively. Producers
deliver `*submitState` values to it via a buffered(1) `signal` channel (fast
path, lock-free) with a mutex-protected `overflow.queue` slow path. The
coordinator drains both each cycle, places SQEs, calls `io_uring_enter`,
and reaps CQEs. A single goroutine owning the ring is what io_uring expects;
multi-coordinator designs (sharded rings) are possible but unnecessary at
our scale.

## Backpressure: `MaxInflightOps`

A scheduler-wide atomic counter, checked at `Submit` time. On overflow
returns `ErrTooBusy` immediately — `Submit` never blocks.

We considered a counter-with-cond-var design that *blocks* `Submit` when
the budget is exhausted. Rejected because:

- It coupled correctness to `Wait` being called (callers who forgot to
  Wait could permanently consume budget).
- There was no clean way to cancel a blocked `Submit` (no context, no
  per-call timeout).
- Caller-side policy (retry, backoff, give up) belongs at the caller, not
  inside the scheduler.

Returning `ErrTooBusy` lets the caller decide. Default `MaxInflightOps=0`
means unlimited — the scheduler does no backpressure of its own.

## We do not implement: `BatchDelay` / opportunistic coalescing

We seriously considered, prototyped, and benchmarked an opt-in
`BatchDelay time.Duration` knob: the coordinator would wait up to
`BatchDelay` after collecting the first batch to give more submissions a
chance to arrive before issuing the syscall. Trade submit-side latency
for syscall efficiency.

The benchmark (concurrent goroutines each doing serial
`Submit → <-sub.Done()`) showed `BatchDelay` is *strongly net-negative*
for our workload:

| goroutines | no delay (ns/op) | 100µs delay (ns/op) | regression |
|---|---|---|---|
| 1 | 2517 | 1,064,825 | 422× |
| 8 | 1793 | 135,049 | 75× |
| 128 | 1222 | 8826 | 7× |

Throughput tells the same story:

| goroutines | no delay (MB/s) | 100µs delay (MB/s) |
|---|---|---|
| 128 | 3353 | 464 |

Yes, `ops/syscall` improved (88 → 127 at 128 goroutines, perfect batching
at lower counts), but pipeline depth collapsed. The coordinator was waiting
on the timer while the kernel sat idle. We trade *throughput* for syscall
density, not (as one might assume) *latency* for throughput.

The benefit would show up only with a fundamentally different producer
model: a goroutine that submits many concurrent ops and does CPU work
between submissions, rather than waiting for each completion. We do not
target that workload (kura's S3 backend uses one goroutine per request,
each doing blocking IO).

If a future workload genuinely needs this, design from scratch — not
necessarily by reintroducing `BatchDelay`. A shared completion queue
(producer-consumer model where the kernel/coordinator publishes completed
submissions onto a single channel for any goroutine to pick up) would be
the more honest answer for fan-out reaping.

## We do not enable `SQPOLL` by default

`URingConfig.SQPOLL` is exposed but defaults to false. Benchmarks show
SQPOLL is **3× slower** at low concurrency (1–2 goroutines) due to the
kernel poller falling asleep and requiring `IORING_ENTER_SQ_WAKEUP` to
restart. It only catches up at 64+ goroutines, where it's marginally
better but not enough to recommend default-on.

## Per-Submission `done` channel

Each `Submission` allocates its own `chan struct{}` when constructed via
`PrepareWaitableSubmission`. This is the largest per-Submit allocation
(~96 B). We've considered:

- **Lazy allocation** (context.Context-style): allocate `done` only when
  caller actually waits. Saves the chan for fire-and-forget callers. Not
  done; fire-and-forget isn't our target case.
- **Shared completion queue**: one chan per scheduler, all completions
  pushed onto it. Drops per-Submission chan entirely but changes the API
  model substantially. Not done.

For now, per-Submission chan is the right balance: clear caller semantics
(`<-sub.Done()` composes with select and context), small but bounded cost.

## Things we explicitly punt on

- **Multiple coordinators** (sharded rings). Single ring is fine for one
  device; multi-device would be a different architecture.
- **Adaptive batching** (measure recent batch size, tune delay). Too clever;
  static config is honest about the tradeoff.
- **Lock-free overflow queue** (Treiber stack with sync.Pool nodes). The
  overflow path is rare; the mutex cost there is invisible in practice.
