# iosched design notes

Decisions and non-decisions, captured so we don't relitigate them.

## Coordinator model

There is no permanent coordinator goroutine. Concurrent callers use a
group-commit leader election: each `Submit` appends a small request to the
pending queue; if no leader is active, that caller swaps the queue into a
private batch, owns the io_uring ring until the batch completes, then publishes
completion for every request under the mutex.

The important property is still single ownership of the ring. At any instant,
exactly one leader touches SQ/CQ state, `inflight`, or `freeSlots`. Other
callers wait on the condition variable and either join the next batch or return
after their request is published complete.

The tradeoff is that the elected leader pays the coordination cost directly.
Its observed completion latency can be worse than followers, and worse than the
previous async-coordinator model, because it submits, reaps, and publishes
completion for the whole batch before returning from its own `Submit`. The
benchmarks measure average API cost well, but they do not separate leader and
follower completion latency; if that tail behavior matters, the benchmark
should record those roles independently.

## Backpressure: `MaxInflightOps`

A scheduler-wide atomic counter, checked at `Submit` time. On overflow
returns `ErrTooBusy` immediately — `Submit` never blocks.

We considered a counter-with-cond-var design that *blocks* `Submit` when
the budget is exhausted. Rejected because:

- It would let callers park inside `Submit` indefinitely and provide no clear
  cancellation story.
- There was no clean way to cancel a blocked `Submit` (no context, no
  per-call timeout).
- Caller-side policy (retry, backoff, give up) belongs at the caller, not
  inside the scheduler.

Returning `ErrTooBusy` lets the caller decide. Default `MaxInflightOps=0`
means unlimited — the scheduler does no backpressure of its own and does not
touch the global in-flight atomic counter on the hot path.

## We do not implement: `BatchDelay`

We seriously considered, prototyped, and benchmarked an opt-in
`BatchDelay time.Duration` knob: the scheduler would wait up to
`BatchDelay` after collecting the first batch to give more submissions a
chance to arrive before issuing the syscall. Trade submit-side latency
for syscall efficiency.

The benchmark (concurrent goroutines each doing serial
`Submit`) showed `BatchDelay` is *strongly net-negative*
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
at lower counts), but pipeline depth collapsed. The scheduler was waiting
on the timer while the kernel sat idle. We trade *throughput* for syscall
density, not (as one might assume) *latency* for throughput.

The benefit would show up only with a fundamentally different producer
model: a goroutine that submits many concurrent ops and does CPU work
between submissions, rather than waiting for each completion. We do not
target that workload (kura's S3 backend uses one goroutine per request,
each doing blocking IO).

The group-commit path still batches opportunistically: all requests already in
the pending queue when a leader swaps the batch share the same ring drain. It
does not sleep to manufacture larger batches.

We also tried a timer-free coalescing yield (`runtime.Gosched`) plus one
bounded post-reap pending drain while the leader's own request was incomplete.
That recovered syscall density, but it made the measured API latency worse
across the compact benchmark matrix, so the code keeps the immediate swap.

## We do not enable `SQPOLL` by default

`URingConfig.SQPOLL` is exposed but defaults to false. Benchmarks show
SQPOLL is **3× slower** at low concurrency (1–2 goroutines) due to the
kernel poller falling asleep and requiring `IORING_ENTER_SQ_WAKEUP` to
restart. It only catches up at 64+ goroutines, where it's marginally
better but not enough to recommend default-on.

## Per-Submit request allocation

The scheduler intentionally allocates one small internal request per `Submit`.
There is no `sync.Pool`: pooled request handles made ownership harder to reason
about and hid a real lifetime bug when one caller returned a handle while a
leader still had it in its active batch. The plain allocation is easier to audit,
and the request becomes unreachable as soon as the completed batch is cleared.

## Things we explicitly punt on

- **Multiple coordinators** (sharded rings). Single ring is fine for one
  device; multi-device would be a different architecture.
- **Adaptive batching** (measure recent batch size, tune delay). Too clever;
  static config is honest about the tradeoff.
- **Lock-free pending queue** (Treiber stack with pooled nodes). The mutex is
  simpler, and submit-side contention is not the bottleneck for this scheduler.
