package iosched

import (
	"os"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBudgetServiceTime(t *testing.T) {
	f := new(os.File)
	cfg := makeSchedulerConfig(nil)
	kib4, mib := make([]byte, 4096), make([]byte, 1<<20)
	for _, tc := range []struct {
		name                string
		op                  Op
		readTime, writeTime int64
	}{
		{"4 KiB read", ReadOp(f, kib4, 0), 2000, 0},
		{"1 MiB read", ReadOp(f, mib, 0), 409600, 0},
		{"1 MiB readv", ReadvOp(f, [][]byte{mib[:1<<19], mib[1<<19:]}, 0), 409600, 0},
		{"1 MiB fixed read", ReadFixedOp(f, mib, 0), 409600, 0},
		{"4 KiB write", WriteOp(f, kib4, 0), 0, 12048},
		{"1 MiB write", WriteOp(f, mib, 0), 0, 859488},
		{"1 MiB fixed write", WriteFixedOp(f, mib, 0), 0, 859488},
		{"fdatasync", FdatasyncOp(f), 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var readTime, writeTime int64
			bytes := int64(opBytes(&tc.op.operation))
			switch tc.op.kind() {
			case OpRead, OpReadv:
				readTime = cfg.disk.device.reads.serviceTime(bytes)
			case OpWrite, OpWritev:
				writeTime = cfg.disk.device.writes.serviceTime(bytes)
			}
			require.Equal(t, tc.readTime, readTime)
			require.Equal(t, tc.writeTime, writeTime)
		})
	}
	off := NewDiskPolicy(WithoutIOBudget())
	require.Zero(t, off.device.reads.serviceTime(1<<20))
	require.Zero(t, off.device.writes.serviceTime(1<<20))
}

func TestBudgetOptions(t *testing.T) {
	cfg := NewDiskPolicy(WithReadBandwidth(1), WithReadIOPS(2), WithWriteBandwidth(3), WithWriteIOPS(4), WithLatencyGoal(5*time.Millisecond))
	require.Equal(t, deviceIO{latency: 5 * time.Millisecond, bytesPerSecond: 1, iops: 2}, cfg.device.reads)
	require.Equal(t, deviceIO{latency: 5 * time.Millisecond, bytesPerSecond: 3, iops: 4}, cfg.device.writes)
	cfg = NewDiskPolicy(WithLatencyGoal(-1), WithReadIOPS(0), WithWriteBandwidth(-1), WithRejectWriteBacklog(-1),
		WithWriteBacklogLowWatermark(-7))
	require.Zero(t, cfg.device.reads.latency, "a non-positive goal disables pacing")
	require.Zero(t, cfg.device.writes.latency)
	require.Equal(t, DefaultReadIOPS, cfg.device.reads.iops, "a non-positive rate keeps the default")
	require.Equal(t, DefaultWriteBandwidth, cfg.device.writes.bytesPerSecond)
	require.Zero(t, cfg.writeBudget.maxOutstandingBytes, "a non-positive limit disables it")
	require.EqualValues(t, -1, cfg.writeBudget.lowWatermark)
	cfg = NewDiskPolicy(WithLatencyGoal(time.Millisecond), WithoutReadIOBudget())
	require.Zero(t, cfg.device.reads.latency)
	require.Equal(t, time.Millisecond, cfg.device.writes.latency)
	cfg = NewDiskPolicy(WithoutIOBudget(), WithLatencyGoal(time.Millisecond))
	require.Equal(t, time.Millisecond, cfg.device.reads.latency)
	require.Equal(t, time.Millisecond, cfg.device.writes.latency)
}

func TestSchedulerOptionDefaults(t *testing.T) {
	cfg := makeSchedulerConfig([]Option{WithRingDepth(2), WithCoordinatorCPU(-5), WithDiskPolicy(nil)})
	require.EqualValues(t, minRingDepth, cfg.ringDepth, "depth leaves room for the longest chain and the doorbell")
	require.Negative(t, cfg.coordinatorCPU)
	require.NotNil(t, cfg.disk)
	require.NotSame(t, cfg.disk, makeSchedulerConfig(nil).disk, "each scheduler gets its own default policy")
}

// testRing is the ring counter for policy tests that never dispatch.
var testRing ringInFlight

func policyWrite(p *DiskPolicy, bytes int) *submission {
	work := prepareForTest(WriteOp(nil, make([]byte, bytes), 0))
	work.writes = ioCharge{ops: 1, bytes: int64(bytes), serviceTime: p.device.writes.serviceTime(int64(bytes))}
	return work
}

func TestDiskAdmissionLimits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		option DiskPolicyOption
		want   DiskUsage
	}{
		{"bytes", WithRejectWriteBacklog(12), DiskUsage{Bytes: 12}},
		{"ops", WithRejectWriteQueueDepth(2), DiskUsage{Depth: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewDiskPolicy(WithoutIOBudget(), tc.option)
			a, b, next := policyWrite(p, 7), policyWrite(p, 5), policyWrite(p, 1)
			require.True(t, p.admit(a))
			require.True(t, p.admit(b))
			require.Equal(t, DiskStats{}, p.Stats(), "Submit acquires nothing")
			p.account(a)
			p.account(b)
			p.updateAdmission()
			require.False(t, p.admit(next))
			require.Equal(t, tc.want, p.Stats().Writes)
			require.EqualValues(t, 1, next.writes.bytes, "rejection does not mutate request totals")
			p.release(a, &testRing)
			require.True(t, p.admit(next))
			p.account(next)
			p.release(b, &testRing)
			p.release(next, &testRing)
			require.Equal(t, DiskStats{}, p.Stats())
		})
	}
}

func TestDiskAdmissionConcurrent(t *testing.T) {
	p := NewDiskPolicy(WithoutIOBudget(), WithRejectWriteBacklog(8192), WithRejectWriteQueueDepth(64))
	var wg sync.WaitGroup
	works := make([]*submission, 64)
	for i := range works {
		works[i] = policyWrite(p, 1024)
		require.True(t, p.admit(works[i]), "staged work can overshoot before a coordinator observes it")
	}
	require.Equal(t, DiskStats{}, p.Stats())
	for _, work := range works {
		wg.Go(func() { p.account(work); p.updateAdmission() })
	}
	wg.Wait()
	require.EqualValues(t, 64, p.Stats().Writes.Depth)
	require.EqualValues(t, 64*1024, p.Stats().Writes.Bytes)
	require.False(t, p.admit(policyWrite(p, 1)))
	for _, work := range works {
		wg.Go(func() { p.release(work, &testRing) })
	}
	wg.Wait()
	require.Equal(t, DiskStats{}, p.Stats())
	require.True(t, p.admit(policyWrite(p, 1)), "completion reopens the circuit")
}

func TestWriteBacklogRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewDiskPolicy(WithoutIOBudget(), WithRejectWriteBacklog(16),
			WithWriteBacklogLowWatermark(2), WithWriteBandwidth(8), WithRejectReadBacklog(16))
		a, b := policyWrite(p, 12), policyWrite(p, 4)
		p.account(a)
		p.account(b)
		p.updateAdmission() // (16 - 2) / 8 = 1.75 seconds.
		require.False(t, p.admit(policyWrite(p, 1)))
		p.release(a, &testRing)
		p.release(b, &testRing)
		require.Equal(t, DiskStats{}, p.Stats())
		require.False(t, p.admit(policyWrite(p, 1)), "early completion does not cancel the recovery interval")
		read := &submission{reads: ioCharge{ops: 1, bytes: 4}}
		require.True(t, p.admit(read), "reads continue during write recovery")
		p.account(read)
		p.release(read, &testRing)
		mandatory := policyWrite(p, 32)
		mandatory.root.opFlags |= opMustAdmit
		require.True(t, p.admit(mandatory))
		p.account(mandatory)
		p.release(mandatory, &testRing)
		require.Equal(t, DiskStats{}, p.Stats())
		time.Sleep(1750*time.Millisecond - time.Nanosecond)
		require.False(t, p.admit(policyWrite(p, 1)))
		p.updateAdmission() // An observation or rejected arrival cannot extend the pause.
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.True(t, p.admit(policyWrite(p, 1)), "the pause ends with no coordinator activity")
	})
}

func TestCoordinatorRecoveryEstimate(t *testing.T) {
	for _, bytes := range []int{16, 20} {
		t.Run(strconv.Itoa(bytes), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := NewDiskPolicy(WithoutIOBudget(), WithRejectWriteBacklog(16),
					WithWriteBacklogLowWatermark(4), WithWriteBandwidth(8))
				work := policyWrite(p, bytes)
				p.account(work)
				var wg sync.WaitGroup
				for range 8 {
					wg.Go(p.updateAdmission)
				}
				wg.Wait()
				require.False(t, p.admit(policyWrite(p, 1)))
				drain := time.Duration(bytes-4) * time.Second / 8
				time.Sleep(drain - time.Nanosecond)
				require.False(t, p.admit(policyWrite(p, 1)))
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				require.True(t, p.admit(policyWrite(p, 1)))
				p.updateAdmission()
				require.False(t, p.admit(policyWrite(p, 1)), "still-full backlog can start another interval")
				p.release(work, &testRing)
				time.Sleep(drain)
				synctest.Wait()
				require.True(t, p.admit(policyWrite(p, 1)))
			})
		})
	}
}

func TestCoordinatorSkipsAlreadyDrainedBacklog(t *testing.T) {
	p := NewDiskPolicy(WithoutIOBudget(), WithRejectWriteBacklog(16), WithWriteBacklogLowWatermark(0))
	work := policyWrite(p, 16)
	p.account(work)
	p.release(work, &testRing)
	require.True(t, p.admit(policyWrite(p, 1)))
}

func TestDiskServiceTimeDispatch(t *testing.T) {
	p := NewDiskPolicy(WithLatencyGoal(100), WithWriteBandwidth(1e9), WithWriteIOPS(1e9))
	var ring, other ringInFlight
	works := make([]*submission, 8)
	dispatched := 0
	for i := range works {
		work := policyWrite(p, 25)
		works[i] = work
		require.True(t, p.admit(work), "time allowance does not reject queued work")
		p.account(work)
		if p.dispatch(work, &ring) {
			work.state = workIssued
			dispatched++
		}
	}
	require.Equal(t, 4, dispatched)
	require.Equal(t, time.Duration(100), p.Stats().Writes.ServiceTime)
	require.EqualValues(t, 100, ring.writes)

	idle := policyWrite(p, 25)
	p.account(idle)
	require.True(t, p.dispatch(idle, &other), "a ring with nothing in flight never waits on other rings")
	idle.state = workIssued
	require.Equal(t, time.Duration(125), p.Stats().Writes.ServiceTime)
	busy := policyWrite(p, 25)
	p.account(busy)
	require.False(t, p.dispatch(busy, &other), "overcommit is one chain per ring")
	p.release(idle, &other)
	p.release(busy, &other)
	require.Zero(t, other.writes)

	for _, work := range works {
		p.release(work, &ring)
	}
	require.Equal(t, DiskStats{}, p.Stats())
	require.Zero(t, ring.writes)

	oversized := policyWrite(p, 200)
	require.True(t, p.admit(oversized))
	p.account(oversized)
	require.True(t, p.dispatch(oversized, &ring), "an oversized chain runs on an idle ring")
	oversized.state = workIssued
	next := policyWrite(p, 1)
	require.True(t, p.admit(next))
	p.account(next)
	require.False(t, p.dispatch(next, &ring))
	p.completeIO(oversized, &oversized.root, &ring)
	require.Zero(t, ring.writes)
	p.release(oversized, &ring)
	require.True(t, p.dispatch(next, &ring))
	next.state = workIssued
	p.release(next, &ring)
	require.Equal(t, DiskStats{}, p.Stats())
	require.Zero(t, ring.writes)
}

func TestMixedDispatchRollback(t *testing.T) {
	p := NewDiskPolicy(WithLatencyGoal(100))
	ring := ringInFlight{reads: 1, writes: 1} // busy in both classes
	held := &submission{writes: ioCharge{serviceTime: 100}}
	require.True(t, p.dispatch(held, &ring))
	held.state = workIssued
	mixed := &submission{reads: ioCharge{serviceTime: 50}, writes: ioCharge{serviceTime: 50}}
	require.False(t, p.dispatch(mixed, &ring))
	require.Zero(t, p.Stats().Reads.ServiceTime)
	require.Equal(t, time.Duration(100), p.Stats().Writes.ServiceTime)
	require.Equal(t, ringInFlight{reads: 1, writes: 101}, ring, "a failed dispatch charges the ring nothing")
	p.release(held, &ring)
	require.True(t, p.dispatch(mixed, &ring))
	mixed.state = workIssued
	p.release(mixed, &ring)
	require.Equal(t, DiskStats{}, p.Stats())
	require.Equal(t, ringInFlight{reads: 1, writes: 1}, ring)
}

func TestSharedDiskPolicy(t *testing.T) {
	p := NewDiskPolicy(WithLatencyGoal(time.Millisecond), WithWriteBandwidth(1234), WithRejectWriteBacklog(2048), WithoutReadIOBudget())
	a := makeSchedulerConfig([]Option{WithDiskPolicy(p), WithCoordinatorCPU(3), WithRingDepth(64)})
	b := makeSchedulerConfig([]Option{WithDiskPolicy(p)})
	require.Same(t, p, a.disk)
	require.Same(t, p, b.disk)
	require.Equal(t, 3, a.coordinatorCPU)
	require.EqualValues(t, 64, a.ringDepth)
	work := policyWrite(p, 2048)
	require.True(t, a.disk.admit(work))
	p.account(work)
	p.updateAdmission()
	require.False(t, b.disk.admit(policyWrite(p, 1)))
	a.disk.release(work, &testRing)
}

var _ func(...DiskPolicyOption) *DiskPolicy = NewDiskPolicy

func TestRecoveryAndDispatchAreIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewDiskPolicy(WithLatencyGoal(time.Second), WithWriteBandwidth(8),
			WithRejectWriteBacklog(16), WithWriteBacklogLowWatermark(4))
		var ring ringInFlight
		first, second := policyWrite(p, 8), policyWrite(p, 8)
		p.account(first)
		p.account(second)
		p.updateAdmission()
		require.True(t, p.dispatch(first, &ring))
		first.state = workIssued
		require.False(t, p.dispatch(second, &ring))
		require.Equal(t, time.Second, p.Stats().Writes.ServiceTime)
		require.False(t, p.admit(policyWrite(p, 1)))
		p.completeIO(first, &first.root, &ring)
		p.release(first, &ring)
		require.False(t, p.admit(policyWrite(p, 1)))
		require.True(t, p.dispatch(second, &ring), "recovery must not stop admitted work draining")
		second.state = workIssued
		p.completeIO(second, &second.root, &ring)
		p.release(second, &ring)
		time.Sleep(1500 * time.Millisecond)
		synctest.Wait()
		require.True(t, p.admit(policyWrite(p, 1)))
		require.Equal(t, DiskStats{}, p.Stats())
	})
}
