package iosched

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBudgetCost(t *testing.T) {
	f := new(os.File)
	b := defaultBudget()
	kib4, mib := make([]byte, 4096), make([]byte, 1<<20)
	for _, tc := range []struct {
		name  string
		op    Op
		class ioClass
		cost  int64
	}{
		// A 4 KiB read is bound by IOPS (1/500k = 2 us), a 1 MiB read by bandwidth.
		{"4 KiB read", ReadOp(f, kib4, 0), classRead, 2000},
		{"1 MiB read", ReadOp(f, mib, 0), classRead, 409600},
		{"1 MiB readv", ReadvOp(f, [][]byte{mib[:1<<19], mib[1<<19:]}, 0), classRead, 409600},
		{"1 MiB fixed read", ReadFixedOp(f, mib, 0), classRead, 409600},
		{"4 KiB write", WriteOp(f, kib4, 0), classWrite, 12048},
		{"1 MiB write", WriteOp(f, mib, 0), classWrite, 859488},
		{"1 MiB fixed write", WriteFixedOp(f, mib, 0), classWrite, 859488},
		{"fdatasync", FdatasyncOp(f), classOther, 0},
		{"chain costs its first operation", WriteOp(f, mib, 0).Link(ReadOp(f, mib, 0)), classWrite, 859488},
		{"chain led by an unbudgeted operation", FdatasyncOp(f).Link(WriteOp(f, mib, 0)), classOther, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			class, cost := b.cost(&tc.op)
			require.Equal(t, tc.class, class)
			require.Equal(t, tc.cost, cost)
		})
	}

	off := makeSchedulerConfig([]Option{WithoutIOBudget()}).budget
	_, cost := off.cost(&Op{opcode: OpRead, f: f, buf: mib})
	require.Zero(t, cost, "WithoutIOBudget still charged")
}

func TestBudgetOptions(t *testing.T) {
	b := makeSchedulerConfig(nil).budget
	require.NoError(t, b.validate())
	require.Equal(t, defaultBudget(), b)

	b = makeSchedulerConfig([]Option{
		WithReadBandwidth(1), WithReadIOPS(2), WithWriteBandwidth(3), WithWriteIOPS(4),
		WithLatencyGoal(5 * time.Millisecond),
	}).budget
	require.NoError(t, b.validate())
	require.Equal(t, classLimits{bandwidth: 1, iops: 2}, b.limits[classRead])
	require.Equal(t, classLimits{bandwidth: 3, iops: 4}, b.limits[classWrite])
	require.Equal(t, 5*time.Millisecond, b.goal)

	require.NoError(t, makeSchedulerConfig([]Option{WithoutIOBudget()}).budget.validate())
	for _, opt := range []Option{WithLatencyGoal(0), WithReadIOPS(0), WithWriteBandwidth(-1)} {
		require.Error(t, makeSchedulerConfig([]Option{opt}).budget.validate())
	}
}
