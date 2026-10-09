package iosched

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestPinnedSchedulersHaveIndependentSlotsAndBarriers(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	var allowed unix.CPUSet
	require.NoError(t, unix.SchedGetaffinity(0, &allowed))
	if allowed.Count() < 2 {
		t.Skip("need two allowed CPUs")
	}
	var cpus []int
	for cpu := 0; len(cpus) < 2; cpu++ {
		if allowed.IsSet(cpu) {
			cpus = append(cpus, cpu)
		}
	}
	var scheds []*URingScheduler
	for _, cpu := range cpus {
		s, err := NewURingScheduler(WithCoordinatorCPU(cpu), WithVFiles(1), WithRingDepth(16))
		require.NoError(t, err)
		scheds = append(scheds, s)
		t.Cleanup(func() { require.NoError(t, s.Close()) })
	}
	wait := func(ticket Ticket, err error) {
		t.Helper()
		require.NoError(t, err)
		select {
		case <-ticket.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("ticket did not complete")
		}
		_, err = ticket.Wait()
		require.NoError(t, err)
	}
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "a"), filepath.Join(dir, "b")}
	for i, s := range scheds {
		wait(s.Submit(VOpenatOp(unix.AT_FDCWD, paths[i], unix.O_CREAT|unix.O_RDWR, 0600, 0)))
	}
	type observed struct {
		mask unix.CPUSet
		tid  int
		err  error
	}
	observe := func() (o observed) {
		o.tid = unix.Gettid()
		o.err = unix.SchedGetaffinity(0, &o.mask)
		return
	}
	entered := make(chan observed, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock) // release before scheduler cleanup, including on failure
	tail := []byte("old")
	first, err := SubmitNotify(scheds[0], VWriteOp(0, []byte("aaa"), 0), func(_ int, _ error) {
		entered <- observe()
		<-release
		copy(tail, "new")
	})
	require.NoError(t, err)
	var a observed
	select {
	case a = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not run")
	}
	// This close-containing chain must wait for the earlier callback, not
	// merely the write CQE. Its buffer is changed by that callback before issue.
	seal, err := scheds[0].Submit(VWriteOp(0, tail, 3).Link(VFdatasyncOp(0), VCloseOp(0)))
	require.NoError(t, err)
	other := make(chan observed, 1)
	second, err := SubmitNotify(scheds[1], VWriteOp(0, []byte("bbb"), 0), func(_ int, _ error) { other <- observe() })
	wait(second, err) // another ring progresses while the first callback is held
	b := <-other
	for i, o := range []observed{a, b} {
		require.NoError(t, o.err)
		require.Equal(t, 1, o.mask.Count())
		require.True(t, o.mask.IsSet(cpus[i]))
	}
	require.NotEqual(t, a.tid, b.tid)
	unblock()
	wait(first, nil)
	wait(seal, nil)
	got, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	require.Equal(t, "aaanew", string(got))
	buf := make([]byte, 3)
	wait(scheds[1].Submit(VReadOp(0, buf, 0).Link(VCloseOp(0))))
	require.Equal(t, "bbb", string(buf), "slot zero belongs to a different file on each ring")
	// Closing one ring's slot leaves the same index reusable on that ring.
	wait(scheds[0].Submit(VOpenatOp(unix.AT_FDCWD, paths[1], unix.O_RDONLY, 0, 0).Link(VReadOp(0, buf, 0))))
	require.Equal(t, "bbb", string(buf))
	wait(scheds[0].Submit(VCloseOp(0)))
}

func TestCoordinatorCPUValidation(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	s, err := NewURingScheduler(WithCoordinatorCPU(-2))
	require.Error(t, err)
	require.Nil(t, s)
}

// Affinity is an optimization: an unavailable CPU must not prevent I/O.
func TestCoordinatorAffinityFailureDoesNotStopIO(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	s, err := NewURingScheduler(WithCoordinatorCPU(int(^uint(0) >> 1)))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	f, err := os.CreateTemp(t.TempDir(), "affinity")
	require.NoError(t, err)
	defer f.Close()
	ticket, err := s.Submit(WriteOp(f, []byte("still works"), 0))
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.NoError(t, err)
	got, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	require.Equal(t, "still works", string(got))
}
