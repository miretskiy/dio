//go:build linux

package iosched_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/iosched"
	"github.com/miretskiy/dio/sys"
)

func TestURing_ReusedSQEWriteAfterFdatasync(t *testing.T) {
	s := newURingSched(t)

	path := filepath.Join(t.TempDir(), "direct.dat")
	f, err := sys.CreateDirect(path, sys.FlDirectIO)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	payload := align.AllocAligned(align.BlockSize)
	t.Cleanup(func() { align.FreeAligned(payload) })
	for i := range payload {
		payload[i] = byte(i)
	}

	res := submitResult(t, s, iosched.WriteOp(f, payload, 0))
	require.NoError(t, res.Err)
	require.Equal(t, len(payload), res.N)

	res = submitResult(t, s, iosched.FdatasyncOp(f))
	require.NoError(t, res.Err)

	res = submitResult(t, s, iosched.WriteOp(f, payload, int64(len(payload))))
	require.NoError(t, res.Err)
	require.Equal(t, len(payload), res.N)
}

func TestURing_ConcurrentDirectWriteAndFdatasync(t *testing.T) {
	const (
		workers = 16
		rounds  = 64
	)
	s := newURingSched(t, iosched.URingConfig{RingDepth: 32})

	path := filepath.Join(t.TempDir(), "parallel-direct.dat")
	f, err := sys.CreateDirect(path, sys.FlDirectIO)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	payloads := make([][]byte, workers)
	for worker := range workers {
		payload := align.AllocAligned(align.BlockSize)
		for i := range payload {
			payload[i] = byte(worker + i)
		}
		payloads[worker] = payload
		t.Cleanup(func() { align.FreeAligned(payload) })
	}

	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			payload := payloads[worker]
			for round := range rounds {
				offset := int64((worker*rounds + round) * align.BlockSize)
				ticket, err := s.Submit(iosched.WriteOp(f, payload, offset).Link(iosched.FdatasyncOp(f)))
				if err != nil {
					errs <- fmt.Errorf("worker %d round %d submit: %w", worker, round, err)
					return
				}
				ticket.Wait()

				r0 := ticket.Op.Result
				if r0.Err != nil {
					ticket.Release()
					errs <- fmt.Errorf("worker %d round %d write: n=%d err=%w", worker, round, r0.N, r0.Err)
					return
				}
				if r0.N != len(payload) {
					ticket.Release()
					errs <- fmt.Errorf("worker %d round %d write count: got %d want %d", worker, round, r0.N, len(payload))
					return
				}
				if err := ticket.Error(); err != nil {
					ticket.Release()
					errs <- fmt.Errorf("worker %d round %d linked chain: %w", worker, round, err)
					return
				}
				ticket.Release()
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}
