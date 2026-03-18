//go:build linux

package iosched_test

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/iosched"
)

func newURingSched(t *testing.T, cfg ...iosched.URingConfig) *iosched.URingScheduler {
	t.Helper()
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	c := iosched.URingConfig{RingDepth: 64}
	if len(cfg) > 0 {
		c = cfg[0]
	}
	s, err := iosched.NewURingScheduler(c)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func newURingIO(t *testing.T) *iosched.BlockingIO {
	t.Helper()
	s := newURingSched(t)
	return iosched.NewBlockingIO(s)
}

func writeUringFile(t *testing.T, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "uring.dat")
	require.NoError(t, os.WriteFile(path, data, 0644))
	return path, data
}

func openRW(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
}

// ── Basic Submit/Wait ─────────────────────────────────────────────────────────

func TestURing_Submit_Read(t *testing.T) {
	s := newURingSched(t)
	path, data := writeUringFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	ticket, err := s.Submit([]iosched.Op{iosched.ReadOp(f, buf, 0)})
	require.NoError(t, err)

	var res [1]iosched.Result
	n, err := s.Wait(ticket, res[:])
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, res[0].Err)
	require.Equal(t, 4096, res[0].N)
	require.Equal(t, data, buf)
}

func TestURing_Submit_Write(t *testing.T) {
	s := newURingSched(t)
	path := filepath.Join(t.TempDir(), "write.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)

	ticket, err := s.Submit([]iosched.Op{iosched.WriteOp(f, payload, 0)})
	require.NoError(t, err)

	var res [1]iosched.Result
	_, err = s.Wait(ticket, res[:])
	require.NoError(t, err)
	require.NoError(t, res[0].Err)
	require.Equal(t, 4096, res[0].N)

	got := make([]byte, 4096)
	ticket2, err := s.Submit([]iosched.Op{iosched.ReadOp(f, got, 0)})
	require.NoError(t, err)
	_, err = s.Wait(ticket2, res[:])
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// ── Group commit: submit multiple tickets, wait on all ────────────────────────

func TestURing_GroupCommit(t *testing.T) {
	const chunks = 16
	s := newURingSched(t)

	path := filepath.Join(t.TempDir(), "gc.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, chunks*4096), 0644))
	f := openRW(t, path)

	payloads := make([][]byte, chunks)
	tickets := make([]iosched.Ticket, chunks)
	for i := range chunks {
		payloads[i] = make([]byte, 4096)
		_, _ = rand.Read(payloads[i])
		var err error
		tickets[i], err = s.Submit([]iosched.Op{
			iosched.WriteOp(f, payloads[i], int64(i*4096)),
		})
		require.NoError(t, err)
	}

	for i, ticket := range tickets {
		var res [1]iosched.Result
		_, err := s.Wait(ticket, res[:])
		require.NoError(t, err, "chunk %d", i)
		require.NoError(t, res[0].Err, "chunk %d", i)
		require.Equal(t, 4096, res[0].N, "chunk %d", i)
	}

	// Verify all writes.
	for i := range chunks {
		buf := make([]byte, 4096)
		ticket, err := s.Submit([]iosched.Op{iosched.ReadOp(f, buf, int64(i*4096))})
		require.NoError(t, err)
		var res [1]iosched.Result
		_, err = s.Wait(ticket, res[:])
		require.NoError(t, err)
		require.Equal(t, payloads[i], buf, "mismatch at chunk %d", i)
	}
}

// ── Linked ops: write then fdatasync ─────────────────────────────────────────

func TestURing_LinkedOps_WriteFdatasync(t *testing.T) {
	s := newURingSched(t)
	path := filepath.Join(t.TempDir(), "linked.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)

	ops := []iosched.Op{
		iosched.WriteOp(f, payload, 0).Linked(),
		iosched.FdatasyncOp(f),
	}
	ticket, err := s.Submit(ops)
	require.NoError(t, err)

	var res [2]iosched.Result
	n, err := s.Wait(ticket, res[:])
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.NoError(t, res[0].Err, "write result")
	require.Equal(t, 4096, res[0].N)
	require.NoError(t, res[1].Err, "fdatasync result")
}

// ── Double-Wait returns ErrTicketConsumed ─────────────────────────────────────

func TestURing_DoubleWait(t *testing.T) {
	s := newURingSched(t)
	path, _ := writeUringFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	ticket, err := s.Submit([]iosched.Op{iosched.ReadOp(f, buf, 0)})
	require.NoError(t, err)

	var res [1]iosched.Result
	_, err = s.Wait(ticket, res[:])
	require.NoError(t, err)

	_, err = s.Wait(ticket, res[:])
	require.ErrorIs(t, err, iosched.ErrTicketConsumed)
}

// ── BlockingIO wrapping URingScheduler ────────────────────────────────────────

func TestURing_BlockingIO_ReadAt(t *testing.T) {
	bio := newURingIO(t)
	path, data := writeUringFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	n, err := bio.ReadAt(f, buf, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, n)
	require.Equal(t, data, buf)
}

func TestURing_BlockingIO_WriteAt(t *testing.T) {
	bio := newURingIO(t)
	path := filepath.Join(t.TempDir(), "write.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)
	n, err := bio.WriteAt(f, payload, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, n)

	got := make([]byte, 4096)
	_, err = bio.ReadAt(f, got, 0)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// ── Concurrent Submit/Wait ────────────────────────────────────────────────────

func TestURing_Concurrent(t *testing.T) {
	const (
		fileSize          = 1 << 20
		goroutines        = 64
		readsPerGoroutine = 50
		readSize          = 4096
	)
	s := newURingSched(t)
	path, data := writeUringFile(t, fileSize)
	f := openRW(t, path)

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, readSize)
			for i := 0; i < readsPerGoroutine; i++ {
				offset := int64((i * readSize) % (fileSize - readSize))
				ticket, err := s.Submit([]iosched.Op{iosched.ReadOp(f, buf, offset)})
				if err != nil {
					errs <- fmt.Errorf("submit at %d: %w", offset, err)
					return
				}
				var res [1]iosched.Result
				if _, err := s.Wait(ticket, res[:]); err != nil {
					errs <- fmt.Errorf("wait at %d: %w", offset, err)
					return
				}
				if res[0].Err != nil {
					errs <- fmt.Errorf("read at %d: %w", offset, res[0].Err)
					return
				}
				if res[0].N != readSize {
					errs <- fmt.Errorf("short read at %d: %d", offset, res[0].N)
					return
				}
				for j := range buf {
					if buf[j] != data[offset+int64(j)] {
						errs <- fmt.Errorf("mismatch at %d+%d", offset, j)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// ── Close behaviour ───────────────────────────────────────────────────────────

func TestURing_CloseThenSubmit(t *testing.T) {
	s, err := iosched.NewURingScheduler(iosched.URingConfig{})
	require.NoError(t, err)
	require.NoError(t, s.Close())

	path := filepath.Join(t.TempDir(), "dummy.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	buf := make([]byte, 4096)
	_, err = s.Submit([]iosched.Op{iosched.ReadOp(f, buf, 0)})
	require.Error(t, err)
}

// ── SQPOLL ────────────────────────────────────────────────────────────────────

func TestURing_SQPOLL(t *testing.T) {
	s, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: 64, SQPOLL: true})
	if err != nil {
		t.Skipf("SQPOLL not available (may require CAP_SYS_NICE): %v", err)
	}
	defer s.Close()

	path, data := writeUringFile(t, 4096)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	buf := make([]byte, 4096)
	ticket, err := s.Submit([]iosched.Op{iosched.ReadOp(f, buf, 0)})
	require.NoError(t, err)

	var res [1]iosched.Result
	_, err = s.Wait(ticket, res[:])
	require.NoError(t, err)
	require.NoError(t, res[0].Err)
	require.Equal(t, data, buf)
}

// ── DefaultIO uses URingScheduler on Linux ────────────────────────────────────

func TestNewDefaultIO_UsesURing(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}
	bio := iosched.NewDefaultIO()
	defer bio.Close()
	require.NotNil(t, bio.S)
	_, ok := bio.S.(*iosched.URingScheduler)
	require.True(t, ok)
}
