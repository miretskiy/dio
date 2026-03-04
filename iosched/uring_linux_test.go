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

func writeUringTestFile(t *testing.T, size int) (string, []byte) {
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

// ─── Availability probe ───────────────────────────────────────────────────────

func TestURingScheduler_Available(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
}

// ─── ReadAt ──────────────────────────────────────────────────────────────────

func TestURingScheduler_ReadAt_Basic(t *testing.T) {
	sched := newURingSched(t)
	path, data := writeUringTestFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	n, err := sched.ReadAt(int(f.Fd()), buf, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, n)
	require.Equal(t, data, buf)
}

func TestURingScheduler_ReadAt_Offset(t *testing.T) {
	sched := newURingSched(t)
	path, data := writeUringTestFile(t, 8192)
	f := openRW(t, path)

	buf := make([]byte, 1024)
	n, err := sched.ReadAt(int(f.Fd()), buf, 4096)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.Equal(t, data[4096:4096+1024], buf)
}

func TestURingScheduler_ReadAt_EmptyBuf(t *testing.T) {
	sched := newURingSched(t)
	n, err := sched.ReadAt(0, nil, 0)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

func TestURingScheduler_ReadAt_Large(t *testing.T) {
	sched := newURingSched(t)
	const size = 1 << 20 // 1 MiB
	path, data := writeUringTestFile(t, size)
	f := openRW(t, path)

	buf := make([]byte, size)
	n, err := sched.ReadAt(int(f.Fd()), buf, 0)
	require.NoError(t, err)
	require.Equal(t, size, n)
	require.Equal(t, data, buf)
}

// ─── WriteAt ─────────────────────────────────────────────────────────────────

func TestURingScheduler_WriteAt_Basic(t *testing.T) {
	sched := newURingSched(t)
	path := filepath.Join(t.TempDir(), "uring-write.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRW(t, path)

	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)

	n, err := sched.WriteAt(int(f.Fd()), payload, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, n)

	got := make([]byte, 4096)
	nn, err := sched.ReadAt(int(f.Fd()), got, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, nn)
	require.Equal(t, payload, got)
}

func TestURingScheduler_WriteAt_EmptyBuf(t *testing.T) {
	sched := newURingSched(t)
	n, err := sched.WriteAt(0, nil, 0)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

// ─── Round-trip ───────────────────────────────────────────────────────────────

func TestURingScheduler_RoundTrip(t *testing.T) {
	sched := newURingSched(t)
	const fileSize = 1 << 20 // 1 MiB
	path := filepath.Join(t.TempDir(), "uring-rt.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, fileSize), 0644))
	f := openRW(t, path)

	written := make([]byte, fileSize)
	_, _ = rand.Read(written)

	// Write in 4 KiB chunks.
	for off := 0; off < fileSize; off += 4096 {
		n, err := sched.WriteAt(int(f.Fd()), written[off:off+4096], int64(off))
		require.NoError(t, err, "WriteAt offset %d", off)
		require.Equal(t, 4096, n)
	}

	// Read back and verify.
	for off := 0; off < fileSize; off += 4096 {
		buf := make([]byte, 4096)
		n, err := sched.ReadAt(int(f.Fd()), buf, int64(off))
		require.NoError(t, err, "ReadAt offset %d", off)
		require.Equal(t, 4096, n)
		require.Equal(t, written[off:off+4096], buf, "mismatch at offset %d", off)
	}
}

// ─── Concurrent ──────────────────────────────────────────────────────────────

func TestURingScheduler_Concurrent(t *testing.T) {
	const (
		fileSize          = 1 << 20
		goroutines        = 64
		readsPerGoroutine = 50
		readSize          = 4096
	)
	sched := newURingSched(t)
	path, data := writeUringTestFile(t, fileSize)
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
				n, err := sched.ReadAt(int(f.Fd()), buf, offset)
				if err != nil {
					errs <- fmt.Errorf("read at %d: %w", offset, err)
					return
				}
				if n != readSize {
					errs <- fmt.Errorf("short read at %d: %d", offset, n)
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

// ─── Close behaviour ─────────────────────────────────────────────────────────

func TestURingScheduler_CloseThenRead(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}
	s, err := iosched.NewURingScheduler(iosched.URingConfig{})
	require.NoError(t, err)

	require.NoError(t, s.Close())

	path, _ := writeUringTestFile(t, 4096)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	_, err = s.ReadAt(int(f.Fd()), make([]byte, 4096), 0)
	require.Error(t, err, "ReadAt after Close must return an error")
}

func TestURingScheduler_CloseThenWrite(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}
	s, err := iosched.NewURingScheduler(iosched.URingConfig{})
	require.NoError(t, err)

	require.NoError(t, s.Close())

	_, err = s.WriteAt(0, make([]byte, 4096), 0)
	require.Error(t, err, "WriteAt after Close must return an error")
}

// ─── SQPOLL ──────────────────────────────────────────────────────────────────

func TestURingScheduler_SQPOLL(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}
	s, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: 64, SQPOLL: true})
	if err != nil {
		t.Skipf("SQPOLL not available (may require CAP_SYS_NICE): %v", err)
	}
	defer s.Close()

	path, data := writeUringTestFile(t, 4096)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	buf := make([]byte, 4096)
	n, err := s.ReadAt(int(f.Fd()), buf, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, n)
	require.Equal(t, data, buf)
}

// ─── Interface compliance ─────────────────────────────────────────────────────

func TestURingScheduler_ImplementsInterface(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}
	s, err := iosched.NewURingScheduler(iosched.URingConfig{})
	require.NoError(t, err)
	defer s.Close()
	var _ iosched.IOScheduler = s
}

// ─── Stats ────────────────────────────────────────────────────────────────────

func TestURingScheduler_Stats(t *testing.T) {
	sched := newURingSched(t)
	path, _ := writeUringTestFile(t, 4096)
	f := openRW(t, path)

	buf := make([]byte, 4096)
	_, err := sched.ReadAt(int(f.Fd()), buf, 0)
	require.NoError(t, err)

	stats := sched.Stats()
	require.NotNil(t, stats.ReadLatency)
	require.Equal(t, int64(1), stats.ReadLatency.TotalCount())
	require.Equal(t, int64(1), stats.Requests)
	require.Equal(t, int64(1), stats.Batches)
}

// ─── DefaultScheduler picks io_uring on Linux ─────────────────────────────────

func TestNewDefaultScheduler_UsesURing(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available")
	}
	sched, err := iosched.NewDefaultScheduler()
	require.NoError(t, err)
	defer sched.Close()

	_, ok := sched.(*iosched.URingScheduler)
	require.True(t, ok, "NewDefaultScheduler should return *URingScheduler when io_uring is available")
}
