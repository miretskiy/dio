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

// helper: write random data to a temp file; return path + content.
func writeTestFile(t *testing.T, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "test.dat")
	require.NoError(t, os.WriteFile(path, data, 0644))
	return path, data
}

// helper: open a file and register t.Cleanup for Close.
func openFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
}

func newPwriteSched(t *testing.T) *iosched.PwriteScheduler {
	t.Helper()
	s, err := iosched.NewPwriteScheduler()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// ─── ReadAt ──────────────────────────────────────────────────────────────────

func TestPwriteScheduler_ReadAt_Basic(t *testing.T) {
	const size = 4096
	path, data := writeTestFile(t, size)
	f := openFile(t, path)

	sched := newPwriteSched(t)
	buf := make([]byte, size)
	n, err := sched.ReadAt(int(f.Fd()), buf, 0)
	require.NoError(t, err)
	require.Equal(t, size, n)
	require.Equal(t, data, buf)
}

func TestPwriteScheduler_ReadAt_Offset(t *testing.T) {
	const size = 8192
	path, data := writeTestFile(t, size)
	f := openFile(t, path)

	sched := newPwriteSched(t)
	buf := make([]byte, 1024)
	n, err := sched.ReadAt(int(f.Fd()), buf, 4096)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.Equal(t, data[4096:4096+1024], buf)
}

func TestPwriteScheduler_ReadAt_EmptyBuf(t *testing.T) {
	path, _ := writeTestFile(t, 512)
	f := openFile(t, path)

	sched := newPwriteSched(t)
	n, err := sched.ReadAt(int(f.Fd()), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

// ─── WriteAt ─────────────────────────────────────────────────────────────────

func TestPwriteScheduler_WriteAt_Basic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "write.dat")
	// Pre-create with zeroes.
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openFile(t, path)

	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)

	sched := newPwriteSched(t)
	n, err := sched.WriteAt(int(f.Fd()), payload, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, n)

	// Read back and verify.
	got := make([]byte, 4096)
	nn, err := sched.ReadAt(int(f.Fd()), got, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, nn)
	require.Equal(t, payload, got)
}

func TestPwriteScheduler_WriteAt_Offset(t *testing.T) {
	// Write at a non-zero offset within a pre-allocated file.
	const fileSize = 8192
	path := filepath.Join(t.TempDir(), "write-offset.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, fileSize), 0644))
	f := openFile(t, path)

	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)

	sched := newPwriteSched(t)
	n, err := sched.WriteAt(int(f.Fd()), payload, 4096)
	require.NoError(t, err)
	require.Equal(t, 1024, n)

	got := make([]byte, 1024)
	_, err = sched.ReadAt(int(f.Fd()), got, 4096)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// ─── Round-trip ───────────────────────────────────────────────────────────────

func TestPwriteScheduler_RoundTrip(t *testing.T) {
	const fileSize = 1 << 20 // 1 MiB
	path := filepath.Join(t.TempDir(), "rt.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, fileSize), 0644))
	f := openFile(t, path)

	sched := newPwriteSched(t)

	// Write random data in 4 KiB chunks.
	written := make([]byte, fileSize)
	_, _ = rand.Read(written)
	for off := 0; off < fileSize; off += 4096 {
		chunk := written[off : off+4096]
		n, err := sched.WriteAt(int(f.Fd()), chunk, int64(off))
		require.NoError(t, err)
		require.Equal(t, 4096, n)
	}

	// Read back and verify.
	for off := 0; off < fileSize; off += 4096 {
		buf := make([]byte, 4096)
		n, err := sched.ReadAt(int(f.Fd()), buf, int64(off))
		require.NoError(t, err)
		require.Equal(t, 4096, n)
		require.Equal(t, written[off:off+4096], buf,
			"mismatch at offset %d", off)
	}
}

// ─── Concurrent ──────────────────────────────────────────────────────────────

func TestPwriteScheduler_Concurrent(t *testing.T) {
	const (
		fileSize          = 1 << 20
		goroutines        = 32
		readsPerGoroutine = 100
		readSize          = 4096
	)
	path, data := writeTestFile(t, fileSize)
	f := openFile(t, path)
	sched := newPwriteSched(t)

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
					errs <- err
					return
				}
				if n != readSize {
					errs <- fmt.Errorf("short read at %d: got %d", offset, n)
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

// ─── Stats ────────────────────────────────────────────────────────────────────

func TestPwriteScheduler_Stats(t *testing.T) {
	path, _ := writeTestFile(t, 4096)
	f := openFile(t, path)
	sched := newPwriteSched(t)

	buf := make([]byte, 4096)
	_, err := sched.ReadAt(int(f.Fd()), buf, 0)
	require.NoError(t, err)

	stats := sched.Stats()
	require.NotNil(t, stats.ReadLatency, "ReadLatency histogram must be populated")
	require.Equal(t, int64(1), stats.ReadLatency.TotalCount())
}

// ─── Interface compliance ─────────────────────────────────────────────────────

func TestPwriteScheduler_ImplementsInterface(t *testing.T) {
	s, err := iosched.NewPwriteScheduler()
	require.NoError(t, err)
	defer s.Close()
	var _ iosched.IOScheduler = s
}
