package iosched_test

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/iosched"
)

func newTestIO(t *testing.T) *iosched.BlockingIO {
	t.Helper()
	bio := iosched.NewDefaultIO()
	t.Cleanup(func() { require.NoError(t, bio.Close()) })
	return bio
}

func writeTempFile(t *testing.T, size int) (path string, data []byte) {
	t.Helper()
	data = make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	path = filepath.Join(t.TempDir(), "test.dat")
	require.NoError(t, os.WriteFile(path, data, 0644))
	return path, data
}

func openRWFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
}

// ── ReadAt ────────────────────────────────────────────────────────────────────

func TestBlockingIO_ReadAt_Basic(t *testing.T) {
	bio := newTestIO(t)
	path, data := writeTempFile(t, 4096)
	f := openRWFile(t, path)

	buf := make([]byte, 4096)
	n, err := bio.ReadAt(f, buf, 0)
	require.NoError(t, err)
	require.Equal(t, 4096, n)
	require.Equal(t, data, buf)
}

func TestBlockingIO_ReadAt_Offset(t *testing.T) {
	bio := newTestIO(t)
	path, data := writeTempFile(t, 8192)
	f := openRWFile(t, path)

	buf := make([]byte, 1024)
	n, err := bio.ReadAt(f, buf, 4096)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.Equal(t, data[4096:4096+1024], buf)
}

func TestBlockingIO_ReadAt_Empty(t *testing.T) {
	bio := newTestIO(t)
	path := filepath.Join(t.TempDir(), "empty.dat")
	require.NoError(t, os.WriteFile(path, []byte{}, 0644))
	f := openRWFile(t, path)

	n, err := bio.ReadAt(f, nil, 0)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

// ── WriteAt ───────────────────────────────────────────────────────────────────

func TestBlockingIO_WriteAt_Basic(t *testing.T) {
	bio := newTestIO(t)
	path := filepath.Join(t.TempDir(), "write.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRWFile(t, path)

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

func TestBlockingIO_WriteAt_Empty(t *testing.T) {
	bio := newTestIO(t)
	path := filepath.Join(t.TempDir(), "empty.dat")
	require.NoError(t, os.WriteFile(path, []byte{}, 0644))
	f := openRWFile(t, path)

	n, err := bio.WriteAt(f, nil, 0)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

// ── Round-trip ────────────────────────────────────────────────────────────────

func TestBlockingIO_RoundTrip(t *testing.T) {
	const fileSize = 1 << 20
	bio := newTestIO(t)
	path := filepath.Join(t.TempDir(), "rt.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, fileSize), 0644))
	f := openRWFile(t, path)

	written := make([]byte, fileSize)
	_, _ = rand.Read(written)

	for off := 0; off < fileSize; off += 4096 {
		n, err := bio.WriteAt(f, written[off:off+4096], int64(off))
		require.NoError(t, err)
		require.Equal(t, 4096, n)
	}
	for off := 0; off < fileSize; off += 4096 {
		buf := make([]byte, 4096)
		n, err := bio.ReadAt(f, buf, int64(off))
		require.NoError(t, err)
		require.Equal(t, 4096, n)
		require.Equal(t, written[off:off+4096], buf, "mismatch at offset %d", off)
	}
}

// ── Concurrent ────────────────────────────────────────────────────────────────

func TestBlockingIO_Concurrent(t *testing.T) {
	const (
		fileSize          = 1 << 20
		goroutines        = 32
		readsPerGoroutine = 100
		readSize          = 4096
	)
	bio := newTestIO(t)
	path, data := writeTempFile(t, fileSize)
	f := openRWFile(t, path)

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, readSize)
			for i := 0; i < readsPerGoroutine; i++ {
				offset := int64((i * readSize) % (fileSize - readSize))
				n, err := bio.ReadAt(f, buf, offset)
				if err != nil {
					errs <- err
					return
				}
				if n != readSize {
					return
				}
				for j := range buf {
					if buf[j] != data[offset+int64(j)] {
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
