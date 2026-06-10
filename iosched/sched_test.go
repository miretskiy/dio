//go:build !windows

package iosched

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// forEachScheduler runs fn against every Scheduler implementation available
// on the host.
func forEachScheduler(t *testing.T, fn func(t *testing.T, s Scheduler)) {
	t.Run("posix", func(t *testing.T) {
		s := NewPosixScheduler()
		defer func() { require.NoError(t, s.Close()) }()
		fn(t, s)
	})
	t.Run("uring", func(t *testing.T) {
		s := newURingForTest(t, URingConfig{})
		if s == nil {
			return
		}
		defer func() { require.NoError(t, s.Close()) }()
		fn(t, s)
	})
}

func tempFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "data"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestWriteThenRead(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s Scheduler) {
		f := tempFile(t)
		fd := int(f.Fd())

		const blocks = 8
		const bs = 4096
		want := make([]byte, blocks*bs)
		_, err := rand.NewChaCha8([32]byte{1}).Read(want)
		require.NoError(t, err)

		// Write all blocks in one Submit, out-of-order offsets.
		writes := make([]Op, 0, blocks)
		for i := blocks - 1; i >= 0; i-- {
			writes = append(writes, WriteOp(fd, want[i*bs:(i+1)*bs], int64(i*bs)))
		}
		require.NoError(t, s.Submit(writes))
		for i := range writes {
			r := writes[i].Result()
			require.NoError(t, r.Err)
			require.Equal(t, bs, r.N)
		}

		// Read them back in a separate Submit.
		got := make([]byte, blocks*bs)
		reads := make([]Op, 0, blocks)
		for i := 0; i < blocks; i++ {
			reads = append(reads, ReadOp(fd, got[i*bs:(i+1)*bs], int64(i*bs)))
		}
		require.NoError(t, s.Submit(reads))
		for i := range reads {
			r := reads[i].Result()
			require.NoError(t, r.Err)
			require.Equal(t, bs, r.N)
		}
		require.True(t, bytes.Equal(want, got))
	})
}

func TestReadAtEOF(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s Scheduler) {
		f := tempFile(t)
		fd := int(f.Fd())
		require.NoError(t, os.WriteFile(f.Name(), []byte("hello"), 0o644))

		buf := make([]byte, 16)
		ops := []Op{
			ReadOp(fd, buf, 0),                // short read: 5 bytes then EOF
			ReadOp(fd, make([]byte, 8), 1000), // entirely past EOF
		}
		require.NoError(t, s.Submit(ops))

		r := ops[0].Result()
		require.NoError(t, r.Err)
		require.Equal(t, 5, r.N)
		require.Equal(t, "hello", string(buf[:r.N]))

		r = ops[1].Result()
		require.NoError(t, r.Err)
		require.Equal(t, 0, r.N) // EOF is N==0, not an error
	})
}

func TestOpError(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s Scheduler) {
		ops := []Op{ReadOp(-1, make([]byte, 8), 0)}
		require.NoError(t, s.Submit(ops))
		require.ErrorIs(t, ops[0].Result().Err, syscall.EBADF)
	})
}

func TestFsyncOps(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s Scheduler) {
		f := tempFile(t)
		fd := int(f.Fd())
		data := []byte("durable")
		ops := []Op{
			WriteOp(fd, data, 0).Linked(),
			FdatasyncOp(fd).Linked(),
			FsyncOp(fd),
		}
		require.NoError(t, s.Submit(ops))
		for i := range ops {
			require.NoError(t, ops[i].Result().Err, "op %d", i)
		}
		require.Equal(t, len(data), ops[0].Result().N)
	})
}

func TestLinkedChainBreaks(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s Scheduler) {
		f := tempFile(t)
		fd := int(f.Fd())

		// Middle op fails (bad fd); the linked tail must be canceled, and the
		// independent op after the chain must still run.
		ops := []Op{
			WriteOp(fd, []byte("aaaa"), 0).Linked(),
			WriteOp(-1, []byte("bbbb"), 4).Linked(),
			WriteOp(fd, []byte("cccc"), 8).Linked(),
			WriteOp(fd, []byte("dddd"), 12),
			WriteOp(fd, []byte("eeee"), 16), // independent of the chain
		}
		require.NoError(t, s.Submit(ops))

		require.NoError(t, ops[0].Result().Err)
		require.ErrorIs(t, ops[1].Result().Err, syscall.EBADF)
		require.ErrorIs(t, ops[2].Result().Err, syscall.ECANCELED)
		require.ErrorIs(t, ops[3].Result().Err, syscall.ECANCELED)
		require.NoError(t, ops[4].Result().Err)
		require.Equal(t, 4, ops[4].Result().N)
	})
}

func TestHardLinkedChainContinues(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s Scheduler) {
		f := tempFile(t)
		fd := int(f.Fd())
		ops := []Op{
			WriteOp(-1, []byte("bad"), 0).HardLinked(),
			WriteOp(fd, []byte("good"), 0),
		}
		require.NoError(t, s.Submit(ops))
		require.ErrorIs(t, ops[0].Result().Err, syscall.EBADF)
		require.NoError(t, ops[1].Result().Err)
	})
}

func TestTrailingLinkRejected(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s Scheduler) {
		f := tempFile(t)
		ops := []Op{WriteOp(int(f.Fd()), []byte("x"), 0).Linked()}
		require.Error(t, s.Submit(ops))
	})
}

func TestSubmitAfterClose(t *testing.T) {
	t.Run("posix", func(t *testing.T) {
		s := NewPosixScheduler()
		require.NoError(t, s.Close())
		require.ErrorIs(t, s.Submit([]Op{FsyncOp(0)}), ErrClosed)
	})
	t.Run("uring", func(t *testing.T) {
		s := newURingForTest(t, URingConfig{})
		if s == nil {
			return
		}
		require.NoError(t, s.Close())
		require.ErrorIs(t, s.Submit([]Op{FsyncOp(0)}), ErrClosed)
	})
}

func TestEmptySubmit(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s Scheduler) {
		require.NoError(t, s.Submit(nil))
	})
}

// TestConcurrentSubmitters hammers one scheduler from many goroutines, each
// owning a disjoint region of the file, and verifies every byte.
func TestConcurrentSubmitters(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s Scheduler) {
		const (
			goroutines = 8
			rounds     = 50
			batch      = 4
			bs         = 4096
		)
		f := tempFile(t)
		fd := int(f.Fd())
		require.NoError(t, f.Truncate(goroutines*rounds*batch*bs))

		var wg sync.WaitGroup
		errs := make([]error, goroutines)
		for gi := 0; gi < goroutines; gi++ {
			wg.Add(1)
			go func(gi int) {
				defer wg.Done()
				rng := rand.NewChaCha8([32]byte{byte(gi)})
				base := int64(gi * rounds * batch * bs)
				wbuf := make([]byte, batch*bs)
				rbuf := make([]byte, batch*bs)
				for round := 0; round < rounds; round++ {
					if _, err := rng.Read(wbuf); err != nil {
						errs[gi] = err
						return
					}
					off := base + int64(round*batch*bs)
					ops := make([]Op, 0, batch)
					for b := 0; b < batch; b++ {
						ops = append(ops, WriteOp(fd, wbuf[b*bs:(b+1)*bs], off+int64(b*bs)))
					}
					if err := s.Submit(ops); err != nil {
						errs[gi] = err
						return
					}
					ops = ops[:0]
					for b := 0; b < batch; b++ {
						ops = append(ops, ReadOp(fd, rbuf[b*bs:(b+1)*bs], off+int64(b*bs)))
					}
					if err := s.Submit(ops); err != nil {
						errs[gi] = err
						return
					}
					for i := range ops {
						if r := ops[i].Result(); r.Err != nil || r.N != bs {
							errs[gi] = fmt.Errorf("round %d op %d: n=%d err=%v", round, i, r.N, r.Err)
							return
						}
					}
					if !bytes.Equal(wbuf, rbuf) {
						errs[gi] = fmt.Errorf("round %d: data mismatch", round)
						return
					}
				}
			}(gi)
		}
		wg.Wait()
		for gi, err := range errs {
			require.NoError(t, err, "goroutine %d", gi)
		}
	})
}

func TestBlockingIO(t *testing.T) {
	run := func(t *testing.T, b *BlockingIO) {
		defer func() { require.NoError(t, b.Close()) }()
		f := tempFile(t)

		want := make([]byte, 64<<10)
		_, err := rand.NewChaCha8([32]byte{7}).Read(want)
		require.NoError(t, err)

		n, err := b.WriteAt(f, want, 128)
		require.NoError(t, err)
		require.Equal(t, len(want), n)
		require.NoError(t, b.Fsync(f))
		require.NoError(t, b.Fdatasync(f))

		got := make([]byte, len(want))
		n, err = b.ReadAt(f, got, 128)
		require.NoError(t, err)
		require.Equal(t, len(want), n)
		require.True(t, bytes.Equal(want, got))

		// ReadAt past EOF returns the available prefix plus io.EOF.
		tail := make([]byte, 4096)
		n, err = b.ReadAt(f, tail, 128+int64(len(want))-100)
		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, 100, n)
		require.True(t, bytes.Equal(want[len(want)-100:], tail[:100]))

		// Zero-length read.
		n, err = b.ReadAt(f, nil, 0)
		require.NoError(t, err)
		require.Zero(t, n)
	}

	t.Run("posix", func(t *testing.T) { run(t, NewPosixIO()) })
	t.Run("default", func(t *testing.T) { run(t, NewDefaultIO()) })
	t.Run("posix-sched", func(t *testing.T) { run(t, NewBlockingIO(NewPosixScheduler())) })
}
