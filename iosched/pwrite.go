package iosched

import (
	"syscall"
	"time"
)

// PwriteScheduler is a synchronous IOScheduler backed by pread(2)/pwrite(2).
// Zero overhead — each ReadAt/WriteAt call maps directly to one syscall.
// Safe for use on all platforms.
type PwriteScheduler struct {
	ioLatency
}

// NewPwriteScheduler returns a synchronous pread/pwrite-based scheduler.
func NewPwriteScheduler() (*PwriteScheduler, error) {
	p := &PwriteScheduler{}
	p.initLatency()
	return p, nil
}

// ReadAt performs a positioned read using pread(2).
func (p *PwriteScheduler) ReadAt(fd int, buf []byte, offset int64) (int, error) {
	start := time.Now()
	n, err := syscall.Pread(fd, buf, offset)
	p.recordRead(start)
	return n, err
}

// WriteAt performs a positioned write using pwrite(2).
func (p *PwriteScheduler) WriteAt(fd int, buf []byte, offset int64) (int, error) {
	start := time.Now()
	n, err := syscall.Pwrite(fd, buf, offset)
	p.recordWrite(start)
	return n, err
}

// Stats returns a latency snapshot for this scheduler.
func (p *PwriteScheduler) Stats() Stats {
	return Stats{
		ReadLatency:  p.readSnapshot(),
		WriteLatency: p.writeSnapshot(),
	}
}

// Close is a no-op for the synchronous scheduler.
func (p *PwriteScheduler) Close() error { return nil }
