//go:build linux

package iosched

// NewDefaultIO returns the best available [BlockingIO] for the host:
// io_uring-backed when the kernel supports it, direct POSIX syscalls
// otherwise.
func NewDefaultIO() *BlockingIO {
	if s, err := NewURingScheduler(URingConfig{}); err == nil {
		return NewBlockingIO(s)
	}
	return NewPosixIO()
}
