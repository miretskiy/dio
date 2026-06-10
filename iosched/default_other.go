//go:build !linux && !windows

package iosched

// NewDefaultIO returns the best available [BlockingIO] for the host. Without
// io_uring that is the direct POSIX syscall path.
func NewDefaultIO() *BlockingIO {
	return NewPosixIO()
}
