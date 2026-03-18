package iosched

// NewDefaultIO returns a [BlockingIO] backed by the best available scheduler
// for the host.
//
// On Linux with io_uring support: wraps a [URingScheduler] with default config.
// Otherwise: wraps nil, using direct POSIX syscalls.
//
// Callers that need more control (SQPOLL, custom ring depth, or direct access
// to the async Submit/Wait API) should construct [URingScheduler] directly.
func NewDefaultIO() *BlockingIO {
	if IOUringAvailable {
		if s, err := NewURingScheduler(URingConfig{}); err == nil {
			return NewBlockingIO(s)
		}
	}
	return NewBlockingIO(nil)
}
