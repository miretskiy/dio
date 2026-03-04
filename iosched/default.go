package iosched

// NewDefaultScheduler returns the best available IOScheduler for the host.
//
// On Linux with io_uring support: returns a [URingScheduler] with default config.
// Otherwise: returns a [PwriteScheduler].
//
// Callers that need more control (SQPOLL, custom ring depth) should construct
// the scheduler directly.
func NewDefaultScheduler() (IOScheduler, error) {
	if IOUringAvailable {
		return NewURingScheduler(URingConfig{})
	}
	return NewPwriteScheduler()
}
