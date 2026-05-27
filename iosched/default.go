package iosched

// NewDefaultScheduler returns the best available Scheduler for the host.
//
// On Linux with io_uring support, it returns a [URingScheduler] with default
// config. Otherwise it returns a [POSIXScheduler].
func NewDefaultScheduler() Scheduler {
	if IOUringAvailable {
		if s, err := NewURingScheduler(URingConfig{}); err == nil {
			return s
		}
	}
	return NewPOSIXScheduler()
}
