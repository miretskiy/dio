package iosched

// NewDefaultScheduler returns the best available Scheduler for the host.
//
// On Linux with io_uring support, it returns a [URingScheduler]; otherwise (or
// if io_uring setup fails) a [POSIXScheduler]. An optional [URingConfig]
// configures the io_uring scheduler — e.g. URingConfig{VFiles: n} to register a
// virtual-file table. The POSIX backend emulates virtual files regardless of
// the table size, so callers that use virtual-file ops get a working scheduler
// on either host.
func NewDefaultScheduler(cfg ...URingConfig) Scheduler {
	var c URingConfig
	if len(cfg) > 0 {
		c = cfg[0]
	}
	if IOUringAvailable {
		if s, err := NewURingScheduler(c); err == nil {
			return s
		}
	}
	return NewPOSIXScheduler()
}
