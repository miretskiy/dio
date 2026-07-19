package iosched

// NewDefaultScheduler returns the best available Scheduler for the host.
//
// On Linux with io_uring support, it returns a [URingScheduler]; otherwise (or
// if io_uring setup fails) a [POSIXScheduler]. Options configure the io_uring
// backend — e.g. WithVFiles(n) to register a virtual-file table, the
// prerequisite for virtual-file ops. The POSIX backend emulates virtual files
// with an unbounded userspace table and ignores these knobs, so virtual-file
// consumers get a working scheduler on either host.
//
// Callers that require io_uring can pass the same options to
// [NewURingScheduler] directly and handle its setup error.
func NewDefaultScheduler(opts ...Option) Scheduler {
	if IOUringAvailable {
		if s, err := NewURingScheduler(opts...); err == nil {
			return s
		}
	}
	return NewPOSIXScheduler()
}
