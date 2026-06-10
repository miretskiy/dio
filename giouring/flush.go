//go:build linux

package giouring

// FlushSQ publishes locally prepared SQEs (obtained via [Ring.GetSQE]) to the
// kernel-visible SQ tail and returns the total number of SQEs the kernel has
// not yet consumed.
//
// It performs no syscall; pair it with [Ring.Enter] to submit. Callers that
// prepare SQEs from multiple goroutines must serialize GetSQE and FlushSQ
// against each other.
func (ring *Ring) FlushSQ() uint32 {
	return ring.internalFlushSQ()
}
