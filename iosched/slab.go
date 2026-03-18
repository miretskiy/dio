package iosched

// indexSlab manages a fixed-size pool of uint16 slot indices via a buffered
// channel. The channel acts as a counting semaphore: callers block in acquire
// when all slots are in use, providing natural backpressure.
//
// indexSlab itself holds no data; each scheduler maintains its own group array
// and uses indexSlab purely for free-index tracking.
type indexSlab struct {
	free chan uint16
}

func newIndexSlab(n int) indexSlab {
	s := indexSlab{free: make(chan uint16, n)}
	for i := range n {
		s.free <- uint16(i)
	}
	return s
}

// acquire returns a free slot index. Blocks until one is available or
// stopCh is closed, in which case it returns errSchedulerClosed.
func (s *indexSlab) acquire(stopCh <-chan struct{}) (uint16, error) {
	select {
	case idx := <-s.free:
		return idx, nil
	case <-stopCh:
		return 0, errSchedulerClosed
	}
}

// release returns idx to the free pool. Called by Wait after recycling.
func (s *indexSlab) release(idx uint16) {
	s.free <- idx
}
