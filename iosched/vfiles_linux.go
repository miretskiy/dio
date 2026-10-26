package iosched

import (
	"math"
	"math/bits"
	"syscall"
)

func (c *coordinator) initVFiles(capacity uint32) {
	c.vfiles = make([]uint64, (int(capacity)+63)/64)
	if tail := capacity % 64; tail != 0 {
		c.vfiles[len(c.vfiles)-1] = math.MaxUint64 << tail
	}
}

func (c *coordinator) allocVFD() uint32 {
	word := c.nextVFile
	for range c.vfiles {
		if free := ^c.vfiles[word]; free != 0 {
			bit := bits.TrailingZeros64(free)
			c.vfiles[word] |= uint64(1) << bit
			c.nextVFile = word
			return uint32(word*64 + bit)
		}
		word++
		if word == len(c.vfiles) {
			word = 0
		}
	}
	return AllocateVFD
}

func (c *coordinator) freeVFD(vfd uint32) {
	c.vfiles[vfd/64] &^= uint64(1) << (vfd % 64)
	c.nextVFile = int(vfd / 64)
}

func (c *coordinator) prepareVFiles(work *submission) error {
	root := &work.root
	if !root.isVirtual() || root.kind() != OpOpenat || root.vfd != AllocateVFD {
		return nil
	}
	vfd := c.allocVFD()
	if vfd == AllocateVFD {
		return syscall.ENFILE
	}
	root.opFlags |= opAllocatedVFD
	for i := range work.opCount() {
		op := work.op(i)
		if op.isVirtual() && op.vfd == AllocateVFD {
			op.vfd = vfd
		}
	}
	return nil
}

func (c *coordinator) releaseVFDReservation(root *operation) {
	if root.opFlags&opAllocatedVFD != 0 {
		c.freeVFD(root.vfd)
	}
}
