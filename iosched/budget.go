package iosched

import "time"

// The default disk model, measured on an AWS m7gd.8xlarge instance store with
// scripts/disk-model.sh. Measure another disk the same way and override these
// with WithReadBandwidth, WithReadIOPS, WithWriteBandwidth and WithWriteIOPS.
const (
	DefaultReadBandwidth  int64 = 2_560_000_000 // bytes per second
	DefaultReadIOPS       int64 = 500_000
	DefaultWriteBandwidth int64 = 1_220_000_000 // bytes per second
	DefaultWriteIOPS      int64 = 83_000        // into freshly allocated space
	DefaultLatencyGoal          = 1500 * time.Microsecond
)

// ioClass groups the operations that share one device limit. Reads and writes
// are separate classes: the disks measured reach both ceilings at once. Other
// operations (open, close, sync, fallocate) are not budgeted.
type ioClass uint8

const (
	classRead ioClass = iota
	classWrite
	budgetedClasses // number of classes with a budget
	classOther      = budgetedClasses
)

func classOf(op *Op) ioClass {
	switch op.kind() {
	case OpRead, OpReadv:
		return classRead
	case OpWrite, OpWritev:
		return classWrite
	default:
		return classOther
	}
}

// classLimits are one class's device limits.
type classLimits struct {
	bandwidth int64 // bytes per second
	iops      int64 // operations per second
}

// ioBudget bounds the operations in flight per class. An operation costs the
// device time its class's limits allow it, max(bytes/bandwidth, 1/IOPS): the
// disks measured have independent IOPS and bandwidth limits, so a small
// operation is bound by the first and a large one by the second. Each class
// keeps at most goal of cost in flight, so by Little's law the device queue
// adds about goal of latency, while a goal at or above the cost a class needs
// in flight to reach its ceiling keeps the device saturated.
type ioBudget struct {
	goal   time.Duration // zero: no budget
	off    bool          // WithoutIOBudget
	limits [budgetedClasses]classLimits
}

func defaultBudget() ioBudget {
	return ioBudget{
		goal: DefaultLatencyGoal,
		limits: [budgetedClasses]classLimits{
			classRead:  {bandwidth: DefaultReadBandwidth, iops: DefaultReadIOPS},
			classWrite: {bandwidth: DefaultWriteBandwidth, iops: DefaultWriteIOPS},
		},
	}
}

// cost returns op's class and its cost in nanoseconds of its class's device
// time. A linked chain is charged as its first operation alone: chains are
// rare, and exact accounting would not change how they are placed.
func (b *ioBudget) cost(op *Op) (ioClass, int64) {
	class := classOf(op)
	if b.goal <= 0 || class == classOther {
		return class, 0
	}
	limits := b.limits[class]
	byBytes := int64(float64(opBytes(op)) * 1e9 / float64(limits.bandwidth))
	byOps := int64(time.Second) / limits.iops
	return class, max(byBytes, byOps)
}
