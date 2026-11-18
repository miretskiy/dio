package iosched

import (
	"os"
	"slices"

	"github.com/miretskiy/dio/v2/nvme"
)

const defaultRingDepth uint32 = 256

// doorbellSlots is the ring entry the io_uring coordinator keeps for its doorbell read.
const doorbellSlots = 1

// minRingDepth holds the longest chain beside the coordinator's doorbell entry.
const minRingDepth = maxChainLength + doorbellSlots

type schedulerConfig struct {
	coordinatorCPU int // negative: an ordinary goroutine
	ringDepth      uint32
	sqPoll         bool
	nvmeCommands   bool
	nvmeDevices    []*nvme.Device // bound the ring depth by their queue depth
	vfiles         uint32
	files          []*os.File
	coalescing     bool
	dmaBuffers     [][]byte
	disk           *DiskPolicy
}

func makeSchedulerConfig(opts []Option) schedulerConfig {
	c := schedulerConfig{
		coordinatorCPU: -1,
		ringDepth:      defaultRingDepth,
		coalescing:     true,
	}
	for _, opt := range opts {
		opt.apply(&c)
	}
	if c.ringDepth == 0 {
		c.ringDepth = defaultRingDepth
	}
	c.ringDepth = max(c.ringDepth, minRingDepth)
	c.vfiles = max(c.vfiles, uint32(len(c.files)))
	if c.disk == nil {
		c.disk = NewDiskPolicy()
	}
	return c
}

// Option configures a scheduler. Device rates and admission limits belong to
// the DiskPolicy passed with WithDiskPolicy. Out-of-range values select the
// default or disable the feature; options never fail. The POSIX fallback honors
// WithVFiles and WithFiles.
type Option interface {
	apply(*schedulerConfig)
}

type optionFunc func(*schedulerConfig)

func (f optionFunc) apply(c *schedulerConfig) { f(c) }

// WithDiskPolicy sets the device model and admission limits. Share one policy
// among the schedulers of a device so they account against the same limits.
// Without it, or with nil, each scheduler uses its own NewDiskPolicy().
func WithDiskPolicy(policy *DiskPolicy) Option {
	return optionFunc(func(c *schedulerConfig) { c.disk = policy })
}

// WithVFiles registers a sparse virtual-file table of size n on the io_uring
// backend. A non-zero table is the prerequisite for virtual-file ops (VOpenatOp
// and friends); without it they fail with EOPNOTSUPP. The POSIX backend
// emulates the table in userspace.
func WithVFiles(n uint32) Option {
	return optionFunc(func(c *schedulerConfig) { c.vfiles = n })
}

// WithFiles installs already-open files in VFD slots 0, 1, ... at startup.
// Those slots are excluded from AllocateVFD. Files are borrowed: keep them open
// until the scheduler closes; the scheduler does not close the original files.
// Combine with WithVFiles to leave space for dynamically opened objects.
func WithFiles(files ...*os.File) Option {
	files = slices.Clone(files)
	return optionFunc(func(c *schedulerConfig) { c.files = files })
}

// WithCoordinatorCPU runs the io_uring coordinator on a dedicated OS thread
// with best-effort affinity to cpu. A negative CPU (the default) leaves it an
// ordinary goroutine. The dedicated thread costs a thread handoff each time an
// idle coordinator wakes: about twice the CPU per I/O at queue depth one, with
// no throughput gain at depths 32 and 128 on the measured host. Affinity
// failures are logged and I/O continues. This pins the userspace coordinator,
// not kernel workers or device IRQs. The POSIX backend ignores this option.
func WithCoordinatorCPU(cpu int) Option {
	return optionFunc(func(c *schedulerConfig) { c.coordinatorCPU = cpu })
}

// WithRingDepth sets the io_uring SQ/CQ depth in entries. Zero (the default)
// uses the backend's default depth. Smaller depths are raised to nine: the
// longest chain, eight operations, plus the coordinator's doorbell entry.
func WithRingDepth(n uint32) Option {
	return optionFunc(func(c *schedulerConfig) { c.ringDepth = n })
}

// WithSQPOLL enables IORING_SETUP_SQPOLL, dedicating a kernel thread to poll the
// submission queue.
func WithSQPOLL() Option {
	return optionFunc(func(c *schedulerConfig) { c.sqPoll = true })
}

// WithNVMeCommands enables NVMeReadOp, NVMeWriteOp, and NVMeFlushOp on the
// io_uring scheduler. It requests 128-byte submission and 32-byte completion
// entries. Ordinary file operations can share this scheduler.
// RawNVMeCmdOp can submit I/O and Admin NVMe commands through the same extended ring.
// The POSIX backend reports EOPNOTSUPP when an NVMe operation is submitted.
//
// The ring depth stays within the QueueDepth of each device listed, so the
// commands in flight fit one hardware queue and are issued inline. Commands
// beyond a full queue still complete, but each waits in an io_uring worker
// thread. Rings sharing a device share its queues.
func WithNVMeCommands(devices ...*nvme.Device) Option {
	return optionFunc(func(c *schedulerConfig) {
		c.nvmeCommands = true
		c.nvmeDevices = append(c.nvmeDevices, devices...)
	})
}

// WithCoalescing controls whether contiguous same-file writes are merged into
// one writev. Coalescing is enabled by default. The POSIX backend ignores it.
func WithCoalescing(enabled bool) Option {
	return optionFunc(func(c *schedulerConfig) { c.coalescing = enabled })
}

// WithDMABuffers registers supplied regions before the io_uring coordinator
// starts, such as allocator.Region() or the Bytes of an alloc.Memory. Regions
// larger than 1 GiB are split into registration entries automatically. Total
// registered memory is limited to 32 GiB per ring.
// ReadFixedOp and WriteFixedOp require a buffer inside one registration entry.
// NVMe commands whose data lies inside one registration entry use it, which
// spares the driver pinning their pages for each command; others use ordinary
// data addresses.
// The caller keeps every region mapped and unchanged in place until the
// scheduler has closed successfully, including between I/O operations.
//
// The region list is copied. Repeated options replace the list; an empty list
// disables registration.
// The POSIX fallback ignores this option.
func WithDMABuffers(buffers ...[]byte) Option {
	buffers = slices.Clone(buffers)
	return optionFunc(func(c *schedulerConfig) {
		c.dmaBuffers = buffers
	})
}
