//go:build linux

package ringo

import "errors"

const defaultDepth = 256

type config struct {
	depth        uint32
	flags        rawSetupFlags
	cqEntries    uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	fixedFiles   uint32
}

func defaultConfig() config {
	return config{
		depth: defaultDepth,
		// rawSetupClamp: lets callers request the largest available
		// queue without hard-coding kernel limits.
		// rawSetupSubmitAll: required for Ringo's linked-submission semantics.
		// rawSetupNoSQArray:  removes an index array that Ringo's ordered SQE
		// preparation does not need. queueInit retries without this optimization.
		flags: rawSetupClamp | rawSetupSubmitAll | rawSetupNoSQArray,
	}
}

// Option configures a Ring created by New. Kernel support for setup options
// varies by Linux version; New returns the io_uring setup or registration error
// when the running kernel does not support an option or option combination.
//
// io_uring: setup flags overview - https://man7.org/linux/man-pages/man7/io_uring_setup_flags.7.html
type Option interface {
	apply(*config) error
}

type optionFunc func(*config) error

func (fn optionFunc) apply(config *config) error {
	return fn(config)
}

// WithDepth requests depth submission queue entries. The default is 256.
//
// Depth bounds both the number of SQEs that can wait for submission and the
// number of operations the Ring can own before their final completions are
// reaped. Linux may round the requested depth up to a power of two. Use
// WithCQSize independently when a workload needs more completion capacity.
//
// io_uring: io_uring_setup - https://man7.org/linux/man-pages/man2/io_uring_setup.2.html
func WithDepth(depth uint32) Option {
	return optionFunc(func(config *config) error {
		if depth == 0 {
			return errors.New("ringo: depth must be nonzero")
		}
		config.depth = depth
		return nil
	})
}

// WithCQSize requests a completion queue with entries slots.
//
// Linux normally creates a CQ with twice as many entries as the SQ. A larger
// CQ is useful when completions arrive in bursts, the caller deliberately
// reaps infrequently, or multishot operations can produce several CQEs from
// one SQE. Ringo requires IORING_FEAT_NODROP, so extra CQ capacity is normally
// a throughput and memory-pressure choice rather than a completion-safety
// requirement: it avoids pushing excess CQEs onto the kernel's overflow list.
//
// The requested size must be greater than the SQ depth and may be rounded up
// to a power of two. It does not increase the number of operations Ringo can
// own; WithDepth controls that limit.
//
// io_uring: IORING_SETUP_CQSIZE - https://man7.org/linux/man-pages/man7/io_uring_setup_flags.7.html
func WithCQSize(entries uint32) Option {
	return optionFunc(func(config *config) error {
		if entries == 0 {
			return errors.New("ringo: completion queue size must be nonzero")
		}
		config.flags |= rawSetupCQSize
		config.cqEntries = entries
		return nil
	})
}

// WithSQPoll creates a kernel thread that polls the submission queue. Under
// sustained load this can publish work without an io_uring_enter system call,
// at the cost of CPU consumed by the polling thread. Measure it against the
// default submission mode; it is not an unconditional performance win.
//
// Ringo callers still call Submit after Push: Submit publishes the userspace
// SQ tail and wakes the polling thread if it has gone idle. WithSQPoll cannot
// be combined with the cooperative, task-run notification, or deferred
// task-run options.
//
// io_uring: IORING_SETUP_SQPOLL - https://man7.org/linux/man-pages/man7/io_uring_sqpoll.7.html
func WithSQPoll() Option {
	return optionFunc(func(config *config) error {
		config.flags |= rawSetupSQPoll
		return nil
	})
}

// WithSQPollCPU enables SQ polling and binds its kernel thread to cpu. A
// dedicated CPU can improve cache locality and avoid scheduler movement for a
// busy ring. The CPU must be online and allowed by the process's cpuset;
// otherwise New fails.
//
// io_uring: IORING_SETUP_SQ_AFF - https://man7.org/linux/man-pages/man7/io_uring_setup_flags.7.html
func WithSQPollCPU(cpu uint32) Option {
	return optionFunc(func(config *config) error {
		config.flags |= rawSetupSQPoll | rawSetupSQAff
		config.sqThreadCPU = cpu
		return nil
	})
}

// WithSQPollIdle enables SQ polling and sets how many idle milliseconds the
// polling thread spins before sleeping. A sleeping thread consumes less CPU,
// but the next Submit must wake it with io_uring_enter and may have higher
// latency. Zero leaves the idle-period behavior to the running kernel.
//
// io_uring: IORING_SETUP_SQPOLL - https://man7.org/linux/man-pages/man7/io_uring_sqpoll.7.html
func WithSQPollIdle(milliseconds uint32) Option {
	return optionFunc(func(config *config) error {
		config.flags |= rawSetupSQPoll
		config.sqThreadIdle = milliseconds
		return nil
	})
}

// WithIOPoll enables completion-side busy polling for supported storage.
// Unlike WithSQPoll, this controls how storage completions are detected, not
// how SQEs are submitted.
//
// I/O polling is a specialized low-latency mode. Read and write operations
// normally require O_DIRECT files, and the filesystem, block device, driver,
// and device queues must support polling. Most other opcodes are forbidden on
// an IOPOLL ring. Call SubmitAndWait with a positive completion count to drive
// polling; Reap alone only consumes CQEs that the kernel has already produced.
//
// io_uring: IORING_SETUP_IOPOLL - https://man7.org/linux/man-pages/man7/io_uring_setup_flags.7.html
func WithIOPoll() Option {
	return optionFunc(func(config *config) error {
		config.flags |= rawSetupIOPoll
		return nil
	})
}

// WithHybridIOPoll enables I/O polling but lets Linux delay briefly before
// busy-polling for a completion. The delay can reduce wasted CPU while
// retaining lower latency than interrupt-driven completion. It has the same
// O_DIRECT, device, driver, opcode, and SubmitAndWait requirements as
// WithIOPoll.
//
// io_uring: IORING_SETUP_HYBRID_IOPOLL - https://man7.org/linux/man-pages/man7/io_uring_setup_flags.7.html
func WithHybridIOPoll() Option {
	return optionFunc(func(config *config) error {
		config.flags |= rawSetupIOPoll | rawSetupHybridIOPoll
		return nil
	})
}

// WithCooperativeTaskRun asks Linux not to interrupt the submitting task with
// an inter-processor interrupt merely to run completion task work. Instead,
// that work runs when the task next crosses the kernel boundary. This can
// improve batching and reduce interruption in an event loop that makes regular
// system calls, but can delay completions when the submitting task remains in
// userspace for long periods. It cannot be combined with WithSQPoll.
//
// io_uring: IORING_SETUP_COOP_TASKRUN - https://man7.org/linux/man-pages/man7/io_uring_setup_flags.7.html
func WithCooperativeTaskRun() Option {
	return optionFunc(func(config *config) error {
		config.flags |= rawSetupCoopTaskrun
		return nil
	})
}

// WithTaskRunFlag enables cooperative task running and asks Linux to set
// IORING_SQ_TASKRUN when task work is waiting. Ringo checks that flag in
// Submit and enters the kernel to process the work even when no new SQEs need
// submission. Callers must still call Submit or SubmitAndWait regularly;
// Reap does not itself enter the kernel. It cannot be combined with
// WithSQPoll.
//
// io_uring: IORING_SETUP_TASKRUN_FLAG - https://man7.org/linux/man-pages/man7/io_uring_setup_flags.7.html
func WithTaskRunFlag() Option {
	return optionFunc(func(config *config) error {
		config.flags |= rawSetupCoopTaskrun | rawSetupTaskrunFlag
		return nil
	})
}

// WithFixedFiles creates an empty sparse registered-file table with count
// slots. OpenAtDirect and OpenAt2Direct populate a slot; CloseDirect releases
// it. Fixed files avoid per-operation descriptor reference work. FixedFile
// values identify slots rather than file generations, so the caller owns slot
// allocation and must not reuse a slot until its old operations and
// CloseDirect have completed. Every Ring requires IORING_FEAT_LINKED_FILE, so
// a linked direct open may be followed by an operation using the populated
// slot.
//
// This option differs from RegisterFiles: it establishes a fixed-size sparse
// table during New without requiring initial files. New does not change
// RLIMIT_NOFILE; an insufficient process limit is returned as an error.
//
// io_uring: io_uring_register_files_sparse - https://man7.org/linux/man-pages/man3/io_uring_register_files_sparse.3.html
func WithFixedFiles(count uint32) Option {
	return optionFunc(func(config *config) error {
		if count == 0 {
			return errors.New("ringo: fixed-file table size must be nonzero")
		}
		config.fixedFiles = count
		return nil
	})
}
