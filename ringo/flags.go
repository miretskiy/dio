//go:build linux

package ringo

// FallocateFlags selects a Linux fallocate mode for FallocateMode. The zero
// value performs a plain allocation that also extends the file size and is
// equivalent to Fallocate.
type FallocateFlags uint32

const (
	// FallocateKeepSize leaves the file size unchanged (FALLOC_FL_KEEP_SIZE).
	FallocateKeepSize FallocateFlags = 1 << 0
	// FallocatePunchHole deallocates a range; the kernel requires it to be
	// combined with FallocateKeepSize (FALLOC_FL_PUNCH_HOLE).
	FallocatePunchHole FallocateFlags = 1 << 1
	// FallocateNoHideStale is FALLOC_FL_NO_HIDE_STALE.
	FallocateNoHideStale FallocateFlags = 1 << 2
	// FallocateCollapseRange removes a range and shifts later data left
	// (FALLOC_FL_COLLAPSE_RANGE).
	FallocateCollapseRange FallocateFlags = 1 << 3
	// FallocateZeroRange zeroes a range (FALLOC_FL_ZERO_RANGE).
	FallocateZeroRange FallocateFlags = 1 << 4
	// FallocateInsertRange inserts a hole and shifts later data right
	// (FALLOC_FL_INSERT_RANGE).
	FallocateInsertRange FallocateFlags = 1 << 5
	// FallocateUnshareRange unshares shared extents (FALLOC_FL_UNSHARE_RANGE).
	FallocateUnshareRange FallocateFlags = 1 << 6
)

const allFallocateFlags = FallocateKeepSize | FallocatePunchHole |
	FallocateNoHideStale | FallocateCollapseRange | FallocateZeroRange |
	FallocateInsertRange | FallocateUnshareRange

// TimeoutFlags modifies timeout and linked-timeout operations.
type TimeoutFlags uint32

const (
	// TimeoutAbsolute interprets the timeout as an absolute time.
	TimeoutAbsolute TimeoutFlags = 1 << 0
	// TimeoutBoottime uses CLOCK_BOOTTIME.
	TimeoutBoottime TimeoutFlags = 1 << 2
	// TimeoutRealtime uses CLOCK_REALTIME.
	TimeoutRealtime TimeoutFlags = 1 << 3
	// TimeoutSuccess reports a successful result rather than ETIME on expiry.
	TimeoutSuccess TimeoutFlags = 1 << 5
	// TimeoutMultishot produces repeated expiry completions until canceled.
	TimeoutMultishot TimeoutFlags = 1 << 6
)

const timeoutUpdateFlag TimeoutFlags = 1 << 1

// TimeoutUpdateFlags modifies a timeout update.
type TimeoutUpdateFlags uint32

const (
	// TimeoutUpdateAbsolute interprets the replacement timeout as absolute.
	TimeoutUpdateAbsolute TimeoutUpdateFlags = 1 << 0
)

// PollUpdateFlags modifies a poll update.
type PollUpdateFlags uint32

const (
	// PollUpdateMultishot converts the updated poll request to multishot.
	PollUpdateMultishot PollUpdateFlags = 1 << 0
	// PollUpdateLevel requests level-triggered multishot polling.
	PollUpdateLevel PollUpdateFlags = 1 << 3
)

const (
	pollUpdateEvents PollUpdateFlags = 1 << 1
)
