//go:build linux

package ringo

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
