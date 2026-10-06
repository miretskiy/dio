//go:build !linux

package iosched

// Off Linux nothing uses the io_uring backend's helpers, and staticcheck
// reports them as unused. Referencing them here keeps the macOS build
// warning-free without build-tagging shared code.
var (
	_ = (*Op).coalescibleWrite
	_ = sameWriteTarget
	_ = defaultRingDepth
	_ = makeSchedulerConfig
	_ = (*Op).isFixed
)
