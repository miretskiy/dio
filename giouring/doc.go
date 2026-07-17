//go:build linux

// Package giouring is a low-level Go mapping of the Linux io_uring and
// liburing interfaces.
//
// # Memory lifetime
//
// Preparing an SQE only encodes addresses; it does not retain any Go object
// referenced by the SQE. The kernel may use those addresses asynchronously,
// after both the prepare call and the submission syscall have returned. The
// caller must retain every referenced buffer, path, iovec array, timespec,
// socket address, and other structure until the CQE is consumed. For a
// multishot request, retain them until the final CQE. An integer address in an
// SQE is invisible to the Go garbage collector, so ownership should normally
// be represented by longer-lived request state, with runtime.KeepAlive after
// completion where necessary.
//
// In contrast, synchronous Ring methods retain their direct and nested Go
// pointer arguments through the syscall or wait performed by that method. The
// caller does not need an additional runtime.KeepAlive merely to make such an
// argument survive until the method returns.
//
// Registration methods retain their registration metadata through the
// io_uring_register syscall, but registration gives the kernel a longer-lived
// reference to the registered resource. The caller must retain registered
// buffer backing memory and provided-buffer rings until they are unregistered
// or the Ring exits. Similarly, memory passed to QueueInitMem must remain valid
// until QueueExit. giouring does not take ownership of or release caller-owned
// memory.
//
// This package deliberately exposes asynchronous ownership. Higher-level code
// should normally wrap it with request objects that own all referenced memory.
//
// On Linux systems with cgo and the liburing development library installed,
// run the ABI conformance suite with:
//
//	go test -tags=liburing_conformance ./giouring
package giouring
