package iosched

// Compile-time references to suppress "unused function" lint warnings.
// These symbols are used exclusively from uring_linux.go; linters running
// on non-Linux platforms flag them as unreachable without these anchors.
var (
	_ = Op.isLinked
)
