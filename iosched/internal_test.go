package iosched

// Compile-time references to suppress "unused function" lint warnings.
// These symbols are used exclusively from uring_linux.go; linters running
// on non-Linux platforms flag them as unreachable without these anchors.
var (
	_ = Op.isLinked
	_ = makeTicket
	_ = Ticket.groupIdx
	_ = Ticket.gen
	_ = (*resultStore).init
	_ = (*resultStore).copyTo
	_ = newIndexSlab
	_ = (*indexSlab).acquire
	_ = (*indexSlab).release
)
