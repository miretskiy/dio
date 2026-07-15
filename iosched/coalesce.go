//go:build linux

package iosched

// Linux permits at most 1024 iovecs for one readv/writev. Capping a coalesced
// run here also bounds the reusable per-ring-slot completion/iovec buffers.
const maxCoalescedWrites = 1024

// coalescibleWrite reports whether o may be merged into a writev: a standalone
// plain positioned write. Fixed-buffer and caller-linked writes stay intact.
func (o *Op) coalescibleWrite() bool {
	return o.kind() == OpWrite && !o.isFixed() && !o.isLinked()
}

func sameWriteTarget(a, b *Op) bool {
	if a.isVirtual() != b.isVirtual() {
		return false
	}
	if a.isVirtual() {
		return a.vfd == b.vfd
	}
	return a.f == b.f
}

func opBytes(op *Op) int {
	switch op.kind() {
	case OpWritev, OpReadv:
		total := 0
		for _, buf := range op.bufs {
			total += len(buf)
		}
		return total
	default:
		return len(op.buf)
	}
}
