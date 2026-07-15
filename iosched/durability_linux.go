//go:build linux

package iosched

// syncOp returns the fdatasync to link after a durable write, targeting the same
// file — virtual slot or regular descriptor. This is the one place the
// virtual/regular distinction is made for durability; callers stay agnostic.
func (o Op) syncOp() Op {
	if o.isVirtual() {
		return VFdatasyncOp(o.vfd)
	}
	return FdatasyncOp(o.f)
}
