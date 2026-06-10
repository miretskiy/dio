//go:build !linux && !windows

package iosched

import "golang.org/x/sys/unix"

// fdatasync degrades to fsync on platforms without a distinct fdatasync.
// (Durable-on-darwin callers needing F_FULLFSYNC should use sys.Fdatasync
// at the file level instead.)
func fdatasync(fd int) error { return unix.Fsync(fd) }
