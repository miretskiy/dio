//go:build linux

package iosched

import "golang.org/x/sys/unix"

func fdatasync(fd int) error { return unix.Fdatasync(fd) }
