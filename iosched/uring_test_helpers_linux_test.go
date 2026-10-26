//go:build linux

package iosched

// diskOptions gives a scheduler its own policy built from opts.
func diskOptions(opts ...DiskPolicyOption) Option { return WithDiskPolicy(NewDiskPolicy(opts...)) }
