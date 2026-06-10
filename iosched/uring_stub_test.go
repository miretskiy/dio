//go:build !linux && !windows

package iosched

import "testing"

// newURingForTest returns nil on platforms without io_uring; callers skip.
func newURingForTest(t *testing.T, _ URingConfig) Scheduler {
	t.Skip("io_uring requires Linux")
	return nil
}
