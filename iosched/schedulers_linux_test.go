//go:build linux

package iosched_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/iosched"
)

// availableSchedulers on Linux exercises both backends: POSIX and io_uring (the
// latter with a virtual-file table so virtual ops can run).
func availableSchedulers() []schedulerFactory {
	return []schedulerFactory{
		{name: "POSIX", newSched: newPOSIX},
		{name: "URing", newSched: func(t *testing.T) iosched.Scheduler {
			return newVURingSched(t, iosched.URingConfig{RingDepth: 16, VFiles: 2})
		}},
	}
}

func TestURingCloseCompletesPending(t *testing.T) {
	if !iosched.IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	s, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: 16})
	require.NoError(t, err)
	testSchedulerCloseCompletesPending(t, s)
}
