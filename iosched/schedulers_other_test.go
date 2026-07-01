//go:build !linux

package iosched_test

// availableSchedulers off Linux provides only the POSIX backend. (Named
// "other", not "non_linux", so Go does not read a GOOS constraint from the
// filename; the //go:build tag alone selects it.)
func availableSchedulers() []schedulerFactory {
	return []schedulerFactory{
		{name: "POSIX", newSched: newPOSIX},
	}
}
