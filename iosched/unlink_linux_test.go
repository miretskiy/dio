package iosched_test

import "testing"

func TestURingUnlink(t *testing.T) {
	testUnlink(t, newURingSched(t))
}
