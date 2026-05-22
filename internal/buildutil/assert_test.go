package buildutil

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTestModeDetectsGoTestBinary(t *testing.T) {
	require.True(t, TestMode())
}

func TestAssertPanicsUnderTest(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "expected panic")
		msg, ok := r.(string)
		require.True(t, ok)
		require.True(t, strings.HasPrefix(msg, "assertion failed at "), "got: %s", msg)
		require.Contains(t, msg, "assert_test.go:", "panic message should include caller location")
	}()
	Assert(false)
}

func TestAssertReturnsNilWhenCondTrue(t *testing.T) {
	require.NoError(t, Assert(true))
}
