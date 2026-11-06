package buildutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTestModeDetectsGoTestBinary(t *testing.T) {
	require.True(t, TestMode)
}

func TestEnableTestMode(t *testing.T) {
	t.Cleanup(EnableTestMode(false))
	reset := EnableTestMode(true)
	require.True(t, TestMode)
	reset()
	require.False(t, TestMode)
}

func TestAssert(t *testing.T) {
	require.NotPanics(t, func() { Assert(true) })
	require.PanicsWithValue(t, "assertion failed", func() { Assert(false) })
}

func TestTestModeSkipsCondition(t *testing.T) {
	t.Cleanup(EnableTestMode(false))

	evaluated := false
	condition := func() bool {
		evaluated = true
		return false
	}
	if TestMode {
		Assert(condition())
	}
	require.False(t, evaluated, "disabled assertions must not evaluate the condition")
	// The caller controls the guard; Assert itself always panics on failure.
	require.PanicsWithValue(t, "assertion failed", func() { Assert(false) })
}
