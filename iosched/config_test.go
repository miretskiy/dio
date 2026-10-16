package iosched

import (
	"testing"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func TestWithDMABuffersCopiesCallerList(t *testing.T) {
	first, second := alloc.NewBuffer(4096), alloc.NewBuffer(8192)
	defer first.Unpin()
	defer second.Unpin()
	input := []*alloc.Buffer{first}
	option := WithDMABuffers(input...)
	input[0] = second
	cfg := makeSchedulerConfig([]Option{option})
	require.Len(t, cfg.dmaBuffers, 1)
	require.Same(t, first, cfg.dmaBuffers[0])
	cfg = makeSchedulerConfig([]Option{option, WithDMABuffers(second, first)})
	require.Equal(t, []*alloc.Buffer{second, first}, cfg.dmaBuffers)
	cfg = makeSchedulerConfig([]Option{option, WithDMABuffers()})
	require.Empty(t, cfg.dmaBuffers)
}
