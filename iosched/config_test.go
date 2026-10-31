package iosched

import (
	"testing"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

func TestWithDMABuffersCopiesCallerList(t *testing.T) {
	first, second := alloc.NewMemory(4096), alloc.NewMemory(8192)
	defer first.Unpin()
	defer second.Unpin()
	input := [][]byte{first.Bytes()}
	option := WithDMABuffers(input...)
	input[0] = second.Bytes()
	cfg := makeSchedulerConfig([]Option{option})
	require.Len(t, cfg.dmaBuffers, 1)
	require.Same(t, &first.Bytes()[0], &cfg.dmaBuffers[0][0])
	cfg = makeSchedulerConfig([]Option{option, WithDMABuffers(second.Bytes(), first.Bytes())})
	require.Equal(t, [][]byte{second.Bytes(), first.Bytes()}, cfg.dmaBuffers)
	cfg = makeSchedulerConfig([]Option{option, WithDMABuffers()})
	require.Empty(t, cfg.dmaBuffers)
}
