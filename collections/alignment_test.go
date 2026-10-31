package collections

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestShardedMapAlignment(t *testing.T) {
	m := NewShardedMap[int, int](4)
	require.EqualValues(t, 64, unsafe.Sizeof(*m.ShardAt(0)))
	require.NoError(t, VerifyAlignment[int, int, Pad32]())
	require.ErrorContains(t, VerifyAlignment[int, int, struct{}](), "not a multiple of 64")

	// Both states produce a 64-byte shard, but the first leaves one byte of
	// implicit tail padding. A size-only check would miss that difference.
	type implicit struct {
		hand uint64
		_    [23]byte
	}
	type explicit struct {
		hand uint64
		_    [24]byte
	}
	require.EqualValues(t, 64, unsafe.Sizeof(MapShard[int, int, implicit]{}))
	require.ErrorContains(t, VerifyAlignment[int, int, implicit](), "implicit padding detected")
	require.NoError(t, VerifyAlignment[int, int, explicit]())

	// An unpadded E is permitted; layout checks belong in caller tests.
	unpadded := NewShardedMapWithExtra[int, int](4, struct{}{})
	unpadded.Put(1, 2)
	value, ok := unpadded.Get(1)
	require.True(t, ok)
	require.Equal(t, 2, value)
}
