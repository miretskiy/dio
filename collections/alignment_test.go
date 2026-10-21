package collections

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// VerifyAlignment checks the shard size and compiler-inserted trailing padding
// in E. Copy this helper into caller _test.go files to check a custom state type.
// These are the original xmap layout checks, not a runtime address check.
func VerifyAlignment[K comparable, V, E any]() error {
	var s MapShard[K, V, E]
	totalSize := unsafe.Sizeof(s)

	// 1. Check Total Alignment (Must be multiple of 64)
	if totalSize%64 != 0 {
		return fmt.Errorf("collections: Shard size (%d) is not a multiple of 64; "+
			"add [%d]byte padding to Extra struct",
			totalSize, 64-(totalSize%64))
	}

	// 2. The Padding Detective (Check for compiler-inserted padding)
	// We inspect 'E' (the Extra struct) to see if the compiler added silent padding
	// after the last field to align the struct.
	eType := reflect.TypeOf(s.Extra)

	// Only inspect if it's a struct with fields
	if eType.Kind() == reflect.Struct && eType.NumField() > 0 {
		lastField := eType.Field(eType.NumField() - 1)

		// The generic offset calculation
		explicitEnd := lastField.Offset + lastField.Type.Size()

		if explicitEnd != eType.Size() {
			invisiblePad := eType.Size() - explicitEnd
			return fmt.Errorf("collections: implicit padding detected in %v: "+
				"compiler added %d bytes after field %q; "+
				"increase manual padding array by %d bytes",
				eType, invisiblePad, lastField.Name, invisiblePad)
		}
	}

	return nil
}

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
