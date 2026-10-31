package collections

import (
	"fmt"
	"reflect"
	"unsafe"
)

// VerifyAlignment reports whether MapShard[K, V, E] needs more padding in E to
// keep shards on separate cache lines: its size must be a multiple of 64, and
// E must not end in compiler-inserted padding, which hides how much padding E
// actually supplies. Call it from a test of the type that defines E.
func VerifyAlignment[K comparable, V, E any]() error {
	var s MapShard[K, V, E]
	if size := unsafe.Sizeof(s); size%64 != 0 {
		return fmt.Errorf("collections: shard size %d is not a multiple of 64; add [%d]byte padding to %T",
			size, 64-size%64, s.Extra)
	}
	e := reflect.TypeOf(s.Extra)
	if e.Kind() != reflect.Struct || e.NumField() == 0 {
		return nil
	}
	last := e.Field(e.NumField() - 1)
	if pad := e.Size() - (last.Offset + last.Type.Size()); pad != 0 {
		return fmt.Errorf("collections: implicit padding detected in %v: %d bytes after field %q; add them to its padding",
			e, pad, last.Name)
	}
	return nil
}
